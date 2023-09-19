#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""This module defines Providers usable from yaml, which is a specification
for where to find and how to invoke services that vend implementations of
various PTransforms."""

import collections
import hashlib
import json
import logging
import os
import subprocess
import sys
import urllib.parse
import uuid
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterable
from typing import Mapping
from typing import Optional

import yaml
from yaml.loader import SafeLoader

import apache_beam as beam
import apache_beam.dataframe.io
import apache_beam.io
import apache_beam.transforms.util
from apache_beam.portability.api import schema_pb2
from apache_beam.transforms import external
from apache_beam.transforms import window
from apache_beam.transforms.fully_qualified_named_transform import FullyQualifiedNamedTransform
from apache_beam.typehints import schemas
from apache_beam.typehints import trivial_inference
from apache_beam.utils import python_callable
from apache_beam.utils import subprocess_server
from apache_beam.version import __version__ as beam_version


class Provider:
  """Maps transform types names and args to concrete PTransform instances."""
  def available(self) -> bool:
    """Returns whether this provider is available to use in this environment."""
    raise NotImplementedError(type(self))

  def cache_artifacts(self) -> Optional[Iterable[str]]:
    raise NotImplementedError(type(self))

  def provided_transforms(self) -> Iterable[str]:
    """Returns a list of transform type names this provider can handle."""
    raise NotImplementedError(type(self))

  def create_transform(
      self,
      typ: str,
      args: Mapping[str, Any],
      yaml_create_transform: Callable[
          [Mapping[str, Any], Iterable[beam.PCollection]], beam.PTransform]
  ) -> beam.PTransform:
    """Creates a PTransform instance for the given transform type and arguments.
    """
    raise NotImplementedError(type(self))

  def underlying_provider(self):
    """If this provider is simply a proxy to another provider, return the
    provider that should actually be used for affinity checking.
    """
    return self

  def affinity(self, other: "Provider"):
    """Returns a value approximating how good it would be for this provider
    to be used immediately following a transform from the other provider
    (e.g. to encourage fusion).
    """
    # TODO(yaml): This is a very rough heuristic. Consider doing better.
    # E.g. we could look at the the expected environments themselves.
    # Possibly, we could provide multiple expansions and have the runner itself
    # choose the actual implementation based on fusion (and other) criteria.
    return (
        self.underlying_provider()._affinity(other) +
        other.underlying_provider()._affinity(self))

  def _affinity(self, other: "Provider"):
    if self is other or self == other:
      return 100
    elif type(self) == type(other):
      return 10
    else:
      return 0


def as_provider(name, provider_or_constructor):
  if isinstance(provider_or_constructor, Provider):
    return provider_or_constructor
  else:
    return InlineProvider({name: provider_or_constructor})


def as_provider_list(name, lst):
  if not isinstance(lst, list):
    return as_provider_list(name, [lst])
  return [as_provider(name, x) for x in lst]


class ExternalProvider(Provider):
  """A Provider implemented via the cross language transform service."""
  _provider_types: Dict[str, Callable[..., Provider]] = {}

  def __init__(self, urns, service):
    self._urns = urns
    self._service = service
    self._schema_transforms = None

  def provided_transforms(self):
    return self._urns.keys()

  def create_transform(self, type, args, yaml_create_transform):
    if callable(self._service):
      self._service = self._service()
    if self._schema_transforms is None:
      try:
        self._schema_transforms = {
            config.identifier: config
            for config in external.SchemaAwareExternalTransform.discover(
                self._service, ignore_errors=True)
        }
      except Exception:
        # It's possible this service doesn't vend schema transforms.
        self._schema_transforms = {}
    urn = self._urns[type]
    if urn in self._schema_transforms:
      return external.SchemaAwareExternalTransform(
          urn, self._service, rearrange_based_on_discovery=True, **args)
    else:
      return type >> self.create_external_transform(urn, args)

  def create_external_transform(self, urn, args):
    return external.ExternalTransform(
        urn,
        external.ImplicitSchemaPayloadBuilder(args).payload(),
        self._service)

  @classmethod
  def provider_from_spec(cls, spec):
    from apache_beam.yaml.yaml_transform import SafeLineLoader
    for required in ('type', 'transforms'):
      if required not in spec:
        raise ValueError(
            f'Missing {required} in provider '
            f'at line {SafeLineLoader.get_line(spec)}')
    urns = spec['transforms']
    type = spec['type']
    config = SafeLineLoader.strip_metadata(spec.get('config', {}))
    extra_params = set(SafeLineLoader.strip_metadata(spec).keys()) - set(
        ['transforms', 'type', 'config'])
    if extra_params:
      raise ValueError(
          f'Unexpected parameters in provider of type {type} '
          f'at line {SafeLineLoader.get_line(spec)}: {extra_params}')
    if config.get('version', None) == 'BEAM_VERSION':
      config['version'] = beam_version
    if type in cls._provider_types:
      try:
        return cls._provider_types[type](urns, **config)
      except Exception as exn:
        raise ValueError(
            f'Unable to instantiate provider of type {type} '
            f'at line {SafeLineLoader.get_line(spec)}: {exn}') from exn
    else:
      raise NotImplementedError(
          f'Unknown provider type: {type} '
          f'at line {SafeLineLoader.get_line(spec)}.')

  @classmethod
  def register_provider_type(cls, type_name):
    def apply(constructor):
      cls._provider_types[type_name] = constructor
      return constructor

    return apply


@ExternalProvider.register_provider_type('javaJar')
def java_jar(urns, jar: str):
  if not os.path.exists(jar):
    parsed = urllib.parse.urlparse(jar)
    if not parsed.scheme or not parsed.netloc:
      raise ValueError(f'Invalid path or url: {jar}')
  return ExternalJavaProvider(urns, lambda: jar)


@ExternalProvider.register_provider_type('mavenJar')
def maven_jar(
    urns,
    *,
    artifact_id,
    group_id,
    version,
    repository=subprocess_server.JavaJarServer.MAVEN_CENTRAL_REPOSITORY,
    classifier=None,
    appendix=None):
  return ExternalJavaProvider(
      urns,
      lambda: subprocess_server.JavaJarServer.path_to_maven_jar(
          artifact_id=artifact_id,
          version=version,
          repository=repository,
          classifier=classifier,
          appendix=appendix))


@ExternalProvider.register_provider_type('beamJar')
def beam_jar(
    urns,
    *,
    gradle_target,
    appendix=None,
    version=beam_version,
    artifact_id=None):
  return ExternalJavaProvider(
      urns,
      lambda: subprocess_server.JavaJarServer.path_to_beam_jar(
          gradle_target=gradle_target, version=version, artifact_id=artifact_id)
  )


@ExternalProvider.register_provider_type('docker')
def docker(urns, **config):
  raise NotImplementedError()


@ExternalProvider.register_provider_type('remote')
class RemoteProvider(ExternalProvider):
  _is_available = None

  def __init__(self, urns, address: str):
    super().__init__(urns, service=address)

  def available(self):
    if self._is_available is None:
      try:
        with external.ExternalTransform.service(self._service) as service:
          service.ready(1)
          self._is_available = True
      except Exception:
        self._is_available = False
    return self._is_available

  def cache_artifacts(self):
    pass


class ExternalJavaProvider(ExternalProvider):
  def __init__(self, urns, jar_provider):
    super().__init__(
        urns, lambda: external.JavaJarExpansionService(jar_provider()))
    self._jar_provider = jar_provider

  def available(self):
    # pylint: disable=subprocess-run-check
    return subprocess.run(['which', 'java'],
                          capture_output=True).returncode == 0

  def cache_artifacts(self):
    return [self._jar_provider()]


@ExternalProvider.register_provider_type('python')
def python(urns, packages=()):
  if packages:
    return ExternalPythonProvider(urns, packages)
  else:
    return InlineProvider({
        name:
        python_callable.PythonCallableWithSource.load_from_fully_qualified_name(
            constructor)
        for (name, constructor) in urns.items()
    })


@ExternalProvider.register_provider_type('pythonPackage')
class ExternalPythonProvider(ExternalProvider):
  def __init__(self, urns, packages):
    super().__init__(urns, PypiExpansionService(packages))

  def available(self):
    return True  # If we're running this script, we have Python installed.

  def cache_artifacts(self):
    return [self._service._venv()]

  def create_external_transform(self, urn, args):
    # Python transforms are "registered" by fully qualified name.
    return external.ExternalTransform(
        "beam:transforms:python:fully_qualified_named",
        external.ImplicitSchemaPayloadBuilder({
            'constructor': urn,
            'kwargs': args,
        }).payload(),
        self._service)

  def _affinity(self, other: "Provider"):
    if isinstance(other, InlineProvider):
      return 50
    else:
      return super()._affinity(other)


# This is needed because type inference can't handle *args, **kwargs forwarding.
# TODO(BEAM-24755): Add support for type inference of through kwargs calls.
def fix_pycallable():
  from apache_beam.transforms.ptransform import label_from_callable

  def default_label(self):
    src = self._source.strip()
    last_line = src.split('\n')[-1]
    if last_line[0] != ' ' and len(last_line) < 72:
      return last_line
    return label_from_callable(self._callable)

  def _argspec_fn(self):
    return self._callable

  python_callable.PythonCallableWithSource.default_label = default_label
  python_callable.PythonCallableWithSource._argspec_fn = property(_argspec_fn)

  original_infer_return_type = trivial_inference.infer_return_type

  def infer_return_type(fn, *args, **kwargs):
    if isinstance(fn, python_callable.PythonCallableWithSource):
      fn = fn._callable
    return original_infer_return_type(fn, *args, **kwargs)

  trivial_inference.infer_return_type = infer_return_type

  original_fn_takes_side_inputs = (
      apache_beam.transforms.util.fn_takes_side_inputs)

  def fn_takes_side_inputs(fn):
    if isinstance(fn, python_callable.PythonCallableWithSource):
      fn = fn._callable
    return original_fn_takes_side_inputs(fn)

  apache_beam.transforms.util.fn_takes_side_inputs = fn_takes_side_inputs


class InlineProvider(Provider):
  def __init__(self, transform_factories):
    self._transform_factories = transform_factories

  def available(self):
    return True

  def cache_artifacts(self):
    pass

  def provided_transforms(self):
    return self._transform_factories.keys()

  def create_transform(self, type, args, yaml_create_transform):
    return self._transform_factories[type](**args)

  def to_json(self):
    return {'type': "InlineProvider"}


class MetaInlineProvider(InlineProvider):
  def create_transform(self, type, args, yaml_create_transform):
    return self._transform_factories[type](yaml_create_transform, **args)


PRIMITIVE_NAMES_TO_ATOMIC_TYPE = {
    py_type.__name__: schema_type
    for (py_type, schema_type) in schemas.PRIMITIVE_TO_ATOMIC_TYPE.items()
    if py_type.__module__ != 'typing'
}


def dicts_to_rows(o):
  if isinstance(o, dict):
    return beam.Row(**{k: dicts_to_rows(v) for k, v in o.items()})
  elif isinstance(o, list):
    return [dicts_to_rows(e) for e in o]
  else:
    return o


def create_builtin_provider():
  def create(elements: Iterable[Any], reshuffle: bool = True):
    """Creates a collection containing a specified set of elements.

    YAML/JSON-style mappings will be interpreted as Beam rows. For example::

        type: Create
        elements:
           - {first: 0, second: {str: "foo", values: [1, 2, 3]}}

    will result in a schema of the form (int, Row(string, List[int])).

    Args:
        elements: The set of elements that should belong to the PCollection.
            YAML/JSON-style mappings will be interpreted as Beam rows.
        reshuffle (optional): Whether to introduce a reshuffle if there is more
            than one element in the collection. Defaults to True.
    """
    return beam.Create(dicts_to_rows(elements), reshuffle)

  def with_schema(**args):
    # TODO: This is preliminary.
    def parse_type(spec):
      if spec in PRIMITIVE_NAMES_TO_ATOMIC_TYPE:
        return schema_pb2.FieldType(
            atomic_type=PRIMITIVE_NAMES_TO_ATOMIC_TYPE[spec])
      elif isinstance(spec, list):
        if len(spec) != 1:
          raise ValueError("Use single-element lists to denote list types.")
        else:
          return schema_pb2.FieldType(
              iterable_type=schema_pb2.IterableType(
                  element_type=parse_type(spec[0])))
      elif isinstance(spec, dict):
        return schema_pb2.FieldType(
            iterable_type=schema_pb2.RowType(schema=parse_schema(spec[0])))
      else:
        raise ValueError("Unknown schema type: {spec}")

    def parse_schema(spec):
      return schema_pb2.Schema(
          fields=[
              schema_pb2.Field(name=key, type=parse_type(value), id=ix)
              for (ix, (key, value)) in enumerate(spec.items())
          ],
          id=str(uuid.uuid4()))

    named_tuple = schemas.named_tuple_from_schema(parse_schema(args))
    names = list(args.keys())

    def extract_field(x, name):
      if isinstance(x, dict):
        return x[name]
      else:
        return getattr(x, name)

    return 'WithSchema(%s)' % ', '.join(names) >> beam.Map(
        lambda x: named_tuple(*[extract_field(x, name) for name in names])
    ).with_output_types(named_tuple)

  # Or should this be posargs, args?
  # pylint: disable=dangerous-default-value
  def fully_qualified_named_transform(constructor, args=(), kwargs={}):
    with FullyQualifiedNamedTransform.with_filter('*'):
      return constructor >> FullyQualifiedNamedTransform(
          constructor, args, kwargs)

  # This intermediate is needed because there is no way to specify a tuple of
  # exactly zero or one PCollection in yaml (as they would be interpreted as
  # PBegin and the PCollection itself respectively).
  class Flatten(beam.PTransform):
    def expand(self, pcolls):
      if isinstance(pcolls, beam.PCollection):
        pipeline_arg = {}
        pcolls = (pcolls, )
      elif isinstance(pcolls, dict):
        pipeline_arg = {}
        pcolls = tuple(pcolls.values())
      else:
        pipeline_arg = {'pipeline': pcolls.pipeline}
        pcolls = ()
      return pcolls | beam.Flatten(**pipeline_arg)

  class WindowInto(beam.PTransform):
    def __init__(self, windowing):
      self._window_transform = self._parse_window_spec(windowing)

    def expand(self, pcoll):
      return pcoll | self._window_transform

    @staticmethod
    def _parse_window_spec(spec):
      spec = dict(spec)
      window_type = spec.pop('type')
      # TODO: These are in seconds, perhaps parse duration strings meaningfully?
      if window_type == 'global':
        window_fn = window.GlobalWindows()
      elif window_type == 'fixed':
        window_fn = window.FixedWindows(spec.pop('size'), spec.pop('offset', 0))
      elif window_type == 'sliding':
        window_fn = window.SlidingWindows(
            spec.pop('size'), spec.pop('period'), spec.pop('offset', 0))
      elif window_type == 'sessions':
        window_fn = window.FixedWindows(spec.pop('gap'))
      if spec:
        raise ValueError(f'Unknown parameters {spec.keys()}')
      # TODO: Triggering, etc.
      return beam.WindowInto(window_fn)

  def log_and_return(x):
    logging.info(x)
    return x

  return InlineProvider(
      dict({
          'Create': create,
          'LogForTesting': lambda: beam.Map(log_and_return),
          'PyTransform': fully_qualified_named_transform,
          'PyToRow': lambda fields: beam.Select(
              **{
                  name: python_callable.PythonCallableWithSource(fn)
                  for (name, fn) in fields.items()
              }),
          'WithSchema': with_schema,
          'Flatten': Flatten,
          'WindowInto': WindowInto,
          'GroupByKey': beam.GroupByKey,
      }))


class PypiExpansionService:
  """Expands transforms by fully qualified name in a virtual environment
  with the given dependencies.
  """
  VENV_CACHE = os.path.expanduser("~/.apache_beam/cache/venvs")

  def __init__(self, packages, base_python=sys.executable):
    self._packages = packages
    self._base_python = base_python

  @classmethod
  def _key(cls, base_python, packages):
    return json.dumps({
        'binary': base_python, 'packages': sorted(packages)
    },
                      sort_keys=True)

  @classmethod
  def _path(cls, base_python, packages):
    return os.path.join(
        cls.VENV_CACHE,
        hashlib.sha256(cls._key(base_python,
                                packages).encode('utf-8')).hexdigest())

  @classmethod
  def _create_venv_from_scratch(cls, base_python, packages):
    venv = cls._path(base_python, packages)
    if not os.path.exists(venv):
      subprocess.run([base_python, '-m', 'venv', venv], check=True)
      venv_python = os.path.join(venv, 'bin', 'python')
      subprocess.run([venv_python, '-m', 'ensurepip'], check=True)
      subprocess.run([venv_python, '-m', 'pip', 'install'] + packages,
                     check=True)
      with open(venv + '-requirements.txt', 'w') as fout:
        fout.write('\n'.join(packages))
    return venv

  @classmethod
  def _create_venv_from_clone(cls, base_python, packages):
    venv = cls._path(base_python, packages)
    if not os.path.exists(venv):
      clonable_venv = cls._create_venv_to_clone(base_python)
      clonable_python = os.path.join(clonable_venv, 'bin', 'python')
      subprocess.run(
          [clonable_python, '-m', 'clonevirtualenv', clonable_venv, venv],
          check=True)
      venv_binary = os.path.join(venv, 'bin', 'python')
      subprocess.run([venv_binary, '-m', 'pip', 'install'] + packages,
                     check=True)
      with open(venv + '-requirements.txt', 'w') as fout:
        fout.write('\n'.join(packages))
    return venv

  @classmethod
  def _create_venv_to_clone(cls, base_python):
    return cls._create_venv_from_scratch(
        base_python, [
            'apache_beam[dataframe,gcp,test]==' + beam_version,
            'virtualenv-clone'
        ])

  def _venv(self):
    return self._create_venv_from_clone(self._base_python, self._packages)

  def __enter__(self):
    venv = self._venv()
    self._service_provider = subprocess_server.SubprocessServer(
        external.ExpansionAndArtifactRetrievalStub,
        [
            os.path.join(venv, 'bin', 'python'),
            '-m',
            'apache_beam.runners.portability.expansion_service_main',
            '--port',
            '{{PORT}}',
            '--fully_qualified_name_glob=*',
            '--pickle_library=cloudpickle',
            '--requirements_file=' + os.path.join(venv + '-requirements.txt')
        ])
    self._service = self._service_provider.__enter__()
    return self._service

  def __exit__(self, *args):
    self._service_provider.__exit__(*args)
    self._service = None


@ExternalProvider.register_provider_type('renaming')
class RenamingProvider(Provider):
  def __init__(self, transforms, mappings, underlying_provider):
    if isinstance(underlying_provider, dict):
      underlying_provider = ExternalProvider.provider_from_spec(
          underlying_provider)
    self._transforms = transforms
    self._underlying_provider = underlying_provider
    for transform in transforms.keys():
      if transform not in mappings:
        raise ValueError(f'Missing transform {transform} in mappings.')
    self._mappings = mappings

  def available(self) -> bool:
    return self._underlying_provider.available()

  def provided_transforms(self) -> Iterable[str]:
    return self._transforms.keys()

  def create_transform(
      self,
      typ: str,
      args: Mapping[str, Any],
      yaml_create_transform: Callable[
          [Mapping[str, Any], Iterable[beam.PCollection]], beam.PTransform]
  ) -> beam.PTransform:
    """Creates a PTransform instance for the given transform type and arguments.
    """
    mappings = self._mappings[typ]
    remapped_args = {
        mappings.get(key, key): value
        for key, value in args.items()
    }
    return self._underlying_provider.create_transform(
        self._transforms[typ], remapped_args, yaml_create_transform)

  def _affinity(self, other):
    raise NotImplementedError(
        'Should not be calling _affinity directly on this provider.')

  def underlying_provider(self):
    return self._underlying_provider.underlying_provider()


def parse_providers(provider_specs):
  providers = collections.defaultdict(list)
  for provider_spec in provider_specs:
    provider = ExternalProvider.provider_from_spec(provider_spec)
    for transform_type in provider.provided_transforms():
      providers[transform_type].append(provider)
      # TODO: Do this better.
      provider.to_json = lambda result=provider_spec: result
  return providers


def merge_providers(*provider_sets):
  result = collections.defaultdict(list)
  for provider_set in provider_sets:
    if isinstance(provider_set, Provider):
      provider = provider_set
      provider_set = {
          transform_type: [provider]
          for transform_type in provider.provided_transforms()
      }
    elif isinstance(provider_set, list):
      provider_set = merge_providers(*provider_set)
    for transform_type, providers in provider_set.items():
      result[transform_type].extend(providers)
  return result


def standard_providers():
  from apache_beam.yaml.yaml_mapping import create_mapping_providers
  from apache_beam.yaml.yaml_io import io_providers
  with open(os.path.join(os.path.dirname(__file__),
                         'standard_providers.yaml')) as fin:
    standard_providers = yaml.load(fin, Loader=SafeLoader)
  return merge_providers(
      create_builtin_provider(),
      create_mapping_providers(),
      io_providers(),
      parse_providers(standard_providers))
