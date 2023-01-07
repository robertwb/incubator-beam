import apache_beam as beam
import argparse
import collections
import json
import re
import sys

import io
import subprocess

import google.protobuf.text_format


class TransformDefinerContext:
  def __init__(self, pipeline, language_bits, parent=None, transform_id=''):
    self.pipeline = pipeline
    self.language_bits = language_bits
    self.unique_names = parent.unique_names if parent else set()
    self.transform_id = transform_id
    if self.transform_id:
      self.transform_proto = pipeline.components.transforms[transform_id]
      self.subtransforms = self.transform_proto.subtransforms
      self.prefix = self.transform_proto.unique_name + '/'
    elif len(pipeline.root_transform_ids) == 1:
      # Hack to handle anonymous first transform.
      self.transform_id = next(iter(pipeline.root_transform_ids))
      self.transform_proto = pipeline.components.transforms[self.transform_id]
      self.subtransforms = self.transform_proto.subtransforms
      self.prefix = ''
    else:
      self.transform_proto = None
      self.subtransforms = pipeline.root_transform_ids
      self.prefix = ''

  def to_safe_name(self, s):
    if not s:
      s = 'Transform'
    s = re.sub('[^a-zA-Z_]', '_', s)
    if s in self.unique_names:
      counter = 1
      while f'{s}_{counter}' in self.unique_names:
        counter += 1
      s = f'{s}_{counter}'
    self.unique_names.add(s)
    return s

  def strip_prefix(self, name):
    assert name.startswith(self.prefix), (self.prefix, name)
    return name[len(self.prefix):]

  def sub_context(self, transform_id):
    return TransformDefinerContext(
        self.pipeline, self.language_bits, self, transform_id)

  def local_consumer_count(self, pcoll_id):
    count = 0
    for subtransform in self.subtransforms:
      for input in self.pipeline.components.transforms[
          subtransform].inputs.values():
        if input == pcoll_id:
          count += 1
    if self.transform_proto:
      for out in self.transform_proto.outputs.values():
        if out == pcoll_id:
          count += 1
    return count

  def define_transform(self, writer, transform_id):
    transform_proto = self.pipeline.components.transforms[transform_id]
    # TODO: We should let front-ends annotate how to construct the various
    # composites in various languages (or x-lang), likely via an entry in the
    # annotation map of the transform in the proto.
    # This should be easy to to for yaml, as we always have a "language-independent"
    # representation that has args + a provider.
    if transform_proto.spec.urn in self.language_bits.well_known_transforms:
      # This works well for the very basic primitives.
      return self.language_bits.well_known_transforms[transform_proto.spec.urn](
          transform_proto.spec.payload)
    elif 'yaml_args' in transform_proto.annotations:
      return self.language_bits.yaml_transform_definer.define_transform(
          writer, transform_id, self)
    elif transform_proto.subtransforms:
      return CompositeTransformDefiner().define_transform(
          writer, transform_id, self)
    else:
      return context.to_safe_name(transform_id + '_TODO') + "()"

  def use_transform(self, writer, pcolls, transform_id, constructor):
    transform_proto = self.pipeline.components.transforms[transform_id]
    if len(transform_proto.inputs) == 0:
      inputs = 'p'  # TODO: This only works at the top level.
    elif len(transform_proto.inputs) == 1:
      inputs = pcolls[next(iter(transform_proto.inputs.values()))]
    else:
      inputs = "{%s}" % ",".join([
          f'"{name}": {pcolls[input]}' for name,
          input in transform_proto.inputs.items()
      ])
    user_name = self.strip_prefix(transform_proto.unique_name)

    def is_default_name(user_name, constructor):
      # language bits?
      if constructor.startswith('new '):
        constructor = constructor[4:]
      if constructor.startswith('beam.'):
        constructor = constructor[5:]
      m = re.match('^([A-Za-z]+)[(](.*)[)]$', user_name)
      if m:
        if m.group(1) in constructor and m.group(2) in constructor:
          return True
      return constructor.startswith(user_name + '(') or constructor.startswith(
          user_name + '.')

    print(
        "ELIDE",
        user_name,
        constructor,
        is_default_name(user_name, constructor))
    if is_default_name(user_name, constructor):
      user_name = None
    result = self.language_bits.apply(inputs, user_name, constructor)
    local_consumer_counts = [
        self.local_consumer_count(pcoll)
        for pcoll in transform_proto.outputs.values()
    ]
    if self.language_bits.chain and local_consumer_counts == [1]:
      pcolls[next(iter(transform_proto.outputs.values()))] = result
    elif sum(local_consumer_counts) == 0:
      writer.add_statement(self.language_bits.maybe_parens(result))
    else:
      # TODO: Better, but unique, names here?
      target = f'pcoll{len(pcolls)}'
      writer.add_statement(
          self.language_bits.declare_and_assign_pcollection(
              target, None, f'{self.language_bits.maybe_parens(result)}'))
      if len(transform_proto.outputs) == 1:
        pcolls[next(iter(transform_proto.outputs.values()))] = target
      else:
        pcolls.update(
            **{
                pcoll: f'{target}["{name}"]'
                for name,
                pcoll in transform_proto.outputs.items()
            })


class TransformDefiner:
  def define_transform(self, code_writer, transform_proto, context):
    raise NotImplementedError(type(self))


class CompositeTransformDefiner(TransformDefiner):
  def define_transform(self, writer, transform_id, context):
    transform_proto = context.pipeline.components.transforms[transform_id]
    # Composites that we don't know.
    if len(transform_proto.inputs) == 0:
      arg_name = 'p'
      local_pcolls = {}
    elif len(transform_proto.inputs) == 1:
      arg_name = 'input'
      local_pcolls = {next(iter(transform_proto.inputs.values())): 'input'}
    else:
      arg_name = 'inputs'
      local_pcolls = {
          pcoll: f'inputs["{name}"]'
          for pcoll,
          name in transform_proto.inputs.items()
      }
    transform_name = context.to_safe_name(
        context.strip_prefix(transform_proto.unique_name))
    # TODO: Can we make parameterized transforms? Seems hard to pull out
    # what can be shared across differently parameterized composites, but
    # would be pretty cool.
    transform_writer = SourceWriter(
        context.language_bits,
        preamble=context.language_bits.declare_ptransform(
            transform_name,
            transform_proto.inputs,
            transform_proto.outputs,
            arg_name))
    sub_context = context.sub_context(transform_id)
    for subtransform in transform_proto.subtransforms:
      constructor = sub_context.define_transform(transform_writer, subtransform)
      sub_context.use_transform(
          transform_writer, local_pcolls, subtransform, constructor)
    if len(transform_proto.outputs) == 0:
      pass
    elif len(transform_proto.outputs) == 1:
      transform_writer.add_statement(
          f'return {context.language_bits.maybe_parens(local_pcolls[next(iter(transform_proto.outputs.values()))])}'
      )
    else:
      transform_writer.add_statement(
          'return {%s}' + ', '.join(
              f'"{name}": local_pcolls[pcoll]' for name,
              pcoll in transform_proto.outputs))
    writer.add_define(transform_writer)
    return context.language_bits.construct_transform(transform_name)


class PythonYamlTransformDefiner(TransformDefiner):
  def define_transform(self, writer, transform_id, context):
    transform_proto = context.pipeline.components.transforms[transform_id]
    yaml_type = transform_proto.annotations['yaml_type'].decode('utf-8')
    pos_args = []
    kwargs = json.loads(transform_proto.annotations['yaml_args'])
    # See the list in yaml's InlineProvider...
    if yaml_type == 'Create':
      transform_type = 'beam.Create'
      pos_args.append(kwargs.pop('elements'))
    elif yaml_type in ('PyMap',
                       'PyFlatMap',
                       'PyMapTuple',
                       'PyFlatMapTuple',
                       'PyFilter'):
      transform_type = 'beam.' + yaml_type[2:]
      source = kwargs.pop('fn')
      if not re.match('(lambda .*)|([a-zA-Z_0-9.]+)', source):
        source = 'PythonCallableWithSource(%r)' % source
      # TODO: Add a way to return that the name should be skipped.
      pos_args = [source]
    else:
      transform_type = yaml_type
    return '%s(%s)' % (
        transform_type,
        ', '.join([str(arg) for arg in pos_args] +
                  ["%s=%r" % item for item in kwargs.items()]),
    )


class JavaYamlTransformDefiner(TransformDefiner):
  def define_transform(self, writer, transform_id, context):
    transform_proto = context.pipeline.components.transforms[transform_id]
    yaml_type = transform_proto.annotations['yaml_type'].decode('utf-8')
    pos_args = []
    kwargs = json.loads(transform_proto.annotations['yaml_args'])
    # See the list in yaml's InlineProvider...
    if yaml_type == 'Create':
      return 'Create.of(%s)' % ', '.join(
          repr(e) for e in kwargs.pop('elements'))

    elif yaml_type in ('PyMap', 'PyFlatMap'):
      fn = kwargs.pop('fn')
      return f'PythonMap.viaMapFn("{fn}")'

    elif yaml_type == 'ReadFromText':
      assert len(kwargs) == 1
      return 'TextIO.read().from("%s")' % kwargs.pop('file_pattern')

    if yaml_type in ('PyMap',
                     'PyFlatMap',
                     'PyMapTuple',
                     'PyFlatMapTuple',
                     'PyFilter'):
      transform_type = 'beam.' + yaml_type[2:]
      source = 'PythonCallableSource.of(%r)' % kwargs.pop('fn')
      # TODO: Add a way to return that the name should be skipped.
      pos_args = [source]
    else:
      transform_type = yaml_type
    constructor = "PythonExternalTransform.from(%r)" % transform_type
    if pos_args:
      constructor += ".withArgs(%s)" % ", ".join(pos_args)

    def java_repr(o):
      if isinstance(o, str):
        return f'"{o}"'
      else:
        return repr(o)

    for key, value in kwargs.items():
      constructor += '.withKwarg("%s", %s)' % (key, java_repr(value))
    return constructor


class LanguageBits:
  chain = True

  def apply(self, input, name, transform):
    raise NotImplementedError(type(self))

  def construct_transform(self, name):
    raise NotImplementedError(type(self))

  def pretty(self, raw):
    return raw

  def braces(self):
    raise NotImplementedError(type(self))

  def l_brace(self):
    return (self.braces() or "")[:1]

  def r_brace(self):
    return (self.braces() or "")[-1:]

  def statement_end(self):
    raise NotImplementedError(type(self))

  def declare_pcollection(self, name, coder):
    raise NotImplementedError(type(self))

  def declare_and_assign_pcollection(self, name, coder, src):
    return f'{self.declare_pcollection(name, coder)} = {src}'

  def maybe_parens(self, s):
    return s

  def pre_pipeline(self):
    raise NotImplementedError(type(self))

  def post_pipeline(self):
    raise NotImplementedError(type(self))

  def declare_ptransform(self, name, inputs, outputs, arg_name):
    raise NotImplementedError(type(self))

  def standard_imports(self):
    return []

  def do_imports(self, non_beam_imports, beam_imports):
    yield from non_beam_imports
    yield ''
    yield from beam_imports


class PythonBits(LanguageBits):
  def standard_imports(self):
    return ['import apache_beam as beam']

  well_known_transforms = {
      'beam:transform:group_by_key:v1': lambda payload: 'beam.GroupByKey()',
      'beam:transform:impulse:v1': lambda payload: 'beam.Impulse()',
  }

  yaml_transform_definer = PythonYamlTransformDefiner()

  def construct_transform(self, name):
    return f"{name}()"

  def apply(self, input, name, transform):
    if name is None:
      return f'{input} | {transform}'
    else:
      return f"{input} | {repr(name)} >> {transform}"

  def pretty(self, raw):
    result = subprocess.run(['yapf'],
                            input=raw.encode('utf-8'),
                            capture_output=True)
    if result.returncode == 0:
      return result.stdout.decode('utf-8')
    else:
      print(result.stderr.decode('utf-8'))
      return raw

  def declare_pcollection(self, name, coder):
    return f"{name}"

  def braces(self):
    return None

  def statement_end(self):
    return ''

  def maybe_parens(self, s):
    if '|' in s and len(s) > 72:
      return '(%s)' % s.replace('|', '\n|')
    else:
      return s

  def pre_pipeline(self):
    return ['with beam.Pipeline() as p:', SourceWriter.INDENT]

  def post_pipeline(self):
    return []

  def declare_ptransform(self, name, inputs, outputs, arg_name):
    return [
        f'class {name}(beam.PTransform):',
        SourceWriter.INDENT,
        f'def expand(self, {arg_name}):',
        SourceWriter.INDENT
    ]


class JavaBits(LanguageBits):
  def standard_imports(self):
    return [
        'import org.apache.beam.sdk.Pipeline;',
        'import org.apache.beam.sdk.options.PipelineOptions;'
        'import org.apache.beam.sdk.options.PipelineOptionsFactory;'
    ]

  well_known_transforms = {
      'beam:transform:group_by_key:v1': lambda payload: 'GroupByKey.of()',
      'beam:transform:impulse:v1': lambda payload: 'Impulse.of()',
  }

  yaml_transform_definer = JavaYamlTransformDefiner()

  def construct_transform(self, name):
    return f"new {name}()"

  def pretty(self, raw):
    result = subprocess.run(
        ['clang-format', '-assume-filename=MyBeamPipeline.java'],
        input=raw.encode('utf-8'),
        capture_output=True)
    if result.returncode == 0:
      return result.stdout.decode('utf-8')
    else:
      print(result.stderr.decode('utf-8'))
      return raw

  def apply(self, input, name, transform):
    if name is None:
      return f'{input}.apply({transform})'
    else:
      return f'{input}.apply("{name}", {transform})'

  def braces(self):
    return '{}'

  def statement_end(self):
    return ';'

  def declare_pcollection(self, name, coder):
    return f"PCollection<?> {name}"

  def pre_pipeline(self):
    return [
        'public class MyBeamPipeline',
        SourceWriter.INDENT,
        'public static void main(String[] args) ',
        SourceWriter.INDENT,
        'PipelineOptions options = PipelineOptionsFactory.create();',
        'Pipeline p = Pipeline.create(options);',
    ]

  def post_pipeline(self):
    return ['p.run().waitUntilFinish()']

  def declare_ptransform(self, name, inputs, outputs, arg_name):
    if len(inputs) == 0:
      input_type = 'PBegin'
    elif len(inputs) == 1:
      input_type = 'PCollection<?>'
    else:
      input_type = 'PCollectionTuple'
    if len(outputs) == 0:
      output_type = 'PDone'
    elif len(outputs) == 1:
      output_type = 'PCollection<?>'
    else:
      output_type = 'PCollectionTuple'
    return [
        f"class {name} extends PTransform<{input_type}, {output_type}>",
        SourceWriter.INDENT,
        f"public {output_type} expand({input_type} {arg_name})",
        SourceWriter.INDENT,
    ]


class GoYamlTransformDefiner(TransformDefiner):
  def define_transform(self, writer, transform_id, context):
    transform_proto = context.pipeline.components.transforms[transform_id]
    yaml_type = transform_proto.annotations['yaml_type'].decode('utf-8')
    pos_args = []
    kwargs = json.loads(transform_proto.annotations['yaml_args'])
    # See the list in yaml's InlineProvider...
    if yaml_type == 'Create':
      return 'beam.Create({SCOPE}, %s)' % ', '.join(
          repr(e) for e in kwargs.pop('elements'))

    elif yaml_type in ('PyMap', 'PyFlatMap'):
      fn = kwargs.pop('fn')
      return f'{yaml_type}({{SCOPE}}, "{fn}", {{INPUT}})'

    elif yaml_type == 'ReadFromText':
      assert len(kwargs) == 1
      return 'textio.Read({SCOPE}, "%s")' % kwargs.pop('file_pattern')

    if yaml_type in ('PyMap',
                     'PyFlatMap',
                     'PyMapTuple',
                     'PyFlatMapTuple',
                     'PyFilter'):
      transform_type = 'beam.' + yaml_type[2:]
      source = 'PythonCallableSource.of(%r)' % kwargs.pop('fn')
      # TODO: Add a way to return that the name should be skipped.
      pos_args = [source]
    else:
      transform_type = yaml_type
    pos_args_str = ", ".join(list(pos_args) + list(kwargs.values()))
    constructor = 'PythonExternalTransform({{SCOPE}}, "{transform_type}", {pos_args_str}, {{INPUT}})' % transform_type

    def java_repr(o):
      if isinstance(o, str):
        return f'"{o}"'
      else:
        return repr(o)

    for key, value in kwargs.items():
      constructor += '.withKwarg("%s", %s)' % (key, java_repr(value))
    return constructor


class GoBits(LanguageBits):
  chain = False

  def standard_imports(self):
    return [
        "context",
        "flag",
        "log",
        "strings",
        "github.com/apache/beam/sdks/v2/go/pkg/beam",
        "github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio",
        "github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx",
    ]

  well_known_transforms = {
      'beam:transform:group_by_key:v1': lambda payload:
      'beam.GroupByKey({SCOPE}, {INPUT})',
      'beam:transform:impulse:v1': lambda payload:
      'beam.GroupByKey({SCOPE}, {INPUT})',
  }

  def do_imports(self, non_beam_imports, beam_imports):
    yield 'package main'  # hack
    yield ''
    yield 'import ('
    for x in non_beam_imports:
      yield f'\t"{x}"'
    yield ''
    for x in beam_imports:
      yield f'\t"{x}"'
    yield ')'

  yaml_transform_definer = GoYamlTransformDefiner()

  def construct_transform(self, name):
    return f"{name}({{SCOPE}}, {{INPUT}})"

  def pretty(self, raw):
    raw = raw.replace('\n{', ' {')
    result = subprocess.run(['gofmt', '-s'],
                            input=raw.encode('utf-8'),
                            capture_output=True)
    if result.returncode == 0:
      return result.stdout.decode('utf-8')
    else:
      print('\n'.join(str(s) for s in list(enumerate(raw.split('\n')))))
      print(result.stderr.decode('utf-8'))
      return raw

  def apply(self, input, name, transform):
    # TODO: Multi-input.
    if input == 'p':
      # implicit pipeline
      transform = transform.replace(', {INPUT}', '')
    if name is None:
      return transform.format(SCOPE='s', INPUT=input)
    else:
      return transform.format(SCOPE=f's.Scope("{name}")', INPUT=input)

  def braces(self):
    return '{}'

  def statement_end(self):
    return ''

  def declare_and_assign_pcollection(self, name, coder, src):
    return f'{name} := {src}'

  def pre_pipeline(self):
    return [
        'func main()',
        SourceWriter.INDENT,
        'flag.Parse()',
        'beam.Init()',
        'p := beam.NewPipeline()',
        's := p.Root()',
    ]

  def post_pipeline(self):
    return [
        'err := beamx.Run(context.Background(), p)',
        'if err != nil {',
        '  log.Fatalf("Failed to execute job: %v", err)'
        '}'
    ]

  def declare_ptransform(self, name, inputs, outputs, arg_name):
    if len(inputs) == 0:
      input_type = ''
    elif len(inputs) == 1:
      input_type = 'beam.PCollection'
    else:
      input_type = 'beam.PCollectionTuple'
    if len(outputs) == 0:
      output_type = 'PDone'
    elif len(outputs) == 1:
      output_type = 'beam.PCollection'
    else:
      output_type = 'beam.PCollectionTuple'
    if input_type:
      input_arg = f', {arg_name} {input_type}'
    else:
      input_arg = ''
    return [
        f"func {name}(s beam.Scope{input_arg}) {output_type}",
        SourceWriter.INDENT,
    ]


def source_for(pipeline, langauge_bits=None):
  if langauge_bits is None:
    print(pipeline)
    return '\n\n----------------------\n\n'.join(
        source_for(pipeline, bits)
        for bits in [JavaBits(), PythonBits(), GoBits()])

  pcolls = {}
  context = TransformDefinerContext(pipeline, langauge_bits)
  # TODO: This should be local, and account for composites.
  consumers = collections.defaultdict(list)
  for transform_id, transform in pipeline.components.transforms.items():
    for tag, pcoll in transform.inputs.items():
      consumers[pcoll].append((transform_id, tag))


#   context.define_transform = define_transform
#   context.use_transform = use_transform

# The top-level one is typically artificial.
  roots = pipeline.root_transform_ids
  top = True
  outputs = {}
  while len(roots) == 1:
    transform_id = next(iter(roots))
    if top:
      top = False
    else:
      outputs = pipeline.components.transforms[transform_id].outputs
      context = context.sub_context(transform_id)
    roots = pipeline.components.transforms[transform_id].subtransforms

  # Note the similarity here between the top-level and each transform.
  # TODO: Consolidate? (The primary difference is being in an expand
  # method vs. being in a with block.)
  pipeline_writer = SourceWriter(
      context.language_bits, preamble=context.language_bits.pre_pipeline())

  for transform_id in roots:
    # Note the similarity here between the top-level and each transform.
    # TODO: Consolidate? (The primary difference is being in an expand
    # method vs. being in a with block.)
    constructor = context.define_transform(pipeline_writer, transform_id)
    context.use_transform(pipeline_writer, pcolls, transform_id, constructor)

  if context.language_bits.chain and len(outputs) == 1:
    # This was considered "consumed" buy the "return" of the inner transform.
    pipeline_writer.add_statement(
        maybe_parens(pcolls[next(iter(outputs.values()))]))

  for statement in context.language_bits.post_pipeline():
    pipeline_writer.add_statement(statement)

  return pipeline_writer.to_source()


class SourceWriter:

  INDENT = object()
  DEDENT = object()

  def __init__(self, language_bits, preamble=[]):
    # Or have a separate scope object which could also track unique names.
    self.language_bits = language_bits
    self._imports = set(language_bits.standard_imports())
    self._defines = []
    self._statements = []
    self._preamble = preamble
    self._close_preamble = [
        self.DEDENT for line in self._preamble if line is self.INDENT
    ]

  def add_statement(self, line):
    self._statements.append(line + self.language_bits.statement_end())

  def add_define(self, line_or_writer):
    if isinstance(line_or_writer, str):
      self._defines.append(line)
    elif isinstance(line_or_writer, SourceWriter):
      for import_ in line_or_writer._imports:
        self._imports.add(import_)
      self._defines.append('')
      # TODO: Fix redundancy with to_source_statements.
      self._defines.extend(line_or_writer._defines)
      self._defines.extend(line_or_writer._preamble)
      self._defines.extend(line_or_writer._statements)
      self._defines.extend(line_or_writer._close_preamble)
      self._defines.append('')
    else:
      raise TypeEror(type(line_or_writer))

  def to_source_statements(self):
    is_beam_import = lambda x: 'apache' in x and 'beam' in x
    yield from self.language_bits.do_imports(
        sorted(x for x in self._imports if not is_beam_import(x)),
        sorted(x for x in self._imports if is_beam_import(x)),
    )
    yield ''
    yield from self._defines
    yield from self._preamble
    yield from self._statements
    yield from self._close_preamble

  def to_source_lines(self, indent=''):
    for line in self.to_source_statements():
      if line is self.INDENT:
        if self.language_bits.l_brace():
          yield self.language_bits.l_brace()
        indent += '  '
      elif line is self.DEDENT:
        indent = indent[:-2]
        if self.language_bits.r_brace():
          yield self.language_bits.r_brace()
      elif line:
        yield indent + line
      else:
        yield line

  def to_source(self):
    return self.language_bits.pretty('\n'.join(self.to_source_lines()) + '\n')


def create_python_pipeline():
  p = beam.Pipeline()
  p | beam.Create([('a', 1), ('a', 2), ('b', 3)],
                  reshuffle=False) | beam.GroupByKey() | beam.Map(print)
  return p


def create_yaml_pipeline():
  from apache_beam.yaml.yaml_transform import YamlTransform
  p = beam.Pipeline()
  p | YamlTransform(
      '''
          type: chain
          transforms:
            - type: Create
              elements: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            - type: PyMap
              fn: "lambda x: x * x + x"
            - type: PyMap
              fn: "lambda x: x + 41"
          ''')
  return p


def create_yaml2_pipeline():
  from apache_beam.yaml.yaml_transform import YamlTransform
  p = beam.Pipeline()
  p | YamlTransform(
      '''
      type: composite
      transforms:
        - name: Foo
          type: chain
          transforms:
            - type: Create
              elements: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            - type: PyMap
              fn: "lambda x: x * x + x"
            - type: PyMap
              fn: "lambda x: x + 41"
        - type: PyMap
          input: Foo
          fn: math.sin
        - type: PyMap
          input: Foo
          fn: math.cos

        - type: ReadFromText
          name: DoRead
          file_pattern: "/tmp/*.txt"
        - type: chain
          name: WordsByFirstLetter
          input: DoRead
          transforms:
            - type: PyMap
              fn: str.lower
            - type: PyFlatMap
              fn: "lambda line: line.split()"
            - type: PyMap
              fn: "lambda word: (word[0], word)"
            - type: GroupByKey
        - type: PyMap
          input: WordsByFirstLetter
          fn: print
          ''')
  return p


def run():
  parser = argparse.ArgumentParser(
      prog='Reverse',
      description='Create a Python template based on a Beam runner API proto')
  parser.add_argument('filename', nargs='?')
  parser.add_argument('--output', default='python')
  parser.add_argument('--source', default='python')
  args = parser.parse_args()

  if args.filename:
    if args.filename == '-':
      proto = google.protobuf.text_format.Parse(
          sys.stdin.read(), beam.beam_runner_api_pb2.Pipeline())
    else:
      with open(args.filename, 'rb') as file:
        proto = google.protobuf.text_format.Parse(
            file.read(), beam.beam_runner_api_pb2.Pipeline())
  else:
    if args.source == 'python':
      pipeline_object = create_python_pipeline()
    elif args.source == 'yaml':
      pipeline_object = create_yaml_pipeline()
    elif args.source == 'yaml2':
      pipeline_object = create_yaml2_pipeline()
    else:
      raise ValueError("Unknown source type: " + args.source)
    proto = pipeline_object.to_runner_api()

  print(source_for(proto))


if __name__ == '__main__':
  run()
