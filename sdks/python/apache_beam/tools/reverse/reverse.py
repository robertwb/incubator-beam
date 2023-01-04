import collections
import re

import apache_beam as beam


class TransformWriter:
  def __init__(self, writer):
    self._writer = writer

  def define_transform(self):
    raise NotImplementedError(type(self))


well_known_transforms = {
    'beam:transform:group_by_key:v1': lambda payload: 'beam.GroupByKey()',
    'beam:transform:impulse:v1': lambda payload: 'beam.Impulse()',
}


def source_for(pipeline):
  print(pipeline)
  # Attempt do to input | Transform | Transform | ...
  # Too naive at identifying multiple consumers.
  chain = False

  pcolls = {}
  unique_names = set()
  root = 'p'
  # TODO: This should be local, and account for composites.
  consumers = collections.defaultdict(list)
  for transform_id, transform in pipeline.components.transforms.items():
    for tag, pcoll in transform.inputs.items():
      consumers[pcoll].append((transform_id, tag))

  def to_safe_name(s):
    if not s:
      s = 'Transform'
    s = re.sub('[^a-zA-Z_]', '_', s)
    if s in unique_names:
      counter = 1
      while f'{s}_{counter}' in unique_names:
        counter += 1
      s = f'{s}_{counter}'
    unique_names.add(s)
    return s

  def define_transform(writer, pcolls, transform_id):
    transform_proto = pipeline.components.transforms[transform_id]
    # TODO: We should let front-ends annotate how to construct the various
    # composites in various languages (or x-lang), likely via an entry in the
    # annotation map of the transform in the proto.
    # This should be easy to to for yaml, as we always have a "language-independent"
    # representation that has args + a provider.
    if transform_proto.spec.urn in well_known_transforms:
      # This works well for the very basic primitives.
      return well_known_transforms[transform_proto.spec.urn](
          transform_proto.spec.payload)
    elif transform_proto.subtransforms:
      # Composites that we don't know.
      if len(transform_proto.inputs) == 0:
        arg_name = 'p'
        local_pcolls = {}
      elif len(transform_proto.inputs) == 1:
        arg_name = 'input'
        local_pcolls = {
            next(iter(transform_proto.inputs.values())): 'input'
        }
      else:
        arg_name = 'inputs'
        local_pcolls = {
            pcoll: f'inputs["{name}"]'
            for pcoll,
            name in transform_proto.inputs.items()
        }
      transform_name = to_safe_name(transform_proto.unique_name)
      # TODO: Can we make parameterized transforms? Seems hard to pull out
      # what can be shared across differently parameterized composites, but
      # would be pretty cool.
      trasform_writer = SourceWriter(
          preamble=[
              f'class {transform_name}(beam.PTransform):',
              SourceWriter.INDENT,
              f'def expand({arg_name})',
              SourceWriter.INDENT,
          ])
      for subtransform in transform_proto.subtransforms:
        constructor = define_transform(
            trasform_writer, local_pcolls, subtransform)
        use_transform(trasform_writer, local_pcolls, subtransform, constructor)
      if len(transform_proto.outputs) == 0:
        pass
      elif len(transform_proto.outputs) == 1:
        trasform_writer.add_statement(
            f'return {local_pcolls[next(iter(transform_proto.outputs.values()))]}'
        )
      else:
        trasform_writer.add_statement(
            'return {%s}' + ', '.join(
                f'"{name}": local_pcolls[pcoll]' for name,
                pcoll in transform_proto.outputs))
      writer.add_define(trasform_writer)
      return transform_name + "()"
    else:
      return to_safe_name(transform_id) + '_TODO()'

  # TODO: All these params, plus duplicated above, indicate some kind of a
  # single scope object would be nice.
  def use_transform(writer, pcolls, transform_id, constructor):
    transform_proto = pipeline.components.transforms[transform_id]
    if len(transform_proto.inputs) == 0:
      inputs = root
    elif len(transform_proto.inputs) == 1:
      inputs = pcolls[next(iter(transform_proto.inputs.values()))]
    else:
      inputs = "{%s}" % ",".join([
          f'"{name}": {pcolls[input]}' for name,
          input in transform_proto.inputs.items()
      ])
    # TODO: Strip unique_name nesting...
    result = f'{inputs} | "{transform_proto.unique_name}" >> {constructor}'
    if chain and len(transform_proto.outputs) == 1 and len(consumers[next(iter(transform_proto.outputs.values()))]) == 1:
      pcolls[next(iter(transform_proto.outputs.values()))] = result
    elif not transform_proto.outputs or not any(consumers[pcoll] for pcoll in transform_proto.outputs.values()):
      writer.add_statement(result)
    else:
      # TODO: Better, but unique, names here?
      target = f'pcoll{len(pcolls)}'
      writer.add_statement(f'{target} = {result}')
      if len(transform_proto.outputs) == 1:
        pcolls[next(iter(transform_proto.outputs.values()))] = target
      else:
        pcolls.update(
            **{
                pcoll: f'{target}["{name}"]'
                for name,
                pcoll in transform_proto.outputs.items()
            })

  roots = pipeline.root_transform_ids
  while len(roots) == 1:
    roots = pipeline.components.transforms[next(iter(roots))].subtransforms

  pipeline_writer = SourceWriter(
      preamble=['with beam.Pipeline() as p:', SourceWriter.INDENT])

  for transform_id in roots:
    # Note the similarity here between the top-level and each transform.
    # TODO: Consolidate? (The primary difference is being in an expand
    # method vs. being in a with block.)
    constructor = define_transform(pipeline_writer, pcolls, transform_id)
    use_transform(pipeline_writer, pcolls, transform_id, constructor)

  return pipeline_writer.to_source()


class SourceWriter:

  INDENT = object()
  DEDENT = object()

  def __init__(self, preamble=[]):
    # Or have a separate scope object which could also track unique names.
    self._imports = set(['import apache_beam as beam'])
    self._defines = []
    self._statements = []
    self._preamble = preamble
    self._close_preamble = [self.DEDENT for line in self._preamble if line is self.INDENT]

  def add_statement(self, line):
    self._statements.append(line)

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
    yield from sorted(self._imports)
    yield ''
    yield from self._defines
    yield from self._preamble
    yield from self._statements
    yield from self._close_preamble

  def to_source_lines(self, indent=''):
    for line in self.to_source_statements():
      if line is self.INDENT:
        indent += '  '
      elif line is self.DEDENT:
        indent = indent[:-2]
      elif line:
        yield indent + line
      else:
        yield line

  def to_source(self):
    return '\n'.join(self.to_source_lines()) + '\n'


def run():
  p = beam.Pipeline()
  p | beam.Create([('a', 1), ('a', 2),
                   ('b', 3)], reshuffle=False) | beam.GroupByKey() | beam.Map(print)
  print(source_for(p.to_runner_api()))


if __name__ == '__main__':
  run()
