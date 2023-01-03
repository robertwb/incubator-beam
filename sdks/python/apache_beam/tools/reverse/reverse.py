import re

import apache_beam as beam


class TransformWriter:
  def __init__(self, writer):
    self._writer = writer

  def define_transform(self):
    raise NotImplementedError(type(self))


well_known_transforms = {
    'beam:transform:group_by_key:v1': lambda payload: 'beam.GroupByKey()'
}


def source_for(pipeline):
  print(pipeline)
  pcolls = {}
  unique_names = set()
  root = 'p'

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
    if transform_proto.spec.urn in well_known_transforms:
      return well_known_transforms[transform_proto.spec.urn](
          transform_proto.spec.payload)
    elif transform_proto.subtransforms:
      if len(transform_proto.inputs) == 0:
        arg_name = 'root'
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
      return to_safe_name(transform_id) + '()'

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
    base = f'pcoll{len(pcolls)}'
    if len(transform_proto.outputs) == 0:
      assignment = ''
      post_assignment = []
    elif len(transform_proto.outputs) == 1:
      assignment = f'{base} = '
      post_assignment = []
      pcolls[next(iter(transform_proto.outputs.values()))] = base
    else:
      assignment = f'{base} = '
      post_assignment = [
          f'{base}_{name} = {base}["name"]' for name,
          _ in transform_proto.outputs.items()
      ]
      pcolls.update(
          **{
              pcoll: f'{base}_{name}'
              for name,
              pcoll in transform_proto.outputs.items()
          })
    # TODO: Strip unique_name nesting...
    writer.add_statement(
        f'{assignment}{inputs} | "{transform_proto.unique_name}" >> {constructor}'
    )
    for line in post_assignment:
      writer.add_statement(line)

  roots = pipeline.root_transform_ids
  while len(roots) == 1:
    roots = pipeline.components.transforms[next(iter(roots))].subtransforms

  pipeline_writer = SourceWriter(
      preamble=['with beam.Pipeline() as p:', SourceWriter.INDENT])

  for transform_id in roots:
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
      # Fix redundancy with to_source_statements.
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
                   ('b', 3)]) | beam.GroupByKey() | beam.Map(print)
  print(source_for(p.to_runner_api()))


if __name__ == '__main__':
  run()
