import apache_beam as beam
import argparse
import collections
import google.protobuf.text_format
import re
import sys


class TransformWriter:
  def __init__(self, writer):
    self._writer = writer

  def define_transform(self):
    raise NotImplementedError(type(self))


well_known_transforms = {
    'beam:transform:group_by_key:v1': lambda payload: 'beam.GroupByKey()',
    'beam:transform:impulse:v1': lambda payload: 'beam.Impulse()',
}


class Graph:
  def __init__(self):
    self.nodes = {}
    self.producers = {}
    self.parent = {}
    self.depth = {}

  def to_dot(self):
    names = {}
    def esc(v):
      if v in names:
        return names[v]
      new = re.sub('[^a-zA-Z0-9_]', '_', v) + str(len(names))
      assert new not in names
      names[v] = new
      return new

    def dot():
      yield('digraph {')
      for k in self.nodes:
        yield(f'{esc(k)} [label="{k}"]');
      for (k,v) in self.nodes.items():
        for child in v.children:
          yield(f'{esc(k)} -> {esc(child)}')
      yield('}')
    return "\n".join(dot())

class Node:
  def __init__(self, key):
    self.key = key
    self.children = []
    self.parents = []
  def __repr__(self):
    return str(vars(self))


def make_graph(pipeline):
  graph = Graph()

  transforms = pipeline.components.transforms

  def build_parents(root, n):
    graph.depth[root] = n
    for t in transforms[root].subtransforms:
      graph.parent[t] = root
      build_parents(t, n+1)

  for id in pipeline.root_transform_ids:
    build_parents(id, 0)

  # Create nodes
  for (k,v) in pipeline.components.transforms.items():
    node = Node(k)
    node.transform = v
    assert k not in graph.nodes
    graph.nodes[k] = node

  # Map output pcollections to its producer
  for key in transforms:
    for (k,v) in transforms[key].outputs.items():
      if v in transforms[key].inputs.values():
        # This operation is its own input and output: probably a reshuffle?
        continue
      kv = (v, graph.depth[key])
      assert kv not in graph.producers, "Already produced by " + str(graph.producers[kv])
      graph.producers[kv] = key

  # Build tree
  for (k,t) in pipeline.components.transforms.items():
    for (_,inp) in t.inputs.items():
      kv = (inp, graph.depth[k])
      while kv not in graph.producers:
        kv = (kv[0], kv[1]-1)
        assert kv[1] >= 0, f"Can't find {inp} from depth {graph.depth[k]}" 

      producer = graph.producers[kv]
      graph.nodes[producer].children.append(k)
      graph.nodes[k].parents.append(producer)

  return graph

class CodeNode:
  def __init__(self, t):
    self.transform = t
    self.refcount = 0
    self.method = False
    self.children = []
    self.comment = None
    self.label = None
    self.operation = None

  def __repr__(self):
    return str(vars(self))

def get_code_nodes(pipeline):
  nodes = {}

  def get(transform):
    assert isinstance(transform, str)
    if transform not in nodes:
      nodes[transform] = CodeNode(transform)
    res = nodes[transform]
    res.refcount += 1
    return res

  def add(root):
    t = pipeline.components.transforms[root]
    urn = t.spec.urn if t.spec is not None else ""

    if False:
      pass
    elif urn == "beam:transform:generic_composite:v1" and t.unique_name.endswith("/Write"):
      result = get(root)
      result.label = "WriteTODO"
      result.operation = "beam.io.WriteToTODO(TODO)"

    elif urn == "beam:transform:generic_composite:v1" and len(t.subtransforms) == 1 and False:
      result = add(t.subtransforms[0])
      nodes[root] = result
      result.label = t.unique_name
      result.comment = t.spec.payload

    elif urn == "beam:transform:generic_composite:v1" or urn == "":
      result = get(root)
      result.method = len(t.subtransforms) > 1
      for sub in t.subtransforms:
        child = add(sub)
        result.children.append(child)

      name = t.unique_name if urn != "" else "pipeline"
      result.label = f'TODO {name}'
      result.operation = f'{name}()'
      result.name = name

    elif urn == "beam:transform:read:v1":
      result = get(root)
      result.label = "ReadTODO"
      result.operation = "beam.io.ReadFromTODO(TODO)"

    elif urn == "beam:transform:pardo:v1":
      assert len(t.subtransforms) == 0, "ParDo has subtransforms"
      result = get(root)
      result.comment = t.unique_name
      result.operation = "beam.TODOMap(TODO)"


    else:
      assert False, "Unhandled urn: " + urn

    return result

  for id in pipeline.root_transform_ids:
    add(id)
  return nodes

def topsort(items):
  res = []
  while len(items) > 0:
    found=False
    for k,v in items.items():
      if len(v) == 0:
        res.append(k)
        del items[k]
        items = { k2: list(filter(lambda x: x == v, v2,)) for k2,v2 in items.items() }
        found = True
        break

    assert found, items
  return res

def order(nodes):
  deps = {}
  for n in nodes:
    deps[n.key] = list(n.parents)
  return topsort(deps)

def codegen(pipeline, graph, codenodes):
  refs = {}
  source = SourceWriter()

  for c in codenodes.values():
    if c.method:
      name = c.name
      lines = []
      lines.append(f'class {name}(beam.PTransform):')
      lines.append(SourceWriter.INDENT)
      if c.comment:
        lines.append(f'""" {c.comment} """')
      lines.append(f'def expand(self, input):')
      lines.append(SourceWriter.INDENT)
      topsort = order(map(lambda x: graph.nodes[x.transform], c.children))

      vars = {}
      for child in topsort:
        cn = codenodes[child]
        gn = graph.nodes[child]

        assert len(gn.parents) <= 1

        if not gn.parents:
          input = "input"
        else:
          if gn.parents[0] in vars:
            input = vars[gn.parents[0]]

        name = "var" + str(len(vars))
        vars[gn.key] = name
        lines.append(f'{name} = {input} | "{cn.label}" >> {cn.operation}')

      lines.append(f'return {vars[child]}')
      lines.append(SourceWriter.DEDENT)
      lines.append(SourceWriter.DEDENT)

      for f in lines:
        source.add_statement(f)
  return source


def source_for(pipeline):
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
      transform_writer = SourceWriter(
          preamble=[
              f'class {transform_name}(beam.PTransform):',
              SourceWriter.INDENT,
              f'def expand({arg_name})',
              SourceWriter.INDENT,
          ])
      for subtransform in transform_proto.subtransforms:
        constructor = define_transform(
            transform_writer, local_pcolls, subtransform)
        use_transform(transform_writer, local_pcolls, subtransform, constructor)
      if len(transform_proto.outputs) == 0:
        pass
      elif len(transform_proto.outputs) == 1:
        transform_writer.add_statement(
            f'return {local_pcolls[next(iter(transform_proto.outputs.values()))]}'
        )
      else:
        transform_writer.add_statement(
            'return {%s}' + ', '.join(
                f'"{name}": local_pcolls[pcoll]' for name,
                pcoll in transform_proto.outputs))
      writer.add_define(transform_writer)
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


def create_pipeline():
  p = beam.Pipeline()
  p | beam.Create([('a', 1), ('a', 2),
                   ('b', 3)], reshuffle=False) | beam.GroupByKey() | beam.Map(print)
  return p

def create_pipeline():
  p = beam.Pipeline()
  x = p | "MyRead" >> beam.io.ReadFromText("/dev/null")
  x = x | beam.Map(lambda x: x)
  x = x | "MyWrite" >> beam.io.WriteToText("/dev/null")
  return p

def run():
  parser = argparse.ArgumentParser(
      prog = 'Reverse',
      description = 'Create a Python template based on a Beam runner API proto')
  parser.add_argument('filename', nargs='?')
  args = parser.parse_args()

  if args.filename:
    if args.filename == '-':
      proto = google.protobuf.text_format.Parse(sys.stdin.read(), beam.beam_runner_api_pb2.Pipeline())
    else:
      with open(args.filename, 'rb') as file:
        proto = google.protobuf.text_format.Parse(file.read(), beam.beam_runner_api_pb2.Pipeline())
  else:
    proto = create_pipeline().to_runner_api()

  graph = make_graph(proto)
  codenodes = get_code_nodes(proto)
  print(codegen(proto, graph, codenodes).to_source())
  sys.exit(0)

if __name__ == '__main__':
  run()
