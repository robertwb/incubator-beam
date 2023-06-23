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

import glob
import logging
import os
import tempfile
import unittest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_transform import YamlTransform


class YamlTransformTest(unittest.TestCase):
  def test_composite(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create([1, 2, 3])
      # TODO(robertwb): Consider making the input implicit (and below).
      result = elements | YamlTransform(
          '''
          type: composite
          input:
              elements: input
          transforms:
            - type: PyMap
              name: Square
              input: elements
              fn: "lambda x: x * x"
            - type: PyMap
              name: Cube
              input: elements
              fn: "lambda x: x * x * x"
            - type: Flatten
              input: [Square, Cube]
          output:
              Flatten
          ''')
      assert_that(result, equal_to([1, 4, 9, 1, 8, 27]))

  def test_chain_with_input(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      elements = p | beam.Create(range(10))
      result = elements | YamlTransform(
          '''
          type: chain
          input:
              elements: input
          transforms:
            - type: PyMap
              fn: "lambda x: x * x + x"
            - type: PyMap
              fn: "lambda x: x + 41"
          ''')
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def test_chain_with_source_sink(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          source:
            type: Create
            elements: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
          transforms:
            - type: PyMap
              fn: "lambda x: x * x + x"
          sink:
            type: PyMap
            fn: "lambda x: x + 41"
          ''')
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def test_chain_with_root(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
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
      assert_that(result, equal_to([41, 43, 47, 53, 61, 71, 83, 97, 113, 131]))

  def test_implicit_flatten(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: Create
              name: CreateSmall
              elements: [1, 2, 3]
            - type: Create
              name: CreateBig
              elements: [100, 200]
            - type: PyMap
              input: [CreateBig, CreateSmall]
              fn: "lambda x: x * x"
          output: PyMap
          ''')
      assert_that(result, equal_to([1, 4, 9, 10000, 40000]))

  def test_csv_to_json(self):
    try:
      import pandas as pd
    except ImportError:
      raise unittest.SkipTest('Pandas not available.')

    with tempfile.TemporaryDirectory() as tmpdir:
      data = pd.DataFrame([
          {
              'label': '11a', 'rank': 0
          },
          {
              'label': '37a', 'rank': 1
          },
          {
              'label': '389a', 'rank': 2
          },
      ])
      input = os.path.join(tmpdir, 'input.csv')
      output = os.path.join(tmpdir, 'output.json')
      data.to_csv(input, index=False)

      with beam.Pipeline() as p:
        result = p | YamlTransform(
            '''
            type: chain
            transforms:
              - type: ReadFromCsv
                path: %s
              - type: WriteToJson
                path: %s
                num_shards: 1
            ''' % (repr(input), repr(output)))

      output_shard = list(glob.glob(output + "*"))[0]
      result = pd.read_json(
          output_shard, orient='records',
          lines=True).sort_values('rank').reindex()
      pd.testing.assert_frame_equal(data, result)

  def test_name_is_not_ambiguous(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
            type: composite
            transforms:
              - type: Create
                name: Create
                elements: [0, 1, 3, 4]
              - type: PyFilter
                name: Filter
                keep: "lambda elem: elem > 2"
                input: Create
            output: Filter
            ''')
      # No exception raised
      assert_that(result, equal_to([3, 4]))

  def test_name_is_ambiguous(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      # pylint: disable=expression-not-assigned
      with self.assertRaises(ValueError):
        p | YamlTransform(
            '''
            type: composite
            transforms:
              - type: Create
                name: CreateData
                elements: [0, 1, 3, 4]
              - type: PyFilter
                name: PyFilter
                keep: "lambda elem: elem > 2"
                input: CreateData
              - type: PyFilter
                name: AnotherFilter
                keep: "lambda elem: elem > 3"
                input: PyFilter
            output: AnotherFilter
            ''')


class CreateTimestamped(beam.PTransform):
  def __init__(self, elements):
    self._elements = elements

  def expand(self, p):
    return (
        p
        | beam.Create(self._elements)
        | beam.Map(lambda x: beam.transforms.window.TimestampedValue(x, x)))


class SumGlobally(beam.PTransform):
  def expand(self, pcoll):
    return pcoll | beam.CombineGlobally(sum).without_defaults()


class SizeLimiter(beam.PTransform):
  def __init__(self, limit, error_handling):
    self._limit = limit
    self._error_handling = error_handling

  def expand(self, pcoll):
    def raise_on_big(element):
      if len(element) > self._limit:
        raise ValueError(element)
      else:
        return element

    good, bad = pcoll | beam.Map(raise_on_big).with_exception_handling()
    return {'small_elements': good, self._error_handling['output']: bad}


TEST_PROVIDERS = {
    'CreateTimestamped': CreateTimestamped,
    'SumGlobally': SumGlobally,
    'SizeLimiter': SizeLimiter,
}


class ErrorHandlingTest(unittest.TestCase):
  def test_error_handling_outputs(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: Create
              elements: ['a', 'b', 'biiiiig']
            - type: SizeLimiter
              limit: 5
              input: Create
              error_handling:
                output: errors
            - name: TrimErrors
              type: PyMap
              input: SizeLimiter.errors
              fn: "lambda x: x[1][1]"
          output:
            good: SizeLimiter
            bad: TrimErrors
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result['good'], equal_to(['a', 'b']), label="CheckGood")
      assert_that(result['bad'], equal_to(["ValueError('biiiiig')"]))

  def test_must_handle_error_output(self):
    with self.assertRaisesRegex(Exception, 'Unconsumed error output .*line 6'):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        _ = p | YamlTransform(
            '''
            type: composite
            transforms:
              - type: Create
                elements: ['a', 'b', 'biiiiig']
              - type: SizeLimiter
                limit: 5
                input: Create
                error_handling:
                  output: errors
            ''',
            providers=TEST_PROVIDERS)

  def test_mapping_errors(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: Create
              elements: [0, 1, 2, 4]
            - type: PyMap
              name: ToRow
              input: Create
              fn: "lambda x: beam.Row(num=x, str='a' * x or 'bbb')"
            - type: MapToFields
              name: MapWithErrorHandling
              input: ToRow
              language: python
              fields:
                num: num
                inverse: float(1 / num)
              keep:
                str[1] >= 'a'
              error_handling:
                output: errors
            - type: PyMap
              name: TrimErrors
              input: MapWithErrorHandling.errors
              fn: "lambda x: x.msg"
            - type: MapToFields
              name: Sum
              language: python
              input: MapWithErrorHandling
              append: True
              fields:
                sum: num + inverse
          output:
            good: Sum
            bad: TrimErrors
          ''',
          providers=TEST_PROVIDERS)
      assert_that(
          result['good'],
          equal_to([
              beam.Row(num=2, inverse=.5, sum=2.5),
              beam.Row(num=4, inverse=.25, sum=4.25)
          ]),
          label="CheckGood")
      assert_that(
          result['bad'],
          equal_to([
              "IndexError('string index out of range')",  # from the filter
              "ZeroDivisionError('division by zero')",  # from the mapping
          ]),
          label='CheckErrors')


class YamlWindowingTest(unittest.TestCase):
  def test_explicit_window_into(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              elements: [0, 1, 2, 3, 4, 5]
            - type: WindowInto
              windowing:
                type: fixed
                size: 4
            - type: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_input(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              elements: [0, 1, 2, 3, 4, 5]
            - type: SumGlobally
              windowing:
                type: fixed
                size: 4
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_multiple_inputs(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: composite
          transforms:
            - type: CreateTimestamped
              name: Create1
              elements: [0, 2, 4]
            - type: CreateTimestamped
              name: Create2
              elements: [1, 3, 5]
            - type: SumGlobally
              input: [Create1, Create2]
              windowing:
                type: fixed
                size: 4
          output: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_output(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              elements: [0, 1, 2, 3, 4, 5]
              windowing:
                type: fixed
                size: 4
            - type: SumGlobally
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))

  def test_windowing_on_outer(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      result = p | YamlTransform(
          '''
          type: chain
          transforms:
            - type: CreateTimestamped
              elements: [0, 1, 2, 3, 4, 5]
            - type: SumGlobally
          windowing:
            type: fixed
            size: 4
          ''',
          providers=TEST_PROVIDERS)
      assert_that(result, equal_to([6, 9]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
