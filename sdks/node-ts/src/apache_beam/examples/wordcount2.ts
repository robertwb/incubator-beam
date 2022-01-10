// TODO: Should this be in a top-level examples dir, rather than under apache_beam.

import * as beam from '../../apache_beam';
import * as textio from '../io/textio';
import {DirectRunner} from '../runners/direct_runner';

import {CombineBy, CountFn} from '../transforms/combine';

import {NodeRunner} from '../runners/node_runner/runner';
import {RemoteJobServiceClient} from '../runners/node_runner/client';

class CountElements extends beam.PTransform<
  beam.PCollection<any>,
  beam.PCollection<any>
> {
  expand(input: beam.PCollection<any>) {
    return input
      .map(e => ({element: e, count: 1}))
      .apply(new CombineBy('element', 'count', new CountFn()));
  }
}

function wordCount(lines: beam.PCollection<string>): beam.PCollection<any> {
  return lines
    .map((s: string) => s.toLowerCase())
    .flatMap(function* (line: string) {
      yield* line.split(/[^a-z]+/);
    })
    .apply(new CountElements('Count'));
}

async function main() {
  // python apache_beam/runners/portability/local_job_service_main.py --port 3333
  await new NodeRunner(new RemoteJobServiceClient('localhost:3333')).run(
    async root => {
      const lines = await root.asyncApply(
        new textio.ReadFromText(
          'gs://dataflow-samples/shakespeare/kinglear.txt'
        )
      );

      lines.apply(wordCount).map(console.log);
    }
  );
}

main()
  .catch(e => console.error(e))
  .finally(() => process.exit());
