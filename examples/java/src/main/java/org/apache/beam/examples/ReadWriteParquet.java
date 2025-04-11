/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.examples;

/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused")
public class ReadWriteParquet {
  private static final Logger LOG = LoggerFactory.getLogger(ReadWriteParquet.class);

  private static final Schema SCHEMA =
      new Schema.Parser()
          .parse(
              "{\n"
                  + " \"namespace\": \"ioitavro\",\n"
                  + " \"type\": \"record\",\n"
                  + " \"name\": \"TestAvroLine\",\n"
                  + " \"fields\": [\n"
                  + "     {\"name\": \"row\", \"type\": \"string\"}\n"
                  + " ]\n"
                  + "}");

  private static String filenamePrefix = "";
  private static Integer numberOfTextLines = 2 * 225400000;

  public static String appendTimestampSuffix(String text) {
    return String.format("%s_%s", text, new Date().getTime());
  }

  /** Constructs text lines in files used for testing. */
  public static class DeterministicallyConstructTestTextLineFn extends DoFn<Long, String> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(String.format("IO IT Test line of text. Line seed: %s", c.element()));
    }
  }

  /** Deletes matching files using the FileSystems API. */
  public static class DeleteFileFn extends DoFn<String, Void> {
    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
      MatchResult match =
          Iterables.getOnlyElement(FileSystems.match(Collections.singletonList(c.element())));

      Set<ResourceId> resourceIds = new HashSet<>();
      for (MatchResult.Metadata metadataElem : match.metadata()) {
        resourceIds.add(metadataElem.resourceId());
      }

      FileSystems.delete(resourceIds);
    }
  }

  public static class DeterministicallyConstructAvroRecordsFn extends DoFn<String, GenericRecord> {

    @ProcessElement
    public void processElement(ProcessContext c) {
      c.output(new GenericRecordBuilder(SCHEMA).set("row", c.element()).build());
    }
  }

  public static class TripleFileNames extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      for (int i = 0; i < 3; i++) {
        c.output(c.element());
      }
    }
  }

  public static class ChangeBucketName extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      c.output(c.element().replace("priyans-testing-bucket", "priyans-bucket"));
    }
  }

  public static final org.apache.avro.Schema avroSchema =
      org.apache.avro.Schema.createRecord(
          ImmutableList.of(
              new Field(
                  "number",
                  org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG),
                  "nodoc",
                  0)));

  public static final SerializableFunction<AvroWriteRequest<Long>, GenericRecord>
      failingLongToAvro =
          new SerializableFunction<AvroWriteRequest<Long>, GenericRecord>() {
            @Override
            public GenericRecord apply(AvroWriteRequest<Long> input) {
              return new GenericRecordBuilder(avroSchema).set("number", input.getElement()).build();
            }
          };

  public static final SimpleFunction<GenericRecord, KV<Integer, GenericRecord>> addKey =
      new SimpleFunction<GenericRecord, KV<Integer, GenericRecord>>() {
        @Override
        public KV<Integer, GenericRecord> apply(GenericRecord input) {
          return KV.of((int) (3 * Math.random()), input);
        }
      };

  public static final SimpleFunction<KV<Integer, Iterable<GenericRecord>>, Integer> processGbk =
      new SimpleFunction<KV<Integer, Iterable<GenericRecord>>, Integer>() {
        @Override
        public Integer apply(KV<Integer, Iterable<GenericRecord>> input) {
          int size = 0;
          for (Object o : input.getValue()) {
            size += 1;
          }
          System.out.println("key " + input.getKey() + " size " + size);
          return size;
        }
      };

  public static final SimpleFunction<GenericRecord, String> toString =
      new SimpleFunction<GenericRecord, String>() {
        @Override
        public String apply(GenericRecord input) {
          return (String) input.toString(); // get("row");
        }
      };

  public static void main(String[] args) {
    System.out.println("args " + java.util.Arrays.toString(args));
    PipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

    PortablePipelineOptions opts = options.as(PortablePipelineOptions.class);

    opts.setRunner(PortableRunner.class);
    opts.setDefaultEnvironmentType("LOOPBACK");
    // python -m apache_beam.runners.portability.local_job_service_main --port 5555
    opts.setJobEndpoint("localhost:5555");

    Pipeline p = Pipeline.create(options);
    //     PCollection<String> all_file_names =
    //         p.apply(
    //             TextIO.read()
    //                 .from(
    //
    // "gs://priyans-bucket/parquet/all_source_file_names_first_shard-00000-of-00001"));
    //     PCollection<String> all_file_names_tripled =
    //         all_file_names.apply(ParDo.of(new TripleFileNames()));
    //     PCollection<GenericRecord> all_data =
    //         all_file_names_tripled
    //             .apply("Find files", FileIO.matchAll())
    //             .apply("Read matched files", FileIO.readMatches())
    //             .apply("Read parquet files", ParquetIO.readFiles(SCHEMA));
    List<GenericRecord> list_data = new ArrayList<>();

    String ooos = "o";
    while (ooos.length() < 1_00_000) {
      ooos += ooos;
    }
    String base = "r" + ooos + "w";
    for (long i = 0; i < 100; i++) {
      list_data.add(new GenericRecordBuilder(SCHEMA).set("row", base + i).build());
    }
    PCollection<GenericRecord> all_data =
        p.apply(Create.of(list_data).withCoder(AvroCoder.of(SCHEMA)));

    if (false) {
      // Peaks at 3.35GB for 5MB * 10 * 5k.
      all_data
          .apply(MapElements.via(addKey))
          .apply(GroupByKey.create())
          .apply(MapElements.via(processGbk));
    } else if (true) {
      // Quickly jumps to 6GB for 5MB * 10 * 5k with 3 shards. 11-13GB with the next shard and the
      // last.
      // At 1MB * 10 * 5k / 3 shards, quickly hits 5.5BG, goes up to 6.75GB by the second shard.
      // Similar for 100k * 100 * 5k / 3.
      // For 100k * 100 * 5k / 30, quickly hits 3.xGB, slowly climbs to 3.88GB.
      all_data.apply(
          FileIO.<GenericRecord>write()
              .via(ParquetIO.sink(SCHEMA).withRowGroupSize(1 << 20))
              .to("./out") // .to("gs://priyans-bucket/parquet_sink_v2_sharding_350")
              .withSuffix(".parquet")
              .withNumShards(3));
    } else {
      // For 100k * 100 * 5k / 3, climbs to 5.2GB? in the first shard, peaks at about 5.5GB during
      // the second.
      // For 1M    * 10 * 5k / 3, climbs to 3.2GB in the first shard, peaks at about 3.4GB during
      // the second and 3.6 in the third.
      all_data
          .apply(MapElements.via(toString))
          .apply(
              FileIO.<String>write()
                  .via(TextIO.sink())
                  .to("./out/")
                  .withSuffix(".txt")
                  .withCompression(Compression.GZIP)
                  .withNumShards(3));
    }

    p.run().waitUntilFinish();
  }
}
