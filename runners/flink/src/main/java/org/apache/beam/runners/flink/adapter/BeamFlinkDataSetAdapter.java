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
package org.apache.beam.runners.flink.adapter;

import java.util.Map;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.FlinkBatchPortablePipelineTranslator;
import org.apache.beam.runners.flink.FlinkPipelineOptions;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;

/** An adapter class that allows one to apply Apache Beam PTransforms directly to Flink DataSets. */
public class BeamFlinkDataSetAdapter extends BeamFlinkAbstractAdapter<DataSet<?>> {

  public BeamFlinkDataSetAdapter(
      PipelineOptions pipelineOptions, ExecutionEnvironment executionEnvironment) {
    super(pipelineOptions, executionEnvironment);
  }

  @SuppressWarnings("nullness")
  public <InputT, OutputT, CollectionT extends PCollection<? extends InputT>>
      DataSet<OutputT> applyBeamPTransform(
          DataSet<InputT> input, PTransform<CollectionT, PCollection<OutputT>> transform) {
    return (DataSet)
        this.<CollectionT, PCollection<OutputT>>applyBeamPTransformInternal(
                ImmutableMap.of("input", input),
                (pipeline, map) -> (CollectionT) map.get("input"),
                (output) -> ImmutableMap.of("output", output),
                transform)
            .get("output");
  }

  @SuppressWarnings("nullness")
  public <OutputT> DataSet<OutputT> applyBeamPTransform(
      Map<String, ? extends DataSet<?>> inputs,
      PTransform<PCollectionTuple, PCollection<OutputT>> transform) {
    return (DataSet)
        applyBeamPTransformInternal(
                inputs,
                BeamAdapterUtils::mapToTuple,
                (output) -> ImmutableMap.of("output", output),
                transform)
            .get("output");
  }

  @SuppressWarnings("nullness")
  public <OutputT> DataSet<OutputT> applyBeamPTransform(
      PTransform<PBegin, PCollection<OutputT>> transform) {
    return (DataSet)
        applyBeamPTransformInternal(
                ImmutableMap.<String, DataSet<?>>of(),
                (pipeline, map) -> PBegin.in(pipeline),
                (output) -> ImmutableMap.of("output", output),
                transform)
            .get("output");
  }

  @SuppressWarnings("nullness")
  public <InputT, CollectionT extends PCollection<? extends InputT>>
      Map<String, DataSet<?>> applyMultiOutputBeamPTransform(
          DataSet<InputT> input, PTransform<CollectionT, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        ImmutableMap.of("input", input),
        (pipeline, map) -> (CollectionT) map.get("input"),
        BeamAdapterUtils::tupleToMap,
        transform);
  }

  public Map<String, DataSet<?>> applyMultiOutputBeamPTransform(
      Map<String, ? extends DataSet<?>> inputs,
      PTransform<PCollectionTuple, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        inputs, BeamAdapterUtils::mapToTuple, BeamAdapterUtils::tupleToMap, transform);
  }

  public Map<String, DataSet<?>> applyMultiOutputBeamPTransform(
      PTransform<PBegin, PCollectionTuple> transform) {
    return applyBeamPTransformInternal(
        ImmutableMap.of(),
        (pipeline, map) -> PBegin.in(pipeline),
        BeamAdapterUtils::tupleToMap,
        transform);
  }

  @SuppressWarnings("nullness")
  public <InputT, CollectionT extends PCollection<? extends InputT>>
      void applyNoOutputBeamPTransform(
          DataSet<InputT> input, PTransform<CollectionT, PDone> transform) {
    applyBeamPTransformInternal(
        ImmutableMap.of("input", input),
        (pipeline, map) -> (CollectionT) map.get("input"),
        pDone -> ImmutableMap.of(),
        transform);
  }

  public void applyNoOutputBeamPTransform(
      Map<String, ? extends DataSet<?>> inputs, PTransform<PCollectionTuple, PDone> transform) {
    applyBeamPTransformInternal(
        inputs, BeamAdapterUtils::mapToTuple, pDone -> ImmutableMap.of(), transform);
  }

  public void applyNoOutputBeamPTransform(PTransform<PBegin, PDone> transform) {
    applyBeamPTransformInternal(
        ImmutableMap.of(),
        (pipeline, map) -> PBegin.in(pipeline),
        pDone -> ImmutableMap.of(),
        transform);
  }

  @Override
  protected TypeInformation<?> getTypeInformation(DataSet<?> dataSet) {
    return dataSet.getType();
  }

  @Override
  protected BeamFlinkAbstractAdapter.FlinkTranslatorAndContext<?> createTranslatorAndContext(
      Map<String, ? extends DataSet<?>> inputs, Map<String, DataSet<?>> outputs) {
    return new FlinkTranslatorAndContext<>(
        FlinkBatchPortablePipelineTranslator.createTranslator(
            ImmutableMap.of(
                FlinkInput.URN, flinkInputTranslator(inputs),
                FlinkOutput.URN, flinkOutputTranslator(outputs))),
        FlinkBatchPortablePipelineTranslator.createTranslationContext(
            org.apache.beam.runners.fnexecution.provisioning.JobInfo.create(
                "unusedJobId",
                "unusedJobName",
                "unusedRetrievalToken",
                PipelineOptionsTranslation.toProto(pipelineOptions)),
            pipelineOptions.as(FlinkPipelineOptions.class),
            executionEnvironment));
  }

  private <InputT> FlinkBatchPortablePipelineTranslator.PTransformTranslator flinkInputTranslator(
      Map<String, ? extends DataSet<?>> inputMap) {
    return (PipelineNode.PTransformNode t,
        RunnerApi.Pipeline p,
        FlinkBatchPortablePipelineTranslator.BatchTranslationContext context) -> {
      // When we run into a FlinkInput operator, it "produces" the corresponding input as its
      // "computed result."
      String inputId = t.getTransform().getSpec().getPayload().toStringUtf8();
      DataSet<InputT> flinkInput = (DataSet<InputT>) inputMap.get(inputId);
      // To make the nullness checker happy...
      if (flinkInput == null) {
        throw new IllegalStateException("Missing input: " + inputId);
      }
      context.addDataSet(
          Iterables.getOnlyElement(t.getTransform().getOutputsMap().values()),
          // new MapOperator(...) rather than .map to manually designate the type information.
          // Note that MapOperator is a subclass of DataSet.
          new MapOperator<InputT, WindowedValue<InputT>>(
              flinkInput,
              BeamAdapterUtils.coderTotoTypeInformation(
                  WindowedValue.getValueOnlyCoder(
                      BeamAdapterUtils.typeInformationToCoder(flinkInput.getType(), coderRegistry)),
                  pipelineOptions),
              x -> WindowedValue.valueInGlobalWindow(x),
              "AddGlobalWindows"));
    };
  }

  private <InputT> FlinkBatchPortablePipelineTranslator.PTransformTranslator flinkOutputTranslator(
      Map<String, DataSet<?>> outputMap) {
    return (PipelineNode.PTransformNode t,
        RunnerApi.Pipeline p,
        FlinkBatchPortablePipelineTranslator.BatchTranslationContext context) -> {
      DataSet<WindowedValue<InputT>> inputDataSet =
          context.getDataSetOrThrow(
              Iterables.getOnlyElement(t.getTransform().getInputsMap().values()));
      // When we run into a FlinkOutput operator, we cache the computed PCollection to return to the
      // user.
      String outputId = t.getTransform().getSpec().getPayload().toStringUtf8();
      Coder<InputT> outputCoder =
          BeamAdapterUtils.lookupCoder(
              p, Iterables.getOnlyElement(t.getTransform().getInputsMap().values()));
      // TODO(robertwb): Also handle or disable length prefix coding (for embedded mode at least).
      outputMap.put(
          outputId,
          new MapOperator<WindowedValue<InputT>, InputT>(
              inputDataSet,
              BeamAdapterUtils.coderTotoTypeInformation(outputCoder, pipelineOptions),
              w -> w.getValue(),
              "StripWindows"));
    };
  }
}
