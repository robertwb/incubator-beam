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

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.IterableCoder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.util.construction.CoderTranslation;
import org.apache.beam.sdk.util.construction.RehydratedComponents;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public class BeamAdapterUtils {
  private BeamAdapterUtils() {}

  @SuppressWarnings("nullness")
  static <T> Coder<T> typeInformationToCoder(
      TypeInformation<T> typeInfo, CoderRegistry coderRegistry) {
    Class<T> clazz = typeInfo.getTypeClass();
    if (typeInfo instanceof CoderTypeInformation) {
      return ((CoderTypeInformation) typeInfo).getCoder();
    } else if (clazz.getTypeParameters().length == 0) {
      try {
        return coderRegistry.getCoder(clazz);
      } catch (CannotProvideCoderException exn) {
        throw new RuntimeException(exn);
      }
    } else if (Iterable.class.isAssignableFrom(clazz)) {
      return (Coder)
          IterableCoder.of(
              typeInformationToCoder(typeInfo.getGenericParameters().get("T"), coderRegistry));
    } else if (Map.class.isAssignableFrom(clazz)) {
      return (Coder)
          MapCoder.of(
              typeInformationToCoder(typeInfo.getGenericParameters().get("K"), coderRegistry),
              typeInformationToCoder(typeInfo.getGenericParameters().get("V"), coderRegistry));
    } else {
      throw new RuntimeException("Coder translation for " + typeInfo + " not yet supported.");
    }
  }

  static <T> TypeInformation<T> coderTotoTypeInformation(Coder<T> coder, PipelineOptions options) {
    // TODO(robertwb): Consider mapping some common types.
    return new CoderTypeInformation<>(coder, options);
  }

  public static Map<String, PCollection<?>> tupleToMap(PCollectionTuple tuple) {
    return tuple.getAll().entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getId(), e -> e.getValue()));
  }

  public static PCollectionTuple mapToTuple(Pipeline p, Map<String, PCollection<?>> map) {
    PCollectionTuple tuple = PCollectionTuple.empty(p);
    for (Map.Entry<String, PCollection<?>> entry : map.entrySet()) {
      tuple = tuple.and(entry.getKey(), entry.getValue());
    }
    return tuple;
  }

  static <T> Coder<T> lookupCoder(RunnerApi.Pipeline p, String pCollectionId) {
    try {
      return (Coder<T>)
          CoderTranslation.fromProto(
              p.getComponents()
                  .getCodersOrThrow(
                      p.getComponents().getPcollectionsOrThrow(pCollectionId).getCoderId()),
              RehydratedComponents.forComponents(p.getComponents()),
              CoderTranslation.TranslationContext.DEFAULT);
    } catch (IOException exn) {
      throw new RuntimeException(exn);
    }
  }
}
