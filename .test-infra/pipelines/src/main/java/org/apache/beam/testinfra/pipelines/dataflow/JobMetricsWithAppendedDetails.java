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
package org.apache.beam.testinfra.pipelines.dataflow;

import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobMetrics;
import java.io.Serializable;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;

/**
 * A {@link JobMetrics} enrichment with a {@link Job}'s id and create time. The purpose of this
 * enrichment is to join metrics with its Job while partitioning on the Job's create time.
 */
@Internal
public class JobMetricsWithAppendedDetails implements Serializable {

  private String jobId = "";

  private Instant jobCreateTime = Instant.EPOCH;

  private JobMetrics jobMetrics = JobMetrics.getDefaultInstance();

  public String getJobId() {
    return jobId;
  }

  public void setJobId(@NonNull String jobId) {
    this.jobId = jobId;
  }

  public Instant getJobCreateTime() {
    return jobCreateTime;
  }

  public void setJobCreateTime(@NonNull Instant jobCreateTime) {
    this.jobCreateTime = jobCreateTime;
  }

  public JobMetrics getJobMetrics() {
    return jobMetrics;
  }

  public void setJobMetrics(@NonNull JobMetrics jobMetrics) {
    this.jobMetrics = jobMetrics;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JobMetricsWithAppendedDetails that = (JobMetricsWithAppendedDetails) o;
    return Objects.equal(jobId, that.jobId)
        && Objects.equal(jobCreateTime, that.jobCreateTime)
        && Objects.equal(jobMetrics, that.jobMetrics);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(jobId, jobCreateTime, jobMetrics);
  }
}
