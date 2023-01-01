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

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use coders::coders::CoderI;
use coders::required_coders::BytesCoder;
use proto::beam::pipeline as proto_pipeline;

use crate::pipeline::Pipeline;

// T should be never(!) for Root
// https://github.com/rust-lang/rust/issues/35121
#[derive(Clone)]
pub struct PValue<T>
where
    T: Send,
{
    ptype: PType,
    name: String,
    pcoll_proto: proto_pipeline::PCollection,
    pipeline: Arc<Pipeline>,

    phantom: PhantomData<T>,
}

impl<T> PValue<T>
where
    T: Send,
{
    pub fn new(
        ptype: PType,
        name: String,
        pcoll_proto: proto_pipeline::PCollection,
        pipeline: Arc<Pipeline>,
    ) -> Self {
        Self {
            ptype,
            name,
            pcoll_proto,
            pipeline,

            phantom: PhantomData::default(),
        }
    }

    pub fn new_root(pipeline: Arc<Pipeline>) -> Self {
        let pcoll_name = "root".to_string();

        let proto_coder_id = pipeline.register_coder_proto(proto_pipeline::Coder {
            spec: Some(proto_pipeline::FunctionSpec {
                urn: String::from(coders::urns::BYTES_CODER_URN),
                payload: Vec::with_capacity(0),
            }),
            component_coder_ids: Vec::with_capacity(0),
        });

        pipeline.register_coder::<BytesCoder, Vec<u8>>(Box::new(BytesCoder::new()));

        let output_proto = proto_pipeline::PCollection {
            unique_name: pcoll_name.clone(),
            coder_id: proto_coder_id,
            is_bounded: proto_pipeline::is_bounded::Enum::Bounded as i32,
            windowing_strategy_id: "placeholder".to_string(),
            display_data: Vec::with_capacity(0),
        };

        let impulse_proto = proto_pipeline::PTransform {
            unique_name: "root".to_string(),
            spec: None,
            subtransforms: Vec::with_capacity(0),
            inputs: HashMap::with_capacity(0),
            outputs: HashMap::from([("out".to_string(), pcoll_name.clone())]),
            display_data: Vec::with_capacity(0),
            environment_id: "".to_string(),
            annotations: HashMap::with_capacity(0),
        };

        pipeline.register_proto_transform(impulse_proto);

        PValue::new(PType::Root, pcoll_name, output_proto, pipeline)
    }

    pub fn register_pipeline_coder<'a, C: CoderI<E> + 'a, E>(
        &self,
        coder: Box<dyn Any + Send + 'a>,
    ) -> TypeId {
        self.pipeline.register_coder::<C, E>(coder)
    }

    pub fn register_pipeline_coder_proto(&self, coder_proto: proto_pipeline::Coder) -> String {
        self.pipeline.register_coder_proto(coder_proto)
    }

    pub fn register_pipeline_proto_transform(&self, transform: proto_pipeline::PTransform) {
        self.pipeline.register_proto_transform(transform)
    }

    pub fn get_pipeline_arc(&self) -> Arc<Pipeline> {
        self.pipeline.clone()
    }

    pub fn get_type(&self) -> &PType {
        &self.ptype
    }

    pub fn apply<F, Out>(self, transform: F) -> PValue<Out>
    where
        Out: Send,
        F: PTransform<T, Out> + Send,
    {
        transform.expand(self)
    }

    // pub fn map(&self, callable: impl Fn() -> PValue) -> PValue {
    //     unimplemented!()
    // }
}

// Anonymous sum types would probably be better, if/when they become
// available. https://github.com/rust-lang/rfcs/issues/294
// TODO: use strum discriminants on PValues instead of this
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PType {
    Root,
    PCollection,
    PValueArr,
    PValueMap,
}

pub trait PTransform<In, Out>
where
    In: Send,
    Out: Send,
{
    fn expand(self, input: PValue<In>) -> PValue<Out>
    where
        Self: Sized,
    {
        match input.get_type() {
            PType::Root => {
                panic!()
            }
            PType::PCollection => {
                unimplemented!()
            }
            _ => unimplemented!(),
        }
    }
}