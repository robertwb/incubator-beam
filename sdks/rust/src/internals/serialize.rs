use std::collections::HashMap;
use std::iter::Iterator;
use std::marker::PhantomData;

use std::any::Any;
use std::boxed::Box;
use std::sync::Mutex;

use once_cell::sync::Lazy;

static SERIALIZED_FNS: Lazy<Mutex<HashMap<String, Box<dyn Any + Sync + Send>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub fn serialize_fn<T: Any + Sync + Send>(obj: Box<T>) -> String {
    let mut binding = SERIALIZED_FNS.lock().unwrap();
    let name = format!("object{}", binding.len());
    binding.insert(name.to_string(), obj);
    name
}

pub fn deserialize_fn<T: Any + Sync + Send>(name: &String) -> Option<&'static T> {
    let binding = SERIALIZED_FNS.lock().unwrap();
    let untyped = binding.get(name);
    let typed = match untyped {
        Some(x) => x.downcast_ref::<T>(),
        None => None,
    };

    unsafe {
        return std::mem::transmute::<Option<&T>, Option<&'static T>>(typed);
    }
}

// ******* DoFn Wrappers, perhaps move elsewhere? *******

// TODO: Give these start/finish_bundles, etc.
pub type GenericDoFn =
    Box<dyn Fn(&dyn Any) -> Box<dyn Iterator<Item = Box<dyn Any>>> + Sync + Send>;

struct GenericDoFnWrapper {
    func: GenericDoFn,
}

unsafe impl std::marker::Send for GenericDoFnWrapper {}

struct BoxedIter<O: Any, I: IntoIterator<Item = O>> {
    typed_iter: I::IntoIter,
}

impl<O: Any, I: IntoIterator<Item = O>> Iterator for BoxedIter<O, I> {
    type Item = Box<dyn Any>;

    fn next(&mut self) -> Option<Box<dyn Any>> {
        if let Some(x) = self.typed_iter.next() {
            return Some(Box::new(x));
        } else {
            return None;
        }
    }
}

pub fn to_generic_dofn<T: Any, O: Any, I: IntoIterator<Item = O> + 'static>(
    func: fn(&T) -> I,
) -> GenericDoFn {
    Box::new(
        move |untyped_input: &dyn Any| -> Box<dyn Iterator<Item = Box<dyn Any>>> {
            let typed_input: &T = untyped_input.downcast_ref::<T>().unwrap();
            Box::new(BoxedIter::<O, I> {
                typed_iter: func(typed_input).into_iter(),
            })
        },
    )
}

pub fn to_generic_dofn_dyn<T: Any, O: Any, I: IntoIterator<Item = O> + 'static>(
    func: Box<dyn Fn(&T) -> I + Sync + Send>,
) -> GenericDoFn {
    Box::new(
        move |untyped_input: &dyn Any| -> Box<dyn Iterator<Item = Box<dyn Any>>> {
            let typed_input: &T = untyped_input.downcast_ref::<T>().unwrap();
            Box::new(BoxedIter::<O, I> {
                typed_iter: func(typed_input).into_iter(),
            })
        },
    )
}

pub trait KeyExtractor: Sync + Send {
    fn extract(&self, kv: &dyn Any) -> (String, Box<dyn Any + Sync + Send>);
    fn recombine(
        &self,
        key: &String,
        values: &Box<Vec<Box<dyn Any + Sync + Send>>>,
    ) -> Box<dyn Any + Sync + Send>;
}

pub struct TypedKeyExtractor<V: Clone + Sync + Send + 'static> {
    phantom_data: PhantomData<V>,
}

impl<V: Clone + Sync + Send + 'static> TypedKeyExtractor<V> {
    pub fn default() -> Self {
        Self {
            phantom_data: PhantomData,
        }
    }
}

impl<V: Clone + Sync + Send + 'static> KeyExtractor for TypedKeyExtractor<V> {
    fn extract(&self, kv: &dyn Any) -> (String, Box<dyn Any + Sync + Send>) {
        let typed_kv = kv.downcast_ref::<(String, V)>().unwrap();
        return (typed_kv.0.clone(), Box::new(typed_kv.1.clone()));
    }
    fn recombine(
        &self,
        key: &String,
        values: &Box<Vec<Box<dyn Any + Sync + Send>>>,
    ) -> Box<dyn Any + Sync + Send> {
        let mut typed_values: Vec<V> = Vec::new();
        for untyped_value in values.iter() {
            typed_values.push(untyped_value.downcast_ref::<V>().unwrap().clone());
        }
        return Box::new((key.clone(), typed_values));
    }
}
