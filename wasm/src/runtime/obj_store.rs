// Copyright 2020-2023 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::runtime::ObjectID;
use anyhow::{anyhow, Ok, Result};

use super::{
    core::core_worker::{
        CoreWorker_AddLocalReference, CoreWorker_Get, CoreWorker_GetMulti, CoreWorker_Put,
        CoreWorker_RemoveLocalReference, CoreWorker_WaitMulti,
    },
    core_worker::CoreWorker_PutWithObjID,
    Base,
};

pub enum ObjectStoreType {
    Native,
    Local,
}

const MAX_RAY_GET_SIZE: usize = 100 * 1024 * 1024;

pub trait ObjectStore {
    fn put(&mut self, data: &[u8]) -> Result<ObjectID>;
    fn put_with_object_id(&mut self, data: &[u8], object_id: &ObjectID) -> Result<()>;
    fn get(&self, object_id: &ObjectID, timeout_ms: i32) -> Result<Vec<u8>>;
    fn gets(&self, object_ids: &[ObjectID], timeout_ms: i32) -> Result<Vec<Vec<u8>>>;
    fn wait(
        &self,
        object_ids: &[ObjectID],
        num_objects: usize,
        timeout_ms: i32,
    ) -> Result<Vec<bool>>;

    fn add_local_ref(&mut self, id: &ObjectID) -> Result<()>;
    fn remove_local_ref(&mut self, id: &ObjectID) -> Result<()>;
}

pub struct ObjectStoreFactory {}

impl ObjectStoreFactory {
    pub fn create_object_store(
        obj_store_type: ObjectStoreType,
    ) -> Box<dyn ObjectStore + Send + Sync> {
        match obj_store_type {
            ObjectStoreType::Native => Box::new(NativeObjectStore::new()),
            ObjectStoreType::Local => Box::new(LocalObjectStore::new()),
        }
    }
}

pub struct LocalObjectStore {}

impl ObjectStore for LocalObjectStore {
    fn put(&mut self, _data: &[u8]) -> Result<ObjectID> {
        unimplemented!()
    }

    fn put_with_object_id(&mut self, _data: &[u8], _object_id: &ObjectID) -> Result<()> {
        unimplemented!()
    }

    fn get(&self, _object_id: &ObjectID, _timeout_ms: i32) -> Result<Vec<u8>> {
        unimplemented!()
    }

    fn gets(&self, _object_ids: &[ObjectID], _timeout_ms: i32) -> Result<Vec<Vec<u8>>> {
        unimplemented!()
    }

    fn wait(
        &self,
        _object_ids: &[ObjectID],
        _num_objects: usize,
        _timeout_ms: i32,
    ) -> Result<Vec<bool>> {
        unimplemented!()
    }

    fn add_local_ref(&mut self, _id: &ObjectID) -> Result<()> {
        unimplemented!()
    }

    fn remove_local_ref(&mut self, _id: &ObjectID) -> Result<()> {
        unimplemented!()
    }
}

impl LocalObjectStore {
    pub fn new() -> Self {
        Self {}
    }
}

pub struct NativeObjectStore {}

impl ObjectStore for NativeObjectStore {
    fn put(&mut self, data: &[u8]) -> Result<ObjectID> {
        let mut obj_id = ObjectID::new();
        unsafe {
            let mut object_id_len = obj_id.id.len();
            let res = CoreWorker_Put(
                data.as_ptr(),
                data.len(),
                obj_id.id.as_mut_ptr(),
                &mut object_id_len,
            );
            if res != 0 {
                return Err(anyhow!("put object failed"));
            }
            if object_id_len != obj_id.id.len() {
                return Err(anyhow!("result obj id has different length"));
            }
        }
        Ok(obj_id)
    }

    fn put_with_object_id(&mut self, data: &[u8], object_id: &ObjectID) -> Result<()> {
        unsafe {
            let res = CoreWorker_PutWithObjID(
                data.as_ptr(),
                data.len(),
                object_id.id.as_ptr(),
                object_id.size(),
            );
            if res != 0 {
                return Err(anyhow!("put object failed"));
            }
        }
        Ok(())
    }

    fn get(&self, object_id: &ObjectID, timeout_ms: i32) -> Result<Vec<u8>> {
        // allocate a temporary buffer for the object
        let mut data = vec![0u8; MAX_RAY_GET_SIZE];
        let mut data_len = data.len();
        unsafe {
            let res = CoreWorker_Get(
                object_id.id.as_ptr(),
                object_id.size(),
                data.as_mut_ptr(),
                &mut data_len,
                timeout_ms,
            );
            if res != 0 || data_len == 0 || data_len > data.len() {
                let msg = format!(
                    "obj store getting object failed: {:x?}. res: {} data_len: {}",
                    object_id, res, data_len
                );
                return Err(anyhow!(msg));
            }
        }
        data.truncate(data_len);
        Ok(data)
    }

    fn gets(&self, object_ids: &[ObjectID], timeout_ms: i32) -> Result<Vec<Vec<u8>>> {
        unsafe {
            // allocate a temporary buffer of buffer pointers
            let obj_ids_buf =
                libc::malloc(object_ids.len() * std::mem::size_of::<*mut u8>()) as *mut *const u8;
            let obj_ids_buf_len =
                libc::malloc(object_ids.len() * std::mem::size_of::<usize>()) as *mut usize;
            for (i, obj_id) in object_ids.iter().enumerate() {
                *obj_ids_buf.add(i) = obj_id.id.as_ptr();
                *obj_ids_buf_len.add(i) = obj_id.size();
            }
            // allocate result buffers
            let data_buf =
                libc::malloc(object_ids.len() * std::mem::size_of::<*mut u8>()) as *mut *mut u8;
            let data_buf_len =
                libc::malloc(object_ids.len() * std::mem::size_of::<usize>()) as *mut usize;
            for i in 0..object_ids.len() {
                // allocate buffer for each entry inside buffer
                *data_buf.add(i) = libc::malloc(4096) as *mut u8;
                *data_buf_len.add(i) = 4096;
            }

            let mut num_results = 0;
            let res = CoreWorker_GetMulti(
                obj_ids_buf,
                obj_ids_buf_len,
                object_ids.len(),
                data_buf,
                data_buf_len,
                &mut num_results,
                timeout_ms,
            );
            // free the temporary buffer
            libc::free(obj_ids_buf as *mut libc::c_void);
            libc::free(obj_ids_buf_len as *mut libc::c_void);
            // create vectors from the result buffers
            let mut results = vec![];
            for i in 0..num_results {
                // copy data from the buffer
                let mut data = vec![0u8; *data_buf_len.add(i)];
                data.copy_from_slice(std::slice::from_raw_parts(
                    *data_buf.add(i),
                    *data_buf_len.add(i),
                ));
                results.push(data);
                // free the buffer
                libc::free(*data_buf.add(i) as *mut libc::c_void);
            }
            // free the result buffers
            libc::free(data_buf as *mut libc::c_void);
            libc::free(data_buf_len as *mut libc::c_void);
            if res != 0 {
                // free the temporary buffer
                return Err(anyhow!("get object failed"));
            }
            Ok(results)
        }
    }

    fn wait(
        &self,
        object_ids: &[ObjectID],
        num_objects: usize,
        timeout_ms: i32,
    ) -> Result<Vec<bool>> {
        unsafe {
            // allocate a temporary buffer of buffer pointers
            let obj_ids_buf =
                libc::malloc(object_ids.len() * std::mem::size_of::<*mut u8>()) as *mut *const u8;
            let obj_ids_buf_len =
                libc::malloc(object_ids.len() * std::mem::size_of::<usize>()) as *mut usize;
            for (i, obj_id) in object_ids.iter().enumerate() {
                *obj_ids_buf.add(i) = obj_id.id.as_ptr();
                *obj_ids_buf_len.add(i) = obj_id.size();
            }
            let mut ready = vec![false; object_ids.len()];
            let res = CoreWorker_WaitMulti(
                obj_ids_buf,
                obj_ids_buf_len,
                object_ids.len(),
                num_objects,
                ready.as_mut_ptr(),
                timeout_ms,
            );
            // free the temporary buffer
            libc::free(obj_ids_buf as *mut libc::c_void);
            libc::free(obj_ids_buf_len as *mut libc::c_void);
            if res != 0 {
                // free the temporary buffer
                return Err(anyhow!("wait object failed"));
            }
            Ok(ready)
        }
    }

    fn add_local_ref(&mut self, id: &ObjectID) -> Result<()> {
        unsafe {
            let res = CoreWorker_AddLocalReference(id.id.as_ptr(), id.size());
            if res != 0 {
                return Err(anyhow!("add local reference failed"));
            }
        }
        Ok(())
    }

    fn remove_local_ref(&mut self, id: &ObjectID) -> Result<()> {
        unsafe {
            let res = CoreWorker_RemoveLocalReference(id.id.as_ptr(), id.size());
            if res != 0 {
                return Err(anyhow!("remove local reference failed"));
            }
        }
        Ok(())
    }
}

impl NativeObjectStore {
    pub fn new() -> Self {
        Self {}
    }
}
