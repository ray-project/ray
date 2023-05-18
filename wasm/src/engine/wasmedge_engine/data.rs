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

use crate::engine::{WasmType, WasmValue};

use wasmedge_types as wt;
use wasmedge_types::ValType;

use wasmedge_sys as sys;

pub fn wasmedgetype_to_wasmtype(t: &ValType) -> WasmType {
    match t {
        ValType::I32 => WasmType::I32,
        ValType::I64 => WasmType::I64,
        ValType::F32 => WasmType::F32,
        ValType::F64 => WasmType::F64,
        ValType::V128 => WasmType::V128,
        ValType::ExternRef => WasmType::ExternRef,
        ValType::FuncRef => WasmType::FuncRef,
    }
}

pub fn wasmtype_to_wasmedgetype(t: &WasmType) -> ValType {
    match t {
        WasmType::I32 => ValType::I32,
        WasmType::I64 => ValType::I64,
        WasmType::F32 => ValType::F32,
        WasmType::F64 => ValType::F64,
        WasmType::V128 => ValType::V128,
        WasmType::ExternRef => ValType::ExternRef,
        WasmType::FuncRef => ValType::FuncRef,
    }
}

pub fn from_wasmedge_value(val: &sys::WasmValue) -> WasmValue {
    match val.ty() {
        ValType::I32 => WasmValue::I32(val.to_i32()),
        ValType::I64 => WasmValue::I64(val.to_i64()),
        ValType::F32 => WasmValue::F32(val.to_f32() as u32),
        ValType::F64 => WasmValue::F64(val.to_f64() as u64),
        ValType::V128 => WasmValue::V128(val.to_v128() as u128),
        ValType::FuncRef => unimplemented!("FuncRef"),
        ValType::ExternRef => unimplemented!("ExternRef"),
    }
}

pub fn to_wasmedge_value(val: &WasmValue) -> sys::WasmValue {
    match val {
        WasmValue::I32(v) => sys::WasmValue::from_i32(*v),
        WasmValue::I64(v) => sys::WasmValue::from_i64(*v),
        WasmValue::F32(v) => sys::WasmValue::from_f32(*v as f32),
        WasmValue::F64(v) => sys::WasmValue::from_f64(*v as f64),
        WasmValue::V128(v) => sys::WasmValue::from_v128(*v as i128),
        WasmValue::FuncRef(_) => unimplemented!("FuncRef"),
        WasmValue::ExternRef(_) => unimplemented!("ExternRef"),
    }
}
