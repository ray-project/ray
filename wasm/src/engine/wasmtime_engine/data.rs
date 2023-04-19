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
use wasmtime::{Val, ValRaw, ValType};

pub fn to_wasmtime_raw_value(val: &WasmValue) -> ValRaw {
    match val {
        WasmValue::I32(v) => ValRaw::i32(*v),
        WasmValue::I64(v) => ValRaw::i64(*v),
        WasmValue::F32(v) => ValRaw::f32(*v),
        WasmValue::F64(v) => ValRaw::f64(*v),
        WasmValue::V128(v) => ValRaw::v128(*v),
        WasmValue::ExternRef(v) => ValRaw::externref(*v),
        WasmValue::FuncRef(v) => ValRaw::funcref(*v),
    }
}

pub fn to_wasmtime_value(val: &WasmValue) -> Val {
    match val {
        WasmValue::I32(v) => Val::I32(*v),
        WasmValue::I64(v) => Val::I64(*v),
        WasmValue::F32(v) => Val::F32(*v),
        WasmValue::F64(v) => Val::F64(*v),
        WasmValue::V128(v) => Val::V128(*v),
        _ => unimplemented!(),
    }
}

pub fn from_wasmtime_raw_value(ty: &WasmType, val: &ValRaw) -> WasmValue {
    match ty {
        WasmType::I32 => WasmValue::I32(val.get_i32()),
        WasmType::I64 => WasmValue::I64(val.get_i64()),
        WasmType::F32 => WasmValue::F32(val.get_u32()),
        WasmType::F64 => WasmValue::F64(val.get_u64()),
        WasmType::V128 => WasmValue::V128(val.get_v128()),
        WasmType::ExternRef => WasmValue::ExternRef(val.get_externref()),
        WasmType::FuncRef => WasmValue::FuncRef(val.get_funcref()),
    }
}

pub fn from_wasmtime_value(ty: &WasmType, val: &Val) -> WasmValue {
    match ty {
        WasmType::I32 => WasmValue::I32(val.unwrap_i32()),
        WasmType::I64 => WasmValue::I64(val.unwrap_i64()),
        WasmType::F32 => WasmValue::F32(val.unwrap_f32().to_bits()),
        WasmType::F64 => WasmValue::F64(val.unwrap_f64().to_bits()),
        WasmType::V128 => WasmValue::V128(val.unwrap_v128()),
        _ => unimplemented!(),
    }
}

pub fn wasmtime_type(ty: &WasmType) -> wasmtime::ValType {
    match ty {
        WasmType::I32 => ValType::I32,
        WasmType::I64 => ValType::I64,
        WasmType::F32 => ValType::F32,
        WasmType::F64 => ValType::F64,
        WasmType::V128 => ValType::V128,
        WasmType::ExternRef => ValType::ExternRef,
        WasmType::FuncRef => ValType::FuncRef,
    }
}

pub fn from_wasmtime_type(ty: &ValType) -> WasmType {
    match ty {
        ValType::I32 => WasmType::I32,
        ValType::I64 => WasmType::I64,
        ValType::F32 => WasmType::F32,
        ValType::F64 => WasmType::F64,
        ValType::V128 => WasmType::V128,
        ValType::ExternRef => WasmType::ExternRef,
        ValType::FuncRef => WasmType::FuncRef,
    }
}
