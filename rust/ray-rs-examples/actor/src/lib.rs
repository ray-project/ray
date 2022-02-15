#![allow(non_upper_case_globals)]
use ray_rs::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Vec2 {
    x: u64,
    y: u64,
}

remote_create_actor! {
pub fn new_vec2(x: u64, y: u64) -> Vec2 {
    Vec2 { x, y }
}
}

remote_actor! {
pub fn add_assign_vec2(v: &mut Vec2, other: Vec2) -> () {
    v.x += other.x;
    v.y += other.y;
}
}

remote_actor! {
pub fn get_vec2(v: &Vec2) -> Vec2 {
    v.clone()
}
}

remote_create_actor! {
pub fn new_string(s: String) -> String {
    s
}
}

remote_actor! {
pub fn append(s: &mut String, tail: String) -> String {
    s.push_str(&tail);
    s.to_string()
}
}

remote! {
pub fn append_stateless(head: String, tail: String) -> String {
    format!("{}, {}", head, tail)
}
}

use std::sync::Mutex;

remote_create_actor! {
pub fn new_string_threadsafe(s: String) -> Mutex<String> {
    Mutex::new(s)
}
}

remote_actor! {
pub fn append_threadsafe(s: &mut Mutex<String>, tail: String) -> String {
    std::thread::sleep(std::time::Duration::from_millis(500));
    let mut guard = s.lock().unwrap();
    guard.push_str(&tail);
    guard.to_string()
}
}

use tokio::sync::Mutex as AsyncMutex;

remote_create_actor! {
pub fn new_string_tokio(s: String) -> AsyncMutex<String> {
    AsyncMutex::new(s)
}
}

remote_actor! {
pub async fn append_tokio(s: &mut AsyncMutex<String>, tail: String) -> String {
    let nested_sleep = tokio_time_sleep::sleep(500);

    let local_sleep = tokio::time::sleep(std::time::Duration::from_millis(500));

    tokio::join!(nested_sleep, local_sleep);

    let mut guard = s.lock().await;
    guard.push_str(&tail);
    guard.to_string()
}
}
