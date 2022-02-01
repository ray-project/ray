#![allow(non_upper_case_globals)]
use ray_rs::*;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
pub struct Vec2 {
    x: f64,
    y: f64,
}

remote! {
pub fn new(x: f64, y: f64) -> Vec2 {
    Vec2 { x, y }
}
}

remote_actor! {
pub fn add_assign(vec: &mut Vec2, other: Vec2) -> () {
    vec.x += other.x;
    vec.y += other.y;
}
}

remote_actor! {
pub fn get(vec: Vec2) -> Vec2 {
    vec.clone()
}
}
