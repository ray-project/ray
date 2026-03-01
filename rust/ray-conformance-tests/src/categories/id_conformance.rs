use crate::golden::{self, GoldenFile};
use ray_common::id::*;
use serde::Deserialize;

// ── MurmurHash64A ──

#[derive(Debug, Deserialize)]
struct MurmurInput {
    key_hex: String,
    seed: u64,
}

#[derive(Debug, Deserialize)]
struct MurmurExpected {
    hash: u64,
}

#[test]
fn test_murmur_hash_conformance() {
    let golden: GoldenFile<MurmurInput, MurmurExpected> =
        golden::load_golden("id/murmur_hash.json");

    for case in &golden.cases {
        let key = hex::decode(&case.input.key_hex).unwrap_or_default();
        let result = murmur_hash_64a_public(&key, case.input.seed);
        assert_eq!(
            result, case.expected.hash,
            "FAIL [{}]: murmur_hash_64a({}, seed={}) = {}, expected {}",
            case.description, case.input.key_hex, case.input.seed, result, case.expected.hash
        );
    }
}

// ── JobID::from_int ──

#[derive(Debug, Deserialize)]
struct JobIdInput {
    value: u32,
}

#[derive(Debug, Deserialize)]
struct JobIdExpected {
    hex: String,
}

#[test]
fn test_job_id_from_int_conformance() {
    let golden: GoldenFile<JobIdInput, JobIdExpected> =
        golden::load_golden("id/job_id_from_int.json");

    for case in &golden.cases {
        let job_id = JobID::from_int(case.input.value);
        let result_hex = job_id.hex();
        assert_eq!(
            result_hex, case.expected.hex,
            "FAIL [{}]: JobID::from_int({}) = {}, expected {}",
            case.description, case.input.value, result_hex, case.expected.hex
        );

        // Verify roundtrip
        let roundtrip = job_id.to_int();
        assert_eq!(
            roundtrip, case.input.value,
            "FAIL [{}]: JobID roundtrip failed",
            case.description
        );
    }
}

// ── ActorID::of ──

#[derive(Debug, Deserialize)]
struct ActorIdInput {
    job_id_int: u32,
    task_id_hex: String,
    counter: usize,
}

#[derive(Debug, Deserialize)]
struct ActorIdExpected {
    actor_id_hex: String,
}

#[test]
fn test_actor_id_of_conformance() {
    let golden: GoldenFile<ActorIdInput, ActorIdExpected> =
        golden::load_golden("id/actor_id_of.json");

    for case in &golden.cases {
        let job_id = JobID::from_int(case.input.job_id_int);
        let task_id = TaskID::from_hex(&case.input.task_id_hex);
        let actor_id = ActorID::of(&job_id, &task_id, case.input.counter);
        let result_hex = actor_id.hex();
        assert_eq!(
            result_hex, case.expected.actor_id_hex,
            "FAIL [{}]: ActorID::of(job={}, task={}, counter={}) = {}, expected {}",
            case.description,
            case.input.job_id_int,
            case.input.task_id_hex,
            case.input.counter,
            result_hex,
            case.expected.actor_id_hex
        );

        // Verify embedded job_id extraction
        assert_eq!(
            actor_id.job_id(),
            job_id,
            "FAIL [{}]: ActorID job_id extraction mismatch",
            case.description
        );
    }
}

// ── ObjectID::from_index ──

#[derive(Debug, Deserialize)]
struct ObjectIdInput {
    task_id_hex: String,
    index: u32,
}

#[derive(Debug, Deserialize)]
struct ObjectIdExpected {
    object_id_hex: String,
}

#[test]
fn test_object_id_from_index_conformance() {
    let golden: GoldenFile<ObjectIdInput, ObjectIdExpected> =
        golden::load_golden("id/object_id_from_index.json");

    for case in &golden.cases {
        let task_id = TaskID::from_hex(&case.input.task_id_hex);
        let object_id = ObjectID::from_index(&task_id, case.input.index);
        let result_hex = object_id.hex();
        assert_eq!(
            result_hex, case.expected.object_id_hex,
            "FAIL [{}]: ObjectID::from_index(task={}, index={}) = {}, expected {}",
            case.description,
            case.input.task_id_hex,
            case.input.index,
            result_hex,
            case.expected.object_id_hex
        );

        // Verify extraction
        assert_eq!(
            object_id.object_index(),
            case.input.index,
            "FAIL [{}]: ObjectID index extraction mismatch",
            case.description
        );
    }
}
