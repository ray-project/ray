use crate::golden::{self, GoldenFile};
use ray_common::status::StatusCode;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct StatusInput {
    code_i8: i8,
}

#[derive(Debug, Deserialize)]
struct StatusExpected {
    name: String,
}

#[test]
fn test_status_code_roundtrip_conformance() {
    let golden: GoldenFile<StatusInput, StatusExpected> =
        golden::load_golden("status/code_roundtrip.json");

    for case in &golden.cases {
        // Verify name → code → i8 roundtrip
        let code = StatusCode::from_str_name(&case.expected.name).unwrap_or_else(|| {
            panic!(
                "FAIL [{}]: StatusCode::from_str_name({}) returned None",
                case.description, case.expected.name
            )
        });

        let actual_i8 = code as i8;
        assert_eq!(
            actual_i8, case.input.code_i8,
            "FAIL [{}]: StatusCode::{} as i8 = {}, expected {}",
            case.description, case.expected.name, actual_i8, case.input.code_i8
        );

        // Verify code → name
        assert_eq!(
            code.as_str(),
            case.expected.name,
            "FAIL [{}]: StatusCode::as_str() = {}, expected {}",
            case.description,
            code.as_str(),
            case.expected.name
        );
    }
}
