use crate::golden::{self, GoldenFile};
use ray_common::scheduling::FixedPoint;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FixedPointInput {
    f64_value: f64,
}

#[derive(Debug, Deserialize)]
struct FixedPointExpected {
    raw_i64: i64,
    roundtrip_f64: f64,
}

#[test]
fn test_fixed_point_conformance() {
    let golden: GoldenFile<FixedPointInput, FixedPointExpected> =
        golden::load_golden("resource/fixed_point.json");

    for case in &golden.cases {
        let fp = FixedPoint::from_f64(case.input.f64_value);

        // Verify raw internal representation
        assert_eq!(
            fp.raw(),
            case.expected.raw_i64,
            "FAIL [{}]: FixedPoint::from_f64({}).raw() = {}, expected {}",
            case.description,
            case.input.f64_value,
            fp.raw(),
            case.expected.raw_i64
        );

        // Verify roundtrip back to f64
        let roundtrip = fp.to_f64();
        assert!(
            (roundtrip - case.expected.roundtrip_f64).abs() < 1e-10,
            "FAIL [{}]: FixedPoint roundtrip = {}, expected {}",
            case.description,
            roundtrip,
            case.expected.roundtrip_f64
        );
    }
}
