use crate::golden::{self, GoldenFile};
use prost::Message;
use ray_proto::rpc::Address;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct AddressInput {
    ip_address: String,
    port: i32,
    node_id_hex: String,
    worker_id_hex: String,
}

#[derive(Debug, Deserialize)]
struct AddressExpected {
    binary_hex: String,
}

#[test]
fn test_address_proto_encoding_conformance() {
    let golden: GoldenFile<AddressInput, AddressExpected> =
        golden::load_golden("proto/address.json");

    for case in &golden.cases {
        let node_id = if case.input.node_id_hex.is_empty() {
            vec![]
        } else {
            hex::decode(&case.input.node_id_hex).unwrap_or_else(|e| {
                panic!(
                    "FAIL [{}]: invalid node_id hex: {e}",
                    case.description
                )
            })
        };

        let worker_id = if case.input.worker_id_hex.is_empty() {
            vec![]
        } else {
            hex::decode(&case.input.worker_id_hex).unwrap_or_else(|e| {
                panic!(
                    "FAIL [{}]: invalid worker_id hex: {e}",
                    case.description
                )
            })
        };

        let address = Address {
            node_id,
            ip_address: case.input.ip_address.clone(),
            port: case.input.port,
            worker_id,
        };

        let mut buf = Vec::new();
        address.encode(&mut buf).unwrap();
        let result_hex = hex::encode(&buf);

        assert_eq!(
            result_hex, case.expected.binary_hex,
            "FAIL [{}]: Address encoding mismatch\n  got:      {}\n  expected: {}",
            case.description, result_hex, case.expected.binary_hex
        );

        // Verify decode roundtrip
        let decoded = Address::decode(buf.as_slice()).unwrap_or_else(|e| {
            panic!(
                "FAIL [{}]: Address decode failed: {e}",
                case.description
            )
        });
        assert_eq!(
            decoded, address,
            "FAIL [{}]: Address decode roundtrip mismatch",
            case.description
        );
    }
}
