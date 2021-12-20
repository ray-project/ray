"""A helper module defining generated information about crate_universe dependencies"""

# This global should match the current release of `crate_unvierse`.
DEFAULT_URL_TEMPLATE = "https://github.com/bazelbuild/rules_rust/releases/download/crate_universe-13/crate_universe_resolver-{host_triple}{extension}"

# Note that if any additional platforms are added here, the pipeline defined
# by `create_universe.yaml` should also be updated
DEFAULT_SHA256_CHECKSUMS = {
    "aarch64-apple-darwin": "c6017cd8a4fee0f1796a8db184e9d64445dd340b7f48a65130d7ee61b97051b4",
    "aarch64-unknown-linux-gnu": "d0a310b03b8147e234e44f6a93e8478c260a7c330e5b35515336e7dd67150f35",
    "x86_64-apple-darwin": "762f1c77b3cf1de8e84d7471442af1314157efd90720c7e1f2fff68556830ee2",
    "x86_64-pc-windows-gnu": "c44bd97373d690587e74448b13267077d133f04e89bedfc9d521ae8ba55dddb9",
    "x86_64-unknown-linux-gnu": "aebf51af6a3dd33fdac463b35b0c3f4c47ab93e052099199673289e2025e5824",
}
