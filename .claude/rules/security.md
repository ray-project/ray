<!-- Security rules applying to all code -->
- Add token-based authentication to all new gRPC endpoints and RPC handlers to maintain Ray's security model
- Propagate auth tokens in new API calls (both C++ and Python sides) so that end-to-end authentication remains consistent
