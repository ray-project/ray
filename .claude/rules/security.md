<!-- Security rules applying to all code -->
- Never commit secrets, tokens, or credentials
- All new gRPC endpoints and RPC handlers must support token-based authentication
- New API calls must include auth token propagation (both C++ and Python sides)
