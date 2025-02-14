#ifndef THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_JWT_H_
#define THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_JWT_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/types/span.h"

// An exception-free wrapper around the JWT library that provides a simple
// interface for decoding and parse the claims of a JWT token. The external JWT
// library is in https://github.com/Thalhammer/jwt-cpp.
// The JWT library has exceptions enabled, this wrapper would convert them to
// absl::Status. The wrapper implementation tries to follow the instructions in
// http://go/cpp-3p-exceptions as closely as possible.
namespace agent_communication {

// Returns the value of the last key in the `keys` from the payload of the JWT
// `token`. The `keys` should be a list of strings that represents the path of
// the value in the payload. For example, if the JWT token has the following
// payload:
// {
//   "aud": "test_aud",
//   "sub": "test_sub",
//   "google": {
//     "compute_engine": {
//       "zone": "test_zone",
//       "project_number": 123456789
//     }
//   }
// }
// and the `keys` is {"google", "compute_engine", "zone"}, then the function
// will return "test_zone".
// and if the `keys` is {"google", "compute_engine", "project_number"}, then
// the function will return "123456789" as a string.
// The function will return an error status if any of the following happens:
// 1. The `token` is invalid.
// 2. The `keys` is empty.
// 3. The `token` is not a JWT.
// 4. Any of the key in the `keys` is not found in the payload.
// 5. Any of the key in the `keys` except the last one is not an object.
absl::StatusOr<std::string> GetValueFromTokenPayloadWithKeys(
    const std::string& token, absl::Span<const std::string> keys);
}  // namespace agent_communication

#endif  // THIRD_PARTY_AGENTCOMMUNICATION_CLIENT_CPP_JWT_H_
