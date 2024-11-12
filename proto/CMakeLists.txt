include(FindProtobuf)

set(PROTO_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}")
set(PROTO_IMPORT_DIRS
 "${CMAKE_CURRENT_SOURCE_DIR}"
 "${Protobuf_INCLUDE_DIRS}"
 "${Protobuf_INCLUDE_DIR}"
)

add_library(acs_proto OBJECT
"agent_communication.proto"
"client.proto"
"field_behavior.proto"
"launch_stage.proto"
"status.proto"
)
target_link_libraries(acs_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)

protobuf_generate(
    TARGET acs_proto
    IMPORT_DIRS ${PROTO_IMPORT_DIRS}
    PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")

protobuf_generate(
    TARGET acs_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=\$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    IMPORT_DIRS ${PROTO_IMPORT_DIRS}
    PROTOC_OUT_DIR "${PROTO_BINARY_DIR}")

target_include_directories(acs_proto
PUBLIC
${PROTO_BINARY_DIR}
${CMAKE_CURRENT_SOURCE_DIR}
)
