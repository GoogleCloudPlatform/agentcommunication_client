cmake_minimum_required(VERSION 3.15)

project(acs VERSION 1.0.0)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STARDARD_REQUIRED True)

# Set the build type to Release
set(CMAKE_BUILD_TYPE Release CACHE STRING "Choose the type of build." FORCE)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__FILENAME__='\"$(subst ${CMAKE_SOURCE_DIR}/,,$(abspath $<))\"'")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O3")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(gRPC CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)
option(protobuf_MODULE_COMPATIBLE TRUE)
find_package(Protobuf CONFIG REQUIRED)
find_package(CURL REQUIRED)
find_package(GTest CONFIG REQUIRED)

add_subdirectory(proto)

add_library(acs_agent_helper
  "cpp/acs_agent_helper.cc"
  "cpp/acs_agent_helper.h"
)
target_link_libraries(acs_agent_helper PUBLIC
  acs_proto
  ${CURL_LIBRARIES}
)
target_include_directories(
  acs_agent_helper PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}
)

add_library(acs_agent_client_reactor
  "cpp/acs_agent_client_reactor.cc"
  "cpp/acs_agent_client_reactor.h"
)
target_link_libraries(acs_agent_client_reactor PUBLIC
acs_agent_helper
)

add_library(fake_acs_agent_server_reactor
  "cpp/fake_acs_agent_server_reactor.cc"
  "cpp/fake_acs_agent_server_reactor.h"
)
target_link_libraries(fake_acs_agent_server_reactor PUBLIC
acs_agent_helper
)

add_library(acs_client SHARED
  "cpp/acs_agent_client.cc"
  "cpp/acs_agent_client.h"
)
target_link_libraries(acs_client PUBLIC
  acs_agent_client_reactor
  acs_agent_helper
)

add_executable(acs_agent_helper_test
 "cpp/acs_agent_helper_test.cc"
)
target_link_libraries(acs_agent_helper_test PUBLIC
 acs_agent_helper
 GTest::gtest_main
 GTest::gmock_main
)

add_executable(acs_agent_client_reactor_test
 "cpp/acs_agent_client_reactor_test.cc"
)
target_link_libraries(acs_agent_client_reactor_test PUBLIC
 acs_agent_client_reactor
 fake_acs_agent_server_reactor
 GTest::gtest_main
 GTest::gmock_main
)

add_executable(acs_agent_client_test
 "cpp/acs_agent_client_test.cc"
)
target_link_libraries(acs_agent_client_test PUBLIC
 acs_client
 fake_acs_agent_server_reactor
 GTest::gtest_main
 GTest::gmock_main
)