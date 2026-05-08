cmake_minimum_required(VERSION 3.16)
project(mini2 CXX)
set(CMAKE_CXX_STANDARD 17)

# ── Dependencies (install first: brew install grpc nlohmann-json) ──────────────
list(APPEND CMAKE_PREFIX_PATH /opt/homebrew)

find_package(gRPC REQUIRED)
find_package(Protobuf REQUIRED)
find_package(nlohmann_json REQUIRED)

# macOS: Apple Clang needs libomp from Homebrew for OpenMP
if(APPLE)
    execute_process(COMMAND brew --prefix libomp
        OUTPUT_VARIABLE LIBOMP_PREFIX OUTPUT_STRIP_TRAILING_WHITESPACE)
    set(OpenMP_C_FLAGS   "-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include")
    set(OpenMP_CXX_FLAGS "-Xpreprocessor -fopenmp -I${LIBOMP_PREFIX}/include")
    set(OpenMP_C_LIB_NAMES   "omp")
    set(OpenMP_CXX_LIB_NAMES "omp")
    set(OpenMP_omp_LIBRARY "${LIBOMP_PREFIX}/lib/libomp.dylib")
endif()
find_package(OpenMP REQUIRED)

# ── Proto codegen ──────────────────────────────────────────────────────────────
get_filename_component(PROTO_FILE "${CMAKE_SOURCE_DIR}/../proto/mini2.proto" ABSOLUTE)
get_filename_component(PROTO_DIR  "${PROTO_FILE}" DIRECTORY)

set(GEN "${CMAKE_BINARY_DIR}/proto_gen")
file(MAKE_DIRECTORY ${GEN})

add_custom_command(
    OUTPUT  "${GEN}/mini2.pb.cc"   "${GEN}/mini2.pb.h"
            "${GEN}/mini2.grpc.pb.cc" "${GEN}/mini2.grpc.pb.h"
    COMMAND $<TARGET_FILE:protobuf::protoc>
            --cpp_out=${GEN}
            --grpc_out=${GEN}
            --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
            -I${PROTO_DIR}
            ${PROTO_FILE}
    DEPENDS ${PROTO_FILE}
    VERBATIM
)

add_library(proto_lib STATIC
    "${GEN}/mini2.pb.cc"
    "${GEN}/mini2.grpc.pb.cc"
)
target_include_directories(proto_lib PUBLIC ${GEN} ${Protobuf_INCLUDE_DIRS})
target_link_libraries(proto_lib PUBLIC gRPC::grpc++ protobuf::libprotobuf)

# ── Server ─────────────────────────────────────────────────────────────────────
add_executable(node_server
    server/main.cpp
    server/data_store.cpp
    server/chunk_manager.cpp
    server/node_service.cpp
)
target_include_directories(node_server PRIVATE ${GEN} server)
target_link_libraries(node_server PRIVATE proto_lib nlohmann_json::nlohmann_json OpenMP::OpenMP_CXX)

# ── Client ─────────────────────────────────────────────────────────────────────
add_executable(node_client
    client/main.cpp
)
target_include_directories(node_client PRIVATE ${GEN})
target_link_libraries(node_client PRIVATE proto_lib nlohmann_json::nlohmann_json)