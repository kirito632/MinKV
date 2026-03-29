cmake_minimum_required(VERSION 3.10)
project(MinKV VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ==========================================
# 【关键】启用 AVX2 和 FMA 指令集
# 这是向量化优化的必要条件
# ==========================================
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-mavx2 -mfma)
elseif(MSVC)
    add_compile_options(/arch:AVX2)
endif()

# 设置输出目录
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

# 包含头文件路径
include_directories(src)

# 查找所有源文件
file(GLOB_RECURSE SOURCES 
    "src/base/*.cpp"
    "src/core/*.cpp"
    "src/db/*.cpp"
    "src/persistence/*.cpp"
    "src/vector/*.cpp"
    "src/graph/graph_serializer.cpp"
    "src/graph/graph_store.cpp"
)
# src/server/*.cpp excluded: requires httplib.h and nlohmann/json.hpp

# 以后这里会添加 executable
# add_executable(minkv_server ${SOURCES} src/main.cpp)

# ==========================================
# 向量化功能测试 (Vector Search Tests)
# ==========================================
add_executable(benchmark_vector tests/benchmark_vector.cpp ${SOURCES})
add_executable(demo_vector_db tests/demo_vector_db.cpp ${SOURCES})
add_executable(test_vector_integration tests/test_vector_integration.cpp ${SOURCES})
# add_executable(test_integration tests/test_integration.cpp ${SOURCES})

# ==========================================
# 并发性能测试 (Concurrency Benchmarks)
# ==========================================
# add_executable(benchmark_false_sharing benchmark_false_sharing.cpp ${SOURCES})
# target_link_libraries(benchmark_false_sharing pthread)

# ==========================================
# 修复验证测试 (Fix Verification Tests)
# ==========================================
# add_executable(simple_fix_test src/tests/simple_fix_test.cpp ${SOURCES})
# target_link_libraries(simple_fix_test pthread)

# ==========================================
# 综合压力测试 (Comprehensive Benchmark)
# ==========================================
add_executable(comprehensive_benchmark src/tests/comprehensive_benchmark.cpp ${SOURCES})
target_link_libraries(comprehensive_benchmark pthread)

# ==========================================
# WAL性能对比测试 (WAL Performance Benchmark)
# ==========================================
add_executable(wal_performance_benchmark tests/wal_performance_benchmark.cpp ${SOURCES})
target_link_libraries(wal_performance_benchmark pthread)

# ==========================================
# 异步日志系统测试 (Async Logger Test)
# ==========================================
add_executable(async_logger_test tests/async_logger_test.cpp ${SOURCES})
target_link_libraries(async_logger_test pthread)

# ==========================================
# 过期管理异常处理测试 (Expiration Exception Test)
# ==========================================
add_executable(expiration_exception_test tests/expiration_exception_test.cpp ${SOURCES})
target_link_libraries(expiration_exception_test pthread)

# ==========================================
# Group Commit系统测试 (Group Commit Test)
# ==========================================
add_executable(group_commit_test tests/group_commit_test.cpp ${SOURCES})
target_link_libraries(group_commit_test pthread)

# ==========================================
# 快照系统测试 (Snapshot Test)
# ==========================================
add_executable(snapshot_test tests/snapshot_test.cpp ${SOURCES})
target_link_libraries(snapshot_test pthread)

# ==========================================
# Graph-on-KV 测试 (Graph Layer Tests)
# ==========================================
# Graph source files (serializer + store)
set(GRAPH_SOURCES
    src/graph/graph_serializer.cpp
    src/graph/graph_store.cpp
)

# Serializer property-based tests (Phase 1, rapidcheck)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/graph/test_graph_serializer.cpp")
    add_executable(test_graph_serializer
        tests/graph/test_graph_serializer.cpp
        ${GRAPH_SOURCES}
        ${SOURCES}
    )
    target_link_libraries(test_graph_serializer pthread rapidcheck)
endif()

# GraphRAG 功能 + 性能 + 一致性演示
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/graph/demo_graphrag.cpp")
    add_executable(demo_graphrag
        tests/graph/demo_graphrag.cpp
        ${GRAPH_SOURCES}
        ${SOURCES}
    )
    target_link_libraries(demo_graphrag pthread)
endif()

# Store CRUD unit tests + PBT (Phase 1, rapidcheck)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/graph/test_graph_store_phase1.cpp")
    add_executable(test_graph_store_phase1
        tests/graph/test_graph_store_phase1.cpp
        ${GRAPH_SOURCES}
        ${SOURCES}
    )
    target_link_libraries(test_graph_store_phase1 pthread rapidcheck)
endif()

# Adjacency consistency PBT (Phase 2, rapidcheck)
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/graph/test_graph_store_phase2.cpp")
    add_executable(test_graph_store_phase2
        tests/graph/test_graph_store_phase2.cpp
        ${GRAPH_SOURCES}
        ${SOURCES}
    )
    target_link_libraries(test_graph_store_phase2 pthread rapidcheck)
endif()

# MCP Server 功能模拟测试（不依赖 HTTP Server 和 OpenAI）
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/graph/test_mcp_simulation.cpp")
    add_executable(test_mcp_simulation
        tests/graph/test_mcp_simulation.cpp
        ${GRAPH_SOURCES}
        ${SOURCES}
    )
    target_link_libraries(test_mcp_simulation pthread)
endif()

# GraphRAG 准确性测试（手工语义向量，科技公司知识图谱）
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/graph/test_graphrag_accuracy.cpp")
    add_executable(test_graphrag_accuracy
        tests/graph/test_graphrag_accuracy.cpp
        ${GRAPH_SOURCES}
        ${SOURCES}
    )
    target_link_libraries(test_graphrag_accuracy pthread)
endif()

# ==========================================
# HTTP Server Example (Graph + Vector API)
# ==========================================
# 注意：http_server.cpp 依赖 httplib.h 和完整的向量接口，
# 向量接口对接尚未完成，暂不加入默认构建。
# 如需构建，参考 build_http_server.sh 脚本。
# find_package(nlohmann_json QUIET)
# if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/examples/http_server_example.cpp")
#     add_executable(http_server_example ...)
# endif()
