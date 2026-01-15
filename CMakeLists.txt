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

# 查找所有源文件 (缓存库核心: db 目录)
file(GLOB_RECURSE SOURCES 
    "src/db/*.cpp"
    "src/base/*.cpp"
)

# 以后这里会添加 executable
# add_executable(minkv_server ${SOURCES} src/main.cpp)

# ==========================================
# 向量化功能测试 (Vector Search Tests)
# ==========================================
add_executable(benchmark_vector src/tests/benchmark_vector.cpp ${SOURCES})
add_executable(demo_vector_db src/tests/demo_vector_db.cpp ${SOURCES})
add_executable(test_vector_integration src/tests/test_vector_integration.cpp ${SOURCES})
add_executable(test_integration src/tests/test_integration.cpp ${SOURCES})

# ==========================================
# 并发性能测试 (Concurrency Benchmarks)
# ==========================================
# add_executable(benchmark_false_sharing benchmark_false_sharing.cpp ${SOURCES})
# target_link_libraries(benchmark_false_sharing pthread)

# ==========================================
# 修复验证测试 (Fix Verification Tests)
# ==========================================
add_executable(simple_fix_test src/tests/simple_fix_test.cpp ${SOURCES})
target_link_libraries(simple_fix_test pthread)

# ==========================================
# 综合压力测试 (Comprehensive Benchmark)
# ==========================================
add_executable(comprehensive_benchmark src/tests/comprehensive_benchmark.cpp ${SOURCES})
target_link_libraries(comprehensive_benchmark pthread)

# ==========================================
# WAL性能对比测试 (WAL Performance Benchmark)
# ==========================================
add_executable(wal_performance_benchmark src/tests/wal_performance_benchmark.cpp ${SOURCES})
target_link_libraries(wal_performance_benchmark pthread)

# ==========================================
# 异步日志系统测试 (Async Logger Test)
# ==========================================
add_executable(async_logger_test src/tests/async_logger_test.cpp ${SOURCES})
target_link_libraries(async_logger_test pthread)

# ==========================================
# Group Commit系统测试 (Group Commit Test)
# ==========================================
add_executable(group_commit_test src/tests/group_commit_test.cpp ${SOURCES})
target_link_libraries(group_commit_test pthread)

# ==========================================
# 快照系统测试 (Snapshot Test)
# ==========================================
add_executable(snapshot_test src/tests/snapshot_test.cpp ${SOURCES})
target_link_libraries(snapshot_test pthread)
