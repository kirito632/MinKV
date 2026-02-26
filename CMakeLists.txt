cmake_minimum_required(VERSION 3.10)
project(MinKV VERSION 0.1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

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
    "src/server/*.cpp"
    "src/vector/*.cpp"
)

# MinKV 静态库
add_library(minkv STATIC ${SOURCES})

# 单元测试
add_executable(test_lru src/tests/unit/test_lru.cpp)
target_link_libraries(test_lru minkv pthread)

add_executable(test_lockfree src/tests/unit/test_lockfree.cpp)
target_link_libraries(test_lockfree minkv pthread)

add_executable(test_template_usage src/tests/unit/test_template_usage.cpp)
target_link_libraries(test_template_usage minkv pthread)

add_executable(test_stats src/tests/unit/test_stats.cpp)
target_link_libraries(test_stats minkv pthread)

add_executable(test_monitoring src/tests/unit/test_monitoring.cpp)
target_link_libraries(test_monitoring minkv pthread)

add_executable(test_parser src/tests/unit/test_parser.cpp)
target_link_libraries(test_parser minkv pthread)

# 性能基准测试
add_executable(benchmark_performance src/tests/bench/benchmark_performance.cpp)
target_link_libraries(benchmark_performance minkv pthread)

# 历史基准测试 (可选)
add_executable(benchmark_legacy src/tests/legacy/benchmark.cpp)
target_link_libraries(benchmark_legacy minkv pthread)

add_executable(benchmark_v2 src/tests/legacy/benchmark_v2.cpp)
target_link_libraries(benchmark_v2 minkv pthread)

add_executable(benchmark_v3 src/tests/legacy/benchmark_v3.cpp)
target_link_libraries(benchmark_v3 minkv pthread)

add_executable(benchmark_v4 src/tests/legacy/benchmark_v4.cpp)
target_link_libraries(benchmark_v4 minkv pthread)

add_executable(benchmark_v5_final src/tests/legacy/benchmark_v5_final.cpp)
target_link_libraries(benchmark_v5_final minkv pthread)
