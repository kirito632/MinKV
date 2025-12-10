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

# 查找所有源文件 (缓存库核心: db 目录)
file(GLOB_RECURSE SOURCES 
    "src/db/*.cpp"
    "src/base/*.cpp"
)

# 以后这里会添加 executable
# add_executable(minkv_server ${SOURCES} src/main.cpp)

# 为了测试方便，我们先编译一个简单的测试目标
add_executable(unit_tests src/tests/test_lru.cpp ${SOURCES})
add_executable(test_optimistic src/tests/test_lockfree.cpp ${SOURCES})
add_executable(test_template_usage src/tests/test_template_usage.cpp ${SOURCES})
add_executable(test_stats src/tests/test_stats.cpp ${SOURCES})
add_executable(benchmark src/tests/benchmark.cpp ${SOURCES})
add_executable(benchmark_v2 src/tests/benchmark_v2.cpp ${SOURCES})
add_executable(benchmark_v3 src/tests/benchmark_v3.cpp ${SOURCES})
add_executable(benchmark_v4 src/tests/benchmark_v4.cpp ${SOURCES})
add_executable(benchmark_v5_final src/tests/benchmark_v5_final.cpp ${SOURCES})
