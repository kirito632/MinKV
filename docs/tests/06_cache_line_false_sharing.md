# Cache Line 对齐（伪共享专项测试）

> 对比有无 64 字节缓存行对齐对多线程并发性能的影响。

---

## 环境准备

参考 [00_environment_setup.md](./00_environment_setup.md)。

---

## 编译

此测试不在 CMakeLists.txt 中，需手动用 g++ 编译：

```bash
# 必须在 MinKV 根目录执行，不能在 build/ 里
cd ~/MinKV
mkdir -p build/bin
g++ -std=c++17 -O2 -march=native -mavx2 -mfma -pthread \
    src/tests/benchmark_false_sharing_focused.cpp \
    src/base/expiration_manager.cpp \
    src/base/append_file.cpp \
    src/base/async_logger.cpp \
    src/persistence/wal.cpp \
    src/persistence/group_commit.cpp \
    -Isrc \
    -o build/bin/benchmark_false_sharing_focused
```

编译成功无报错即可。

---

## 运行

```bash
cd ~/MinKV
./build/bin/benchmark_false_sharing_focused
```

---

## 测试场景参数

测试代码：`MinKV/src/tests/benchmark_false_sharing_focused.cpp`

| 参数 | 值 |
|------|-----|
| 分片数 | 32 |
| 每分片容量 | 1,000 |
| 线程数 | 硬件并发数（本机 8 线程） |
| 每线程操作数 | 1,000,000 次 |
| 读写比 | 50% GET + 50% PUT |
| Key 范围 | 0 ~ 31,999（预热数量的 2 倍） |

### 两种版本

| 版本 | 说明 |
|------|------|
| 普通版本（`EnableCacheAlign=false`） | 分片 mutex 无对齐，可能存在伪共享 |
| 对齐版本（`EnableCacheAlign=true`） | 分片 mutex 按 64 字节缓存行对齐 |

两个版本在同一次运行中依次测试，使用相同数据和线程数。

---

## 测试结果

| 版本 | QPS |
|------|-----|
| 普通版本（无对齐） | 329万 |
| 对齐版本（64字节对齐） | 339万 |
| 性能提升 | **1.03x（+3%）** |

结论：**缓存行对齐效果不明显（±5%以内），伪共享不是当前负载的主要瓶颈。**

---

## 原因分析

本项目已有 32 路分片锁，不同线程大概率访问不同分片，分片间伪共享机会本来就少。缓存行对齐在锁竞争极其激烈（如单锁）的场景下才会有显著收益，32 分片已经把锁竞争分散得足够开了。




