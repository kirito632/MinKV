# 分片消融测试

> 测试不同分片数对性能的影响，找到最优分片配置。

---

## 环境准备

参考 [00_environment_setup.md](./00_environment_setup.md)。

---

## 编译

`shard_ablation_quick.cpp` 依赖 `sharded_cache.h`，后者会引入 WAL 和过期管理模块，需要手动用 g++ 链接相关 `.cpp`（不在 CMakeLists.txt 中）：

```bash
# 必须在 MinKV 根目录执行，不能在 build/ 里
cd ~/MinKV
mkdir -p build/bin
g++ -std=c++17 -O2 -march=native -mavx2 -mfma -pthread \
    src/tests/shard_ablation_quick.cpp \
    src/base/expiration_manager.cpp \
    src/base/append_file.cpp \
    src/base/async_logger.cpp \
    src/persistence/wal.cpp \
    src/persistence/group_commit.cpp \
    -Isrc \
    -o build/bin/shard_ablation_quick
```

编译成功无报错即可。

---

## 运行

```bash
cd ~/MinKV
./build/bin/shard_ablation_quick
```

运行结束后，结果会同时打印到终端，并自动保存到 `~/MinKV/shard_ablation_quick_results.csv`。

---

## 测试场景参数

测试代码：`MinKV/src/tests/shard_ablation_quick.cpp`

| 参数 | 值 |
|------|-----|
| 固定线程数 | 8 |
| 每线程操作数 | 50,000 次 |
| 总操作数 | 400,000 次 |
| 预填充数据量 | 10,000 条 |
| Key 范围 | 0 ~ 99,999（10万） |
| 读写比 | 30% GET + 70% PUT |
| 测试分片数 | 1 / 2 / 4 / 8 / 16 / 32 / 64 |

> 注意：此测试读写比（W70R30）和 key 范围（10万）与综合性能测试不同，不能直接横向比较 QPS 绝对值。

---

## 测试结果

| 分片数 | QPS | 相对性能 | 评价 |
|--------|-----|---------|------|
| 1 | 171万 | 0.57x | 严重锁竞争 |
| 2 | 248万 | 0.82x | 明显改善 |
| 4 | 299万 | 0.99x | 接近最优 |
| 8 | 302万 | 0.99x | 接近最优 |
| 16 | 302万 | 0.99x | 接近最优 |
| 32 | **304万** | **1.00x** | 最优 |
| 64 | 299万 | 0.99x | 略有下降 |

### 关键结论

- 最优分片数：**32**
- 峰值 QPS：**304万**（8线程，W70R30）
- 相比 1 分片提升：**+77%**
- 4 分片起性能趋于饱和，16~64 分片差异在 1% 以内

---

## 原因分析

分片数从 1 增加到 N 时，锁竞争从"所有线程争一把锁"变为"平均 8/N 个线程争一把锁"，理论上线性提升。但超过某个阈值后：

1. 分片数过多，每个分片的 LRU 链表太短，缓存命中率下降
2. 哈希计算本身的开销占比上升
3. 内存碎片增加

因此存在一个最优分片数，通常在 `线程数 × 2` 附近（8线程 → 16~32分片）。
