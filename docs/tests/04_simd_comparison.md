# SIMD 算法级对比测试

> 纯算法层面对比标量 vs AVX2 的 L2 距离计算性能，排除系统锁竞争等干扰。

---

## 环境准备

参考 [00_environment_setup.md](./00_environment_setup.md)。

---

## 编译

`simd_comparison_simple.cpp` 是独立文件，不依赖项目其他 `.cpp`，直接 g++ 单文件编译：

```bash
# 必须在 MinKV 根目录执行，不能在 build/ 里
cd ~/MinKV
mkdir -p build/bin
g++ -std=c++17 -O2 -march=native -mavx2 -mfma \
    src/tests/simd_comparison_simple.cpp \
    -o build/bin/simd_comparison_simple
```

编译成功无报错即可。

> 注意：不需要链接 `expiration_manager.cpp`、`wal.cpp` 等，该文件只用了标准库。

---

## 运行

```bash
cd ~/MinKV
./build/bin/simd_comparison_simple
```

---

## 测试场景参数

测试代码：`MinKV/src/tests/simd_comparison_simple.cpp`

| 参数 | 值 |
|------|-----|
| 向量维度 | 512 |
| 数据集大小 | 10,000 个向量 |
| 查询次数 | 100,000 次 |
| 测试内容 | 单线程 L2 距离计算（标量 vs AVX2） |
| 线程数 | 1（纯算法级，无并发） |

### 两种实现

| 版本 | 说明 |
|------|------|
| 标量版本 | 逐元素循环计算，`for (i) sum += diff*diff` |
| SIMD 版本 | AVX2 指令，每次并行处理 8 个 float，使用 FMA 指令（`_mm256_fmadd_ps`） |

---

## 测试结果

> 以下为 2026-03-17 定版测试数据，腾讯云竞价实例上海8区，`-O2 -march=native -mavx2 -mfma`

| 指标 | 标量版本 | SIMD 版本 | 提升 |
|------|---------|----------|------|
| 耗时 | 24 ms | 5 ms | -79% |
| QPS | 409万 | 1,873万 | +358% |
| 平均延迟 | 0.24 μs | 0.05 μs | -79% |

- 加速比：**4.58x**（理论最大 8x）
- 效率：**57.2%**（实际加速比 / 理论最大加速比）

<!-- 如需重新测试，将服务器输出粘贴到这里替换上方数据 -->

---

## 原因分析

实际效率 57.2%（而非理论 100%）的原因：

1. **内存带宽限制**：512维向量 = 2KB 数据，频繁访问 10,000 个向量会产生 cache miss
2. **水平求和开销**：AVX2 没有直接的水平加法，需要 `_mm256_storeu_ps` + 标量累加
3. **循环控制开销**：每次迭代的循环判断、指针递增等
4. **编译器自动向量化**：`-O2 -march=native` 下标量版本可能已被编译器部分向量化

---

## 说明

此测试是纯算法级别的对比，不涉及系统锁、哈希、LRU、网络等开销。

实际系统中 SIMD 对 KV 操作的影响很小（±3% 误差范围），因为向量计算仅占 KV 系统总耗时的 1~2%，系统瓶颈在哈希查找和锁管理。但对向量检索专项有明显提升（约 +8.4%，P99 降低 14.3%）。

这验证了 Amdahl 定律：算法级 4.58x 加速在系统级被稀释到几乎不可见。
