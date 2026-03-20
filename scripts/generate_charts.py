"""
MinKV Benchmark Charts Generator
数据来源: MinKV/docs/tests/RELEASE_BENCHMARK.md (2026-03-17 定版)
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import os

# 输出目录
OUT_DIR = os.path.join(os.path.dirname(__file__), "../docs/images")
os.makedirs(OUT_DIR, exist_ok=True)

# 全局样式
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 12,
    "axes.titlesize": 14,
    "axes.titleweight": "bold",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "figure.dpi": 150,
})

BLUE   = "#2563EB"
ORANGE = "#EA580C"
GREEN  = "#16A34A"
GRAY   = "#6B7280"

# ─────────────────────────────────────────────
# 图1: QPS vs 线程数（并发基准，R90W10）
# ─────────────────────────────────────────────
def chart_qps_vs_threads():
    threads = [1, 2, 4, 8, 16]
    qps     = [3.72, 5.08, 3.93, 3.10, 2.86]   # 单位：百万 QPS

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(threads, qps, color=BLUE, width=0.6, zorder=3)

    # 标注峰值
    peak_idx = qps.index(max(qps))
    bars[peak_idx].set_color(ORANGE)

    # 数值标签
    for bar, val in zip(bars, qps):
        ax.text(bar.get_x() + bar.get_width()/2, val + 0.05,
                f"{val:.2f}M", ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax.set_xlabel("Number of Threads", fontsize=12)
    ax.set_ylabel("QPS (Million ops/sec)", fontsize=12)
    ax.set_title("Concurrent QPS Benchmark\n(32 shards, 90% GET + 10% PUT, -O2)")
    ax.set_xticks(threads)
    ax.set_ylim(0, 6.2)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x:.0f}M"))
    ax.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)
    ax.annotate("Peak: 5.08M QPS\n(2 threads)", xy=(2, 5.08),
                xytext=(4.5, 5.4), fontsize=10, color=ORANGE,
                arrowprops=dict(arrowstyle="->", color=ORANGE))

    fig.tight_layout()
    path = os.path.join(OUT_DIR, "01_qps_vs_threads.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ {path}")

# ─────────────────────────────────────────────
# 图2: WAL 持久化损耗对比
# ─────────────────────────────────────────────
def chart_wal_overhead():
    threads        = [1, 2, 4, 8]
    qps_no_wal     = [3.42, 3.94, 3.76, 3.09]
    qps_group_commit = [2.23, 3.02, 3.11, 2.76]
    loss_pct       = [34.9, 23.4, 17.4, 10.6]

    x = np.arange(len(threads))
    width = 0.35

    fig, ax1 = plt.subplots(figsize=(9, 5))

    b1 = ax1.bar(x - width/2, qps_no_wal,      width, label="No WAL (baseline)", color=BLUE,   zorder=3)
    b2 = ax1.bar(x + width/2, qps_group_commit, width, label="Group Commit (10ms)", color=GREEN, zorder=3)

    for bar, val in zip(b1, qps_no_wal):
        ax1.text(bar.get_x() + bar.get_width()/2, val + 0.04,
                 f"{val:.2f}M", ha="center", va="bottom", fontsize=9)
    for bar, val in zip(b2, qps_group_commit):
        ax1.text(bar.get_x() + bar.get_width()/2, val + 0.04,
                 f"{val:.2f}M", ha="center", va="bottom", fontsize=9)

    ax1.set_xlabel("Number of Threads", fontsize=12)
    ax1.set_ylabel("QPS (Million ops/sec)", fontsize=12)
    ax1.set_title("Persistence Overhead with Group Commit WAL\n(10ms interval vs No WAL)")
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"{t}T" for t in threads])
    ax1.set_ylim(0, 5.0)
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:.0f}M"))
    ax1.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)

    # 右轴：损耗百分比折线
    ax2 = ax1.twinx()
    ax2.plot(x, loss_pct, color=ORANGE, marker="o", linewidth=2,
             linestyle="--", label="Overhead %", zorder=4)
    for xi, pct in zip(x, loss_pct):
        ax2.text(xi + 0.05, pct + 0.8, f"{pct}%", color=ORANGE, fontsize=9)
    ax2.set_ylabel("Overhead (%)", fontsize=12, color=ORANGE)
    ax2.tick_params(axis="y", labelcolor=ORANGE)
    ax2.set_ylim(0, 50)
    ax2.spines["right"].set_visible(True)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right", fontsize=10)

    fig.tight_layout()
    path = os.path.join(OUT_DIR, "02_wal_overhead.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ {path}")

# ─────────────────────────────────────────────
# 图3: SIMD 算法级加速（标量 vs AVX2）
# ─────────────────────────────────────────────
def chart_simd_speedup():
    labels   = ["Scalar\n(loop)", "AVX2 SIMD\n(8-wide FMA)"]
    qps      = [4.09, 18.73]   # 百万 QPS
    latency  = [0.24, 0.05]    # μs

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))

    # QPS 柱状图
    colors = [GRAY, BLUE]
    b = ax1.bar(labels, qps, color=colors, width=0.45, zorder=3)
    for bar, val in zip(b, qps):
        ax1.text(bar.get_x() + bar.get_width()/2, val + 0.3,
                 f"{val:.2f}M", ha="center", va="bottom", fontsize=11, fontweight="bold")
    ax1.set_ylabel("QPS (Million ops/sec)", fontsize=12)
    ax1.set_title("AVX2 SIMD Speedup — QPS\n(512-dim L2 distance, 100K queries)")
    ax1.set_ylim(0, 23)
    ax1.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:.0f}M"))
    ax1.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)
    ax1.annotate("4.58x faster", xy=(1, 18.73), xytext=(0.6, 20.5),
                 fontsize=11, color=BLUE, fontweight="bold",
                 arrowprops=dict(arrowstyle="->", color=BLUE))

    # 延迟柱状图
    b2 = ax2.bar(labels, latency, color=colors, width=0.45, zorder=3)
    for bar, val in zip(b2, latency):
        ax2.text(bar.get_x() + bar.get_width()/2, val + 0.003,
                 f"{val:.2f}μs", ha="center", va="bottom", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Avg Latency (μs)", fontsize=12)
    ax2.set_title("AVX2 SIMD Speedup — Latency\n(lower is better)")
    ax2.set_ylim(0, 0.32)
    ax2.grid(axis="y", linestyle="--", alpha=0.5, zorder=0)

    fig.suptitle("AVX2 SIMD vs Scalar  |  QPS: 18.73M vs 4.09M  |  4.58x faster (57% of theoretical 8x)",
                 fontsize=11, color=GRAY)
    fig.tight_layout()
    path = os.path.join(OUT_DIR, "03_simd_speedup.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ {path}")

# ─────────────────────────────────────────────
# 图4: 分片消融测试
# ─────────────────────────────────────────────
def chart_shard_ablation():
    shards  = [1, 2, 4, 8, 16, 32, 64]
    qps     = [1.71, 2.48, 2.99, 3.02, 3.02, 3.04, 2.99]
    rel     = [0.57, 0.82, 0.99, 0.99, 0.99, 1.00, 0.99]

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.plot(shards, qps, color=BLUE, marker="o", linewidth=2.5, zorder=3)

    # 标注最优点
    best_idx = qps.index(max(qps))
    ax.scatter([shards[best_idx]], [qps[best_idx]], color=ORANGE, s=120, zorder=5)
    ax.annotate(f"Best: 32 shards\n{qps[best_idx]:.2f}M QPS (+77% vs 1 shard)",
                xy=(shards[best_idx], qps[best_idx]),
                xytext=(20, 2.5), fontsize=10, color=ORANGE,
                arrowprops=dict(arrowstyle="->", color=ORANGE))

    for x, y in zip(shards, qps):
        ax.text(x, y + 0.05, f"{y:.2f}M", ha="center", fontsize=9)

    ax.set_xlabel("Number of Shards", fontsize=12)
    ax.set_ylabel("QPS (Million ops/sec)", fontsize=12)
    ax.set_title("QPS vs Shard Count Ablation\n(8 threads, 70% PUT + 30% GET, 100K key range)")
    ax.set_xscale("log", base=2)
    ax.set_xticks(shards)
    ax.get_xaxis().set_major_formatter(ticker.ScalarFormatter())
    ax.set_ylim(1.0, 3.6)
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda v, _: f"{v:.1f}M"))
    ax.grid(linestyle="--", alpha=0.5, zorder=0)

    fig.tight_layout()
    path = os.path.join(OUT_DIR, "04_shard_ablation.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ {path}")

# ─────────────────────────────────────────────
# 图5: P99 延迟 vs 线程数
# ─────────────────────────────────────────────
def chart_p99_latency():
    threads = [1, 2, 4, 8, 16]
    p99     = [0.66, 2.81, 8.85, 18.52, 51.65]   # μs

    fig, ax = plt.subplots(figsize=(8, 5))
    ax.plot(threads, p99, color=ORANGE, marker="o", linewidth=2.5, zorder=3)
    ax.fill_between(threads, p99, alpha=0.1, color=ORANGE)

    for x, y in zip(threads, p99):
        ax.text(x, y + 1.2, f"{y:.2f}μs", ha="center", fontsize=10)

    ax.set_xlabel("Number of Threads", fontsize=12)
    ax.set_ylabel("P99 Latency (μs)", fontsize=12)
    ax.set_title("P99 Latency vs Thread Count\n(32 shards, 90% GET + 10% PUT)")
    ax.set_xticks(threads)
    ax.set_ylim(0, 62)
    ax.grid(linestyle="--", alpha=0.5, zorder=0)
    ax.annotate("Sharp contention increase\nbeyond 2 threads",
                xy=(4, 8.85), xytext=(8, 15), fontsize=9, color=GRAY,
                arrowprops=dict(arrowstyle="->", color=GRAY))

    fig.tight_layout()
    path = os.path.join(OUT_DIR, "05_p99_latency.png")
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    print(f"✓ {path}")


if __name__ == "__main__":
    print("生成 MinKV Benchmark 图表...")
    chart_qps_vs_threads()
    chart_wal_overhead()
    chart_simd_speedup()
    chart_shard_ablation()
    chart_p99_latency()
    print(f"\n全部完成，图片保存在 MinKV/docs/images/")
