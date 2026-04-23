import matplotlib.pyplot as plt
import pandas as pd
import sys

# 如果没有安装 pandas/matplotlib，给个提示
try:
    import pandas as pd
    import matplotlib.pyplot as plt
except ImportError:
    print("Please install pandas and matplotlib: pip install pandas matplotlib")
    sys.exit(1)

def plot_benchmark():
    try:
        df = pd.read_csv('benchmark_results.csv')
    except FileNotFoundError:
        print("Error: benchmark_results.csv not found.")
        return

    # 设置风格
    plt.style.use('seaborn-v0_8-whitegrid')

    # 1. QPS Comparison
    plt.figure(figsize=(10, 6))
    
    scenarios = df['Scenario'].unique()
    for scenario in scenarios:
        data = df[df['Scenario'] == scenario]
        # 排除掉 Redis(Local) 如果数据是假的且不想展示
        # if 'Redis' in scenario: continue 
        plt.plot(data['Threads'], data['QPS'], marker='o', linewidth=2, label=scenario)

    plt.title('Throughput Comparison (Higher is Better)', fontsize=14)
    plt.xlabel('Concurrent Threads', fontsize=12)
    plt.ylabel('QPS (Queries Per Second)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.savefig('benchmark_qps.png', dpi=300)
    print("Saved benchmark_qps.png")

    # 2. Latency Comparison
    plt.figure(figsize=(10, 6))
    for scenario in scenarios:
        data = df[df['Scenario'] == scenario]
        plt.plot(data['Threads'], data['P99_Latency_us'], marker='s', linewidth=2, label=scenario)

    plt.title('P99 Latency Comparison (Lower is Better)', fontsize=14)
    plt.xlabel('Concurrent Threads', fontsize=12)
    plt.ylabel('Latency (microseconds)', fontsize=12)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.savefig('benchmark_latency.png', dpi=300)
    print("Saved benchmark_latency.png")

if __name__ == "__main__":
    plot_benchmark()
