#!/usr/bin/env python3
"""
AegisDB Research Benchmark Plotting Script (Phase 11)
Reads experiments/data/results.json and generates publication-ready research graphs.
Master Project Plan §20 (RQ1, RQ2, RQ3) & §21
"""

import json
import os
import sys

def main():
    results_path = os.path.join("experiments", "data", "results.json")
    if not os.path.exists(results_path):
        print(f"Error: {results_path} not found. Run benchmark suite first.")
        sys.exit(1)

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("Warning: matplotlib not installed. Skipping graphical plot generation.")
        print("Raw benchmark results are available in experiments/data/results.csv and results.json")
        sys.exit(0)

    output_dir = os.path.join("experiments", "graphs")
    os.makedirs(output_dir, exist_ok=True)

    with open(results_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Loaded {len(data)} experiment results. Generating research figures...")

    # 1. Plot RQ1: Raft Write Batching
    rq1_data = [d for d in data if "RQ1" in d["config"]["experimentName"]]
    if rq1_data:
        plot_rq1(rq1_data, output_dir, plt)

    # 2. Plot RQ2: Fault Recovery & Availability
    rq2_data = [d for d in data if "RQ2" in d["config"]["experimentName"]]
    if rq2_data:
        plot_rq2(rq2_data, output_dir, plt)

    # 3. Plot RQ3: MVCC Contention & Abort Rates
    rq3_data = [d for d in data if "RQ3" in d["config"]["experimentName"]]
    if rq3_data:
        plot_rq3(rq3_data, output_dir, plt)

    print(f"All research graphs generated successfully in: {output_dir}")

def plot_rq1(data, output_dir, plt):
    data.sort(key=lambda x: x["config"]["batchSize"])
    batches = [d["config"]["batchSize"] for d in data]
    throughput = [d["throughputOpsSec"] for d in data]
    p99 = [d["p99LatencyMs"] for d in data]

    fig, ax1 = plt.subplots(figsize=(8, 5))

    color = "tab:blue"
    ax1.set_xlabel("Raft Write Batch Size (entries)", fontsize=12)
    ax1.set_ylabel("Throughput (ops/sec)", color=color, fontsize=12)
    line1 = ax1.plot(batches, throughput, color=color, marker="o", linewidth=2.5, label="Throughput (ops/sec)")
    ax1.tick_params(axis="y", labelcolor=color)
    ax1.grid(True, linestyle="--", alpha=0.5)

    ax2 = ax1.twinx()
    color = "tab:red"
    ax2.set_ylabel("P99 Tail Latency (ms)", color=color, fontsize=12)
    line2 = ax2.plot(batches, p99, color=color, marker="s", linestyle="--", linewidth=2.5, label="P99 Latency (ms)")
    ax2.tick_params(axis="y", labelcolor=color)

    plt.title("RQ1: Impact of Raft Write Batching on Throughput & Tail Latency", fontsize=13, fontweight="bold")
    fig.tight_layout()

    out_file = os.path.join(output_dir, "rq1_batching.png")
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"  Saved: {out_file}")

def plot_rq2(data, output_dir, plt):
    names = [d["config"]["experimentName"].replace("RQ2_", "") for d in data]
    throughput = [d["throughputOpsSec"] for d in data]
    p99 = [d["p99LatencyMs"] for d in data]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 5))

    colors = ["#2ecc71", "#f39c12", "#e74c3c"]
    ax1.bar(names, throughput, color=colors, width=0.5)
    ax1.set_ylabel("Throughput (ops/sec)", fontsize=11)
    ax1.set_title("Throughput Under Failure Scenarios", fontsize=12, fontweight="bold")
    ax1.grid(axis="y", linestyle="--", alpha=0.5)

    ax2.bar(names, p99, color=colors, width=0.5)
    ax2.set_ylabel("P99 Latency (ms)", fontsize=11)
    ax2.set_title("P99 Tail Latency Under Failure Scenarios", fontsize=12, fontweight="bold")
    ax2.grid(axis="y", linestyle="--", alpha=0.5)

    fig.suptitle("RQ2: Resilience, Failover Duration & Tail Latency Under Adverse Conditions", fontsize=13, fontweight="bold")
    fig.tight_layout()

    out_file = os.path.join(output_dir, "rq2_recovery.png")
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"  Saved: {out_file}")

def plot_rq3(data, output_dir, plt):
    names = [d["config"]["experimentName"].replace("RQ3_", "") for d in data]
    abort_rates = [d["abortRate"] * 100.0 for d in data]
    throughput = [d["throughputOpsSec"] for d in data]

    fig, ax1 = plt.subplots(figsize=(8, 5))

    color = "tab:purple"
    ax1.set_ylabel("Transaction Abort Rate (%)", color=color, fontsize=12)
    bars = ax1.bar(names, abort_rates, color=color, alpha=0.7, width=0.4, label="Abort Rate (%)")
    ax1.tick_params(axis="y", labelcolor=color)
    ax1.set_ylim(0, 100)
    ax1.grid(axis="y", linestyle="--", alpha=0.5)

    ax2 = ax1.twinx()
    color = "tab:green"
    ax2.set_ylabel("Throughput (tx/sec)", color=color, fontsize=12)
    line = ax2.plot(names, throughput, color=color, marker="D", linewidth=2.5, label="Throughput (tx/sec)")
    ax2.tick_params(axis="y", labelcolor=color)

    plt.title("RQ3: MVCC Contention Level vs Abort Rate and Throughput", fontsize=13, fontweight="bold")
    fig.tight_layout()

    out_file = os.path.join(output_dir, "rq3_contention.png")
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"  Saved: {out_file}")

if __name__ == "__main__":
    main()
