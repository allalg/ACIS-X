import os
import shutil
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARTIFACT_DIR = r"C:\Users\nikhi\.gemini\antigravity-ide\brain\5f3d1383-505b-4b43-b9ca-b09171b9c69b"

def copy_monotonicity_chart():
    src = os.path.join(PROJECT_ROOT, "tests", "outputs", "monotonicity_proof.png")
    dst = os.path.join(ARTIFACT_DIR, "monotonicity_proof.png")
    if os.path.exists(src):
        shutil.copy2(src, dst)
        print(f"Copied monotonicity chart to {dst}")
    else:
        print(f"Warning: {src} not found!")

def generate_consumer_replica_scaling_chart():
    # Palette
    c_blue = "#3b82f6"
    c_indigo = "#6366f1"
    c_emerald = "#10b981"
    c_amber = "#f59e0b"
    bg_canvas = "#f8fafc"
    card_bg = "#ffffff"
    grid_color = "#e2e8f0"
    text_dark = "#0f172a"
    text_muted = "#64748b"

    fig, ax = plt.subplots(figsize=(10, 6.2), dpi=220, facecolor=bg_canvas)
    ax.set_facecolor(card_bg)
    for spine in ax.spines.values():
        spine.set_color(grid_color)
        spine.set_linewidth(1.2)

    # Replicas and throughput values from canonical benchmark
    replicas = ["1 Replica", "2 Replicas"]
    throughput_load_balanced = [16027.7, 33932.6]  # events/sec
    throughput_broadcast = [16027.7, 16027.7]      # broadcast would stay flat in unique events/sec

    x = np.arange(len(replicas))
    bar_width = 0.42

    # Draw bars for load-balanced consumer group scaling
    bars = ax.bar(
        x,
        throughput_load_balanced,
        width=bar_width,
        color=[c_blue, c_indigo],
        edgecolor=["#1d4ed8", "#4338ca"],
        linewidth=1.2,
        alpha=0.92,
        zorder=3,
        label="Load-Balanced (Shared Canonical group_id)"
    )

    # Add ghost/reference line showing broadcast / duplicate consumption ceiling
    ax.axhline(
        throughput_load_balanced[0],
        linestyle="--",
        color="#94a3b8",
        linewidth=1.4,
        zorder=2,
        label="Broadcast / Fan-out Ceiling (Unique group_ids, No Partition Split)"
    )

    # Value callouts on top of bars
    for i, bar in enumerate(bars):
        h = bar.get_height()
        if i == 0:
            label_text = f"{h:,.1f} eps\n(1.00× Baseline)"
        else:
            speedup = h / throughput_load_balanced[0]
            label_text = f"{h:,.1f} eps\n({speedup:.2f}× Scaling — 112% Boost)"
        
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            h + 900,
            label_text,
            ha="center",
            va="bottom",
            fontsize=10.5,
            fontweight="bold",
            color=text_dark
        )

    # Add upward speedup arrow between bars
    ax.annotate(
        "",
        xy=(1.0, 32000), xytext=(0.0, 18000),
        arrowprops=dict(arrowstyle="->", color=c_emerald, lw=2.2, mutation_scale=16, linestyle="-")
    )
    ax.text(
        0.5, 26000,
        "+17,904.9 events/s\nPartition Load-Balanced",
        ha="center", va="center",
        fontsize=9.8, fontweight="bold",
        color="#047857",
        bbox=dict(boxstyle="round,pad=0.4", facecolor="#ecfdf5", edgecolor="#a7f3d0", lw=1)
    )

    ax.set_xticks(x)
    ax.set_xticklabels(["1 Consumer Replica\n(N = 1)", "2 Consumer Replicas\n(N = 2)"], fontsize=11, fontweight="bold", color=text_dark)
    ax.set_ylabel("Events Processed per Second (events/s)", fontsize=11.5, fontweight="bold", color=text_dark, labelpad=10)
    ax.set_xlabel("Number of Consumer Replicas", fontsize=11.5, fontweight="bold", color=text_dark, labelpad=10)
    ax.set_ylim(0, 42000)

    ax.yaxis.grid(True, linestyle=":", alpha=0.7, color="#cbd5e1")
    ax.set_axisbelow(True)

    ax.set_title(
        "ACIS-X Consumer Replica Throughput Scaling\n"
        "Horizontal Partition Load-Balancing vs. Broadcast Semantics",
        fontsize=13.5,
        fontweight="bold",
        color=text_dark,
        pad=16
    )

    ax.legend(loc="upper left", frameon=True, framealpha=0.95, facecolor="#ffffff", edgecolor=grid_color, fontsize=9.5)

    # Explanatory subtitle banner at bottom
    fig.text(
        0.5, 0.02,
        "Verification: Sharing canonical group_id partitions Kafka topic across replicas. "
        "Throughput doubles from 16,028 to 33,933 eps rather than duplicating messages.",
        ha="center", fontsize=8.8, color=text_muted
    )

    plt.tight_layout(rect=[0, 0.05, 1, 0.96])

    out_paths = [
        os.path.join(PROJECT_ROOT, "tests", "outputs", "throughput_scaling_consumer_replicas.png"),
        os.path.join(PROJECT_ROOT, "tests", "outputs", "throughput_scaling_bar_chart.png"),
        os.path.join(ARTIFACT_DIR, "throughput_scaling_consumer_replicas.png"),
        os.path.join(ARTIFACT_DIR, "throughput_scaling.png")
    ]

    for p in out_paths:
        os.makedirs(os.path.dirname(p), exist_ok=True)
        fig.savefig(p, dpi=220, facecolor=bg_canvas)
        print(f"Saved throughput scaling bar chart to: {p}")

    plt.close(fig)

if __name__ == "__main__":
    copy_monotonicity_chart()
    generate_consumer_replica_scaling_chart()
