#!/usr/bin/env python3
"""
Analyze and visualize results from mempool batch processing
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse
import duckdb


class MempoolAnalyzer:
    """Analyze processed mempool data"""

    def __init__(self, results_dir: str = "../results"):
        self.results_dir = Path(results_dir)
        self.figures_dir = self.results_dir / "figures"
        self.figures_dir.mkdir(exist_ok=True)

        # Set style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (14, 8)

    def load_all_parquets(self, pattern: str) -> pd.DataFrame:
        """Load and concatenate all parquet files matching pattern"""
        files = list(self.results_dir.glob(f"{pattern}*.parquet"))

        if not files:
            raise FileNotFoundError(f"No files found matching: {pattern}")

        print(f"Loading {len(files)} files for {pattern}...")
        dfs = [pd.read_parquet(f) for f in files]
        df = pd.concat(dfs, ignore_index=True)

        # Sort by block number if available
        if 'block_number' in df.columns:
            df = df.sort_values('block_number').reset_index(drop=True)

        print(f"Loaded {len(df):,} rows")
        return df

    def analyze_il_metrics(self):
        """Analyze inclusion list metrics"""
        print("\n" + "="*60)
        print("Analyzing Inclusion List Metrics")
        print("="*60)

        df = self.load_all_parquets("block_il_metrics")

        # Summary statistics
        print("\nSummary Statistics:")
        print(df[['il_tx_count', 'il_size_bytes', 'time_window_tx_count']].describe())

        # Plot IL size over time
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. IL transaction count over time
        axes[0, 0].plot(df['block_number'], df['il_tx_count'], alpha=0.6, linewidth=0.5)
        axes[0, 0].set_xlabel('Block Number')
        axes[0, 0].set_ylabel('IL Transaction Count')
        axes[0, 0].set_title('Inclusion List Size Over Time')
        axes[0, 0].grid(True, alpha=0.3)

        # 2. IL size in KB
        axes[0, 1].plot(df['block_number'], df['il_size_bytes'] / 1024, alpha=0.6, linewidth=0.5)
        axes[0, 1].set_xlabel('Block Number')
        axes[0, 1].set_ylabel('IL Size (KB)')
        axes[0, 1].set_title('Inclusion List Bandwidth Over Time')
        axes[0, 1].grid(True, alpha=0.3)

        # 3. Average time offset
        axes[1, 0].plot(df['block_number'], df['avg_time_offset_secs'], alpha=0.6, linewidth=0.5)
        axes[1, 0].set_xlabel('Block Number')
        axes[1, 0].set_ylabel('Average Time Offset (seconds)')
        axes[1, 0].set_title('Average Transaction Time Offset from Block')
        axes[1, 0].axhline(y=0, color='r', linestyle='--', alpha=0.5)
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Distribution of IL sizes
        axes[1, 1].hist(df['il_tx_count'], bins=100, alpha=0.7, edgecolor='black')
        axes[1, 1].set_xlabel('IL Transaction Count')
        axes[1, 1].set_ylabel('Frequency')
        axes[1, 1].set_title('Distribution of Inclusion List Sizes')
        axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.figures_dir / 'il_metrics_analysis.png', dpi=300, bbox_inches='tight')
        print(f"\nSaved figure: {self.figures_dir / 'il_metrics_analysis.png'}")
        plt.close()

    def analyze_bandwidth_savings(self):
        """Analyze bandwidth savings (1-slot vs 2-slot)"""
        print("\n" + "="*60)
        print("Analyzing Bandwidth Savings")
        print("="*60)

        df = self.load_all_parquets("bandwidth_analysis")

        # Summary statistics
        print("\nBandwidth Savings Statistics:")
        print(df[['bandwidth_savings_bytes', 'bandwidth_savings_percent']].describe())

        # Calculate average savings
        avg_savings_pct = df['bandwidth_savings_percent'].mean()
        median_savings_pct = df['bandwidth_savings_percent'].median()

        print(f"\nAverage bandwidth savings: {avg_savings_pct:.2f}%")
        print(f"Median bandwidth savings: {median_savings_pct:.2f}%")

        # Plot
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Savings over time
        axes[0, 0].plot(df['block_number'], df['bandwidth_savings_percent'], alpha=0.6, linewidth=0.5)
        axes[0, 0].axhline(y=0, color='r', linestyle='--', alpha=0.5, label='Break-even')
        axes[0, 0].axhline(y=avg_savings_pct, color='g', linestyle='--', alpha=0.5, label=f'Average: {avg_savings_pct:.1f}%')
        axes[0, 0].set_xlabel('Block Number')
        axes[0, 0].set_ylabel('Bandwidth Savings (%)')
        axes[0, 0].set_title('1-Slot vs 2-Slot IL Bandwidth Savings')
        axes[0, 0].legend()
        axes[0, 0].grid(True, alpha=0.3)

        # 2. Distribution of savings
        axes[0, 1].hist(df['bandwidth_savings_percent'], bins=100, alpha=0.7, edgecolor='black')
        axes[0, 1].axvline(x=0, color='r', linestyle='--', alpha=0.5)
        axes[0, 1].axvline(x=avg_savings_pct, color='g', linestyle='--', alpha=0.5)
        axes[0, 1].set_xlabel('Bandwidth Savings (%)')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].set_title('Distribution of Bandwidth Savings')
        axes[0, 1].grid(True, alpha=0.3)

        # 3. IL sizes comparison
        axes[1, 0].scatter(df['one_slot_size_bytes'] / 1024, df['two_slot_size_bytes'] / 1024,
                          alpha=0.3, s=1)
        max_size = max(df['one_slot_size_bytes'].max(), df['two_slot_size_bytes'].max()) / 1024
        axes[1, 0].plot([0, max_size], [0, max_size], 'r--', alpha=0.5, label='Equal size')
        axes[1, 0].set_xlabel('1-Slot IL Size (KB)')
        axes[1, 0].set_ylabel('2-Slot IL Size (KB)')
        axes[1, 0].set_title('IL Size Comparison')
        axes[1, 0].legend()
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Cumulative savings
        df['cumulative_savings_mb'] = df['bandwidth_savings_bytes'].cumsum() / (1024 * 1024)
        axes[1, 1].plot(df['block_number'], df['cumulative_savings_mb'], linewidth=1)
        axes[1, 1].set_xlabel('Block Number')
        axes[1, 1].set_ylabel('Cumulative Savings (MB)')
        axes[1, 1].set_title('Cumulative Bandwidth Savings')
        axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.figures_dir / 'bandwidth_savings.png', dpi=300, bbox_inches='tight')
        print(f"\nSaved figure: {self.figures_dir / 'bandwidth_savings.png'}")
        plt.close()

    def analyze_nonce_replacements(self):
        """Analyze transaction replacements"""
        print("\n" + "="*60)
        print("Analyzing Nonce Replacements")
        print("="*60)

        df = self.load_all_parquets("nonce_replacements")

        print(f"\nTotal replacements: {len(df):,}")
        print(f"Unique senders: {df['sender'].nunique():,}")

        # Statistics
        print("\nFee Multiplier Statistics:")
        print(df['fee_multiplier'].describe())

        print("\nReplacement Time Statistics (seconds):")
        print(df['replacement_time_secs'].describe())

        # Plot
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))

        # 1. Fee multiplier distribution
        axes[0, 0].hist(df['fee_multiplier'].clip(upper=5), bins=100, alpha=0.7, edgecolor='black')
        axes[0, 0].set_xlabel('Fee Multiplier (capped at 5x)')
        axes[0, 0].set_ylabel('Frequency')
        axes[0, 0].set_title('Distribution of Fee Increases')
        axes[0, 0].grid(True, alpha=0.3)

        # 2. Replacement time distribution
        axes[0, 1].hist(df['replacement_time_secs'].clip(upper=300), bins=100, alpha=0.7, edgecolor='black')
        axes[0, 1].set_xlabel('Replacement Time (seconds, capped at 300s)')
        axes[0, 1].set_ylabel('Frequency')
        axes[0, 1].set_title('Time Between Replacements')
        axes[0, 1].grid(True, alpha=0.3)

        # 3. Replacement count distribution
        replacement_counts = df['replacement_count'].value_counts().sort_index()
        axes[1, 0].bar(replacement_counts.index, replacement_counts.values, alpha=0.7)
        axes[1, 0].set_xlabel('Number of Replacements')
        axes[1, 0].set_ylabel('Frequency')
        axes[1, 0].set_title('Distribution of Replacement Counts')
        axes[1, 0].grid(True, alpha=0.3)

        # 4. Fee multiplier vs replacement time
        sample_df = df.sample(min(10000, len(df)))  # Sample for scatter plot
        axes[1, 1].scatter(sample_df['replacement_time_secs'],
                          sample_df['fee_multiplier'],
                          alpha=0.3, s=1)
        axes[1, 1].set_xlabel('Replacement Time (seconds)')
        axes[1, 1].set_ylabel('Fee Multiplier')
        axes[1, 1].set_title('Fee Increase vs Replacement Time')
        axes[1, 1].set_xlim(0, 300)
        axes[1, 1].set_ylim(1, 5)
        axes[1, 1].grid(True, alpha=0.3)

        plt.tight_layout()
        plt.savefig(self.figures_dir / 'nonce_replacements.png', dpi=300, bbox_inches='tight')
        print(f"\nSaved figure: {self.figures_dir / 'nonce_replacements.png'}")
        plt.close()

    def generate_summary_report(self):
        """Generate a comprehensive summary report"""
        print("\n" + "="*60)
        print("Generating Summary Report")
        print("="*60)

        report = []
        report.append("# Ethereum Mempool Analysis Summary Report")
        report.append(f"\nGenerated: {pd.Timestamp.now()}\n")

        # IL Metrics
        try:
            df_il = self.load_all_parquets("block_il_metrics")
            report.append("## Inclusion List Metrics")
            report.append(f"- Total blocks analyzed: {len(df_il):,}")
            report.append(f"- Average IL size: {df_il['il_tx_count'].mean():.2f} transactions")
            report.append(f"- Average IL bandwidth: {df_il['il_size_bytes'].mean() / 1024:.2f} KB")
            report.append(f"- Average time offset: {df_il['avg_time_offset_secs'].mean():.2f} seconds\n")
        except Exception as e:
            report.append(f"## Inclusion List Metrics\nError: {e}\n")

        # Bandwidth Savings
        try:
            df_bw = self.load_all_parquets("bandwidth_analysis")
            report.append("## Bandwidth Savings (1-slot vs 2-slot)")
            report.append(f"- Average savings: {df_bw['bandwidth_savings_percent'].mean():.2f}%")
            report.append(f"- Median savings: {df_bw['bandwidth_savings_percent'].median():.2f}%")
            total_savings_mb = df_bw['bandwidth_savings_bytes'].sum() / (1024 * 1024)
            report.append(f"- Total savings: {total_savings_mb:.2f} MB\n")
        except Exception as e:
            report.append(f"## Bandwidth Savings\nError: {e}\n")

        # Nonce Replacements
        try:
            df_repl = self.load_all_parquets("nonce_replacements")
            report.append("## Transaction Replacements")
            report.append(f"- Total replacements: {len(df_repl):,}")
            report.append(f"- Unique senders: {df_repl['sender'].nunique():,}")
            report.append(f"- Average fee increase: {df_repl['fee_multiplier'].mean():.2f}x")
            report.append(f"- Average replacement time: {df_repl['replacement_time_secs'].mean():.2f} seconds\n")
        except Exception as e:
            report.append(f"## Transaction Replacements\nError: {e}\n")

        # Save report
        report_text = "\n".join(report)
        report_path = self.results_dir / "summary_report.md"
        with open(report_path, 'w') as f:
            f.write(report_text)

        print(report_text)
        print(f"\nSaved report: {report_path}")


def main():
    parser = argparse.ArgumentParser(description="Analyze mempool processing results")
    parser.add_argument('--results-dir', default='../results', help="Results directory")
    parser.add_argument('--analysis', choices=['il', 'bandwidth', 'replacements', 'all'],
                        default='all', help="Which analysis to run")

    args = parser.parse_args()

    analyzer = MempoolAnalyzer(args.results_dir)

    if args.analysis in ['il', 'all']:
        analyzer.analyze_il_metrics()

    if args.analysis in ['bandwidth', 'all']:
        analyzer.analyze_bandwidth_savings()

    if args.analysis in ['replacements', 'all']:
        analyzer.analyze_nonce_replacements()

    if args.analysis == 'all':
        analyzer.generate_summary_report()

    print("\n✓ Analysis complete!")


if __name__ == "__main__":
    main()
