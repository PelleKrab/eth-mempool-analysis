#!/usr/bin/env python3
"""
Ethereum Mempool Batch Processor
Processes historical mempool data in chunks using ClickHouse SQL
"""

import os
import sys
import yaml
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional
from clickhouse_driver import Client
from tqdm import tqdm


class MempoolBatchProcessor:
    """Process Ethereum mempool data in batches"""

    def __init__(self, config_path: str = "../config/config.yaml"):
        """Initialize the processor with configuration"""
        self.config = self._load_config(config_path)
        self._setup_logging()
        self.client = self._init_clickhouse()
        self.queries_dir = Path(__file__).parent.parent / "queries"
        self.results_dir = Path(self.config['output']['results_dir'])
        self.results_dir.mkdir(parents=True, exist_ok=True)

    def _load_config(self, config_path: str) -> Dict:
        """Load configuration from YAML file"""
        config_file = Path(__file__).parent.parent / "config" / "config.yaml"
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)

    def _setup_logging(self):
        """Setup logging configuration"""
        log_level = self.config['logging']['level']
        log_file = Path(self.config['logging']['file'])
        log_file.parent.mkdir(parents=True, exist_ok=True)

        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _init_clickhouse(self) -> Client:
        """Initialize ClickHouse client"""
        ch_config = self.config['clickhouse']
        self.logger.info(f"Connecting to ClickHouse at {ch_config['url']}")

        # Parse URL to extract host and port
        url = ch_config['url'].replace('https://', '').replace('http://', '')
        if ':' in url:
            host, port = url.split(':')
            port = int(port)
        else:
            host = url
            port = 9000  # Native protocol port

        return Client(
            host=host,
            port=port,
            user=ch_config['user'],
            password=ch_config['password'],
            database=ch_config['database'],
            settings={'max_execution_time': 3600}  # 1 hour timeout
        )

    def _load_query(self, query_name: str) -> str:
        """Load SQL query from file"""
        query_file = self.queries_dir / f"{query_name}.sql"
        if not query_file.exists():
            raise FileNotFoundError(f"Query file not found: {query_file}")

        with open(query_file, 'r') as f:
            return f.read()

    def _execute_query(self, query: str, params: Dict) -> pd.DataFrame:
        """Execute ClickHouse query and return as DataFrame"""
        # Replace parameters in query
        for key, value in params.items():
            query = query.replace(f"{{{key}}}", str(value))

        self.logger.debug(f"Executing query:\n{query[:200]}...")

        # Execute and fetch results
        result = self.client.execute(query, with_column_types=True)
        data, columns = result

        if not data:
            self.logger.warning("Query returned no results")
            return pd.DataFrame()

        # Convert to DataFrame
        column_names = [col[0] for col in columns]
        df = pd.DataFrame(data, columns=column_names)

        return df

    def _save_results(self, df: pd.DataFrame, name: str, batch_id: Optional[str] = None):
        """Save results to Parquet/CSV"""
        if df.empty:
            self.logger.warning(f"No data to save for {name}")
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        suffix = f"_{batch_id}" if batch_id else ""
        filename = f"{name}{suffix}_{timestamp}"

        output_format = self.config['output']['format']

        if output_format in ['parquet', 'both']:
            parquet_path = self.results_dir / f"{filename}.parquet"
            df.to_parquet(
                parquet_path,
                compression=self.config['output']['compression'],
                index=False
            )
            self.logger.info(f"Saved Parquet: {parquet_path} ({len(df)} rows)")

        if output_format in ['csv', 'both']:
            csv_path = self.results_dir / f"{filename}.csv"
            df.to_csv(csv_path, index=False)
            self.logger.info(f"Saved CSV: {csv_path} ({len(df)} rows)")

    def process_block_il_metrics(self, start_block: int, end_block: int) -> pd.DataFrame:
        """Process inclusion list metrics for a block range"""
        self.logger.info(f"Processing IL metrics for blocks {start_block} to {end_block}")

        query = self._load_query("block_il_metrics")
        params = {
            'start_block': start_block,
            'end_block': end_block,
            'window_start_secs': self.config['analysis']['time_window_start_secs'],
            'window_end_secs': self.config['analysis']['time_window_end_secs']
        }

        df = self._execute_query(query, params)
        self._save_results(df, "block_il_metrics", f"{start_block}_{end_block}")

        return df

    def process_nonce_replacements(self, start_block: int, end_block: int) -> pd.DataFrame:
        """Process nonce replacements for a block range"""
        self.logger.info(f"Processing nonce replacements for blocks {start_block} to {end_block}")

        # Convert blocks to approximate timestamps (12s per block)
        start_timestamp = 1663224179 + (start_block - 15537394) * 12  # Merge timestamp
        end_timestamp = start_timestamp + (end_block - start_block) * 12

        query = self._load_query("nonce_replacements")
        params = {
            'start_timestamp': start_timestamp,
            'end_timestamp': end_timestamp
        }

        df = self._execute_query(query, params)
        self._save_results(df, "nonce_replacements", f"{start_block}_{end_block}")

        return df

    def process_bandwidth_analysis(self, start_block: int, end_block: int) -> pd.DataFrame:
        """Process bandwidth analysis for a block range"""
        self.logger.info(f"Processing bandwidth analysis for blocks {start_block} to {end_block}")

        query = self._load_query("bandwidth_analysis")
        params = {
            'start_block': start_block,
            'end_block': end_block,
            'window_start_secs': self.config['analysis']['time_window_start_secs'],
            'window_end_secs': self.config['analysis']['time_window_end_secs']
        }

        df = self._execute_query(query, params)
        self._save_results(df, "bandwidth_analysis", f"{start_block}_{end_block}")

        return df

    def process_censorship_events(self, start_block: int, end_block: int) -> pd.DataFrame:
        """Process censorship events for a block range"""
        self.logger.info(f"Processing censorship events for blocks {start_block} to {end_block}")

        query = self._load_query("censorship_events")
        params = {
            'start_block': start_block,
            'end_block': end_block,
            'min_pending_blocks': self.config['analysis']['min_pending_blocks'],
            'window_start_secs': self.config['analysis']['time_window_start_secs'],
            'window_end_secs': self.config['analysis']['time_window_end_secs']
        }

        df = self._execute_query(query, params)
        self._save_results(df, "censorship_events", f"{start_block}_{end_block}")

        return df

    def run_batch(self, start_block: int, end_block: int):
        """Run all analyses for a batch"""
        self.logger.info(f"\n{'='*60}")
        self.logger.info(f"Processing batch: {start_block} to {end_block}")
        self.logger.info(f"{'='*60}\n")

        try:
            # Run all queries for this batch
            self.process_block_il_metrics(start_block, end_block)
            self.process_nonce_replacements(start_block, end_block)
            self.process_bandwidth_analysis(start_block, end_block)
            # Censorship events can be slow, uncomment if needed
            # self.process_censorship_events(start_block, end_block)

            self.logger.info(f"✓ Batch {start_block}-{end_block} completed successfully\n")

        except Exception as e:
            self.logger.error(f"✗ Batch {start_block}-{end_block} failed: {e}\n")
            raise

    def run_full_analysis(self):
        """Run analysis on the full block range in batches"""
        start_block = self.config['analysis']['start_block']
        end_block = self.config['analysis']['end_block']
        batch_size = self.config['analysis']['batch_size_blocks']

        self.logger.info(f"Starting full analysis from block {start_block} to {end_block}")
        self.logger.info(f"Batch size: {batch_size} blocks")

        total_blocks = end_block - start_block
        num_batches = (total_blocks + batch_size - 1) // batch_size

        self.logger.info(f"Total batches: {num_batches}\n")

        # Process in batches with progress bar
        with tqdm(total=num_batches, desc="Processing batches") as pbar:
            for i in range(num_batches):
                batch_start = start_block + (i * batch_size)
                batch_end = min(batch_start + batch_size, end_block)

                self.run_batch(batch_start, batch_end)
                pbar.update(1)

        self.logger.info("\n" + "="*60)
        self.logger.info("✓ Full analysis completed!")
        self.logger.info("="*60)


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="Ethereum Mempool Batch Processor")
    parser.add_argument('--start-block', type=int, help="Start block number")
    parser.add_argument('--end-block', type=int, help="End block number")
    parser.add_argument('--batch-size', type=int, help="Batch size in blocks")
    parser.add_argument('--query', choices=['il_metrics', 'replacements', 'bandwidth', 'censorship'],
                        help="Run specific query only")

    args = parser.parse_args()

    processor = MempoolBatchProcessor()

    # Override config with CLI args if provided
    if args.start_block:
        processor.config['analysis']['start_block'] = args.start_block
    if args.end_block:
        processor.config['analysis']['end_block'] = args.end_block
    if args.batch_size:
        processor.config['analysis']['batch_size_blocks'] = args.batch_size

    if args.query:
        # Run specific query
        start = processor.config['analysis']['start_block']
        end = processor.config['analysis']['end_block']

        if args.query == 'il_metrics':
            processor.process_block_il_metrics(start, end)
        elif args.query == 'replacements':
            processor.process_nonce_replacements(start, end)
        elif args.query == 'bandwidth':
            processor.process_bandwidth_analysis(start, end)
        elif args.query == 'censorship':
            processor.process_censorship_events(start, end)
    else:
        # Run full analysis
        processor.run_full_analysis()


if __name__ == "__main__":
    main()
