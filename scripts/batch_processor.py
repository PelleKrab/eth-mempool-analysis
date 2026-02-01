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
        self.http_mode = False  # Will be set by _init_clickhouse
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

        # Parse URL to extract host, port, and protocol
        url = ch_config['url']
        use_https = url.startswith('https://')

        # Remove protocol prefix
        url_clean = url.replace('https://', '').replace('http://', '')

        # Extract host and port
        if ':' in url_clean:
            host, port_str = url_clean.split(':')
            port = int(port_str)
        else:
            host = url_clean
            # Use HTTP port by default for HTTPS URLs
            port = 8123 if use_https else 9000

        # For HTTPS connections, we need to use the HTTP interface
        if use_https or port == 8123:
            # Use requests-based connection for HTTP/HTTPS
            import requests
            self.logger.info(f"Using HTTP(S) connection to {host}:{port}")

            # We'll need to use a different approach - execute queries via HTTP
            # Store connection params for HTTP queries
            self.http_mode = True
            self.http_url = ch_config['url']
            self.http_auth = (ch_config['user'], ch_config['password'])
            return None  # No native client for HTTP mode
        else:
            # Use native protocol
            self.http_mode = False
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
        import requests
        import io

        # Replace parameters in query
        for key, value in params.items():
            query = query.replace(f"{{{key}}}", str(value))

        self.logger.debug(f"Executing query:\n{query[:200]}...")

        if self.http_mode:
            # Execute via HTTP interface
            # Format: CSV with header for easy pandas parsing
            query_with_format = query + " FORMAT CSVWithNames"

            try:
                response = requests.post(
                    self.http_url,
                    auth=self.http_auth,
                    data=query_with_format.encode('utf-8'),
                    params={'database': self.config['clickhouse']['database']},
                    timeout=3600
                )

                if response.status_code != 200:
                    raise Exception(f"HTTP {response.status_code}: {response.text}")

                # Parse CSV response
                if len(response.content) == 0:
                    self.logger.warning("Query returned no results")
                    return pd.DataFrame()

                df = pd.read_csv(io.StringIO(response.text))
                return df

            except Exception as e:
                self.logger.error(f"Query failed: {e}")
                raise

        else:
            # Execute via native protocol
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

        # Step 1: Get blocks
        self.logger.info("  Fetching block data...")
        blocks_query = self._load_query("block_il_metrics")
        blocks_df = self._execute_query(blocks_query, {
            'start_block': start_block,
            'end_block': end_block
        })

        if blocks_df.empty:
            self.logger.warning("No blocks found in range")
            return pd.DataFrame()

        # Step 2: Calculate time range for mempool query
        window_start = self.config['analysis']['time_window_start_secs']
        window_end = self.config['analysis']['time_window_end_secs']

        # Add buffer to time range
        min_timestamp = blocks_df['block_timestamp'].min() + window_start - 2
        max_timestamp = blocks_df['block_timestamp'].max() + window_end + 2

        self.logger.info(f"  Fetching mempool data from {min_timestamp} to {max_timestamp}...")

        # Step 3: Get mempool transactions in time range
        mempool_query = self._load_query("mempool_time_range")
        mempool_df = self._execute_query(mempool_query, {
            'start_timestamp': int(min_timestamp),
            'end_timestamp': int(max_timestamp)
        })

        self.logger.info(f"  Got {len(blocks_df)} blocks and {len(mempool_df)} mempool txs")

        # Step 4: Join and analyze in pandas
        self.logger.info("  Analyzing IL metrics...")
        results = []

        for _, block in blocks_df.iterrows():
            block_ts = block['block_timestamp']
            base_fee = int(block['base_fee'])

            # Filter mempool txs in time window
            window_mask = (
                (mempool_df['seen_timestamp'] >= block_ts + window_start) &
                (mempool_df['seen_timestamp'] <= block_ts + window_end) &
                (mempool_df['max_fee'].astype('Int64') >= base_fee)
            )
            window_txs = mempool_df[window_mask]

            results.append({
                'block_number': block['block_number'],
                'block_timestamp': block_ts,
                'base_fee': str(base_fee),
                'included_tx_count': block['included_tx_count'],
                'gas_used': block['gas_used'],
                'gas_limit': block['gas_limit'],
                'time_window_tx_count': len(window_txs),
                'il_tx_count': len(window_txs),
                'il_size_bytes': len(window_txs) * 200,  # Estimate
                'avg_time_offset_secs': (window_txs['seen_timestamp'] - block_ts).mean() if len(window_txs) > 0 else None,
                'avg_priority_fee': window_txs['priority_fee'].astype(float).mean() if len(window_txs) > 0 else None,
                'median_priority_fee': window_txs['priority_fee'].astype(float).median() if len(window_txs) > 0 else None
            })

        df = pd.DataFrame(results)
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
        # Run specific query in batches
        start_block = processor.config['analysis']['start_block']
        end_block = processor.config['analysis']['end_block']
        batch_size = processor.config['analysis']['batch_size_blocks']

        total_blocks = end_block - start_block
        num_batches = (total_blocks + batch_size - 1) // batch_size

        processor.logger.info(f"Running {args.query} from block {start_block} to {end_block}")
        processor.logger.info(f"Batch size: {batch_size} blocks, Total batches: {num_batches}\n")

        with tqdm(total=num_batches, desc=f"Processing {args.query}") as pbar:
            for i in range(num_batches):
                batch_start = start_block + (i * batch_size)
                batch_end = min(batch_start + batch_size, end_block)

                if args.query == 'il_metrics':
                    processor.process_block_il_metrics(batch_start, batch_end)
                elif args.query == 'replacements':
                    processor.process_nonce_replacements(batch_start, batch_end)
                elif args.query == 'bandwidth':
                    processor.process_bandwidth_analysis(batch_start, batch_end)
                elif args.query == 'censorship':
                    processor.process_censorship_events(batch_start, batch_end)

                pbar.update(1)
    else:
        # Run full analysis
        processor.run_full_analysis()


if __name__ == "__main__":
    main()
