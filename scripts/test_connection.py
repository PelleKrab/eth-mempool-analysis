#!/usr/bin/env python3
"""
Test ClickHouse connection and basic queries
"""

import sys
from pathlib import Path
import yaml


def test_connection():
    """Test basic ClickHouse connection"""

    # Load config
    config_file = Path(__file__).parent.parent / "config" / "config.yaml"
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    ch_config = config['clickhouse']

    print("Testing ClickHouse connection...")
    print(f"  URL: {ch_config['url']}")
    print(f"  User: {ch_config['user']}")
    print(f"  Database: {ch_config['database']}\n")

    # Check if using HTTP(S)
    url = ch_config['url']
    use_https = url.startswith('https://') or url.startswith('http://')

    try:
        if use_https:
            # Use HTTP interface
            import requests
            print("Using HTTP(S) connection...\n")

            # Test with a simple query
            response = requests.post(
                url,
                auth=(ch_config['user'], ch_config['password']),
                data="SELECT 1 FORMAT CSV".encode('utf-8'),
                params={'database': ch_config['database']},
                timeout=10
            )

            if response.status_code != 200:
                raise Exception(f"HTTP {response.status_code}: {response.text}")

            print("✓ Connected successfully!\n")

            # Define query execution function for HTTP
            def execute_query(query):
                response = requests.post(
                    url,
                    auth=(ch_config['user'], ch_config['password']),
                    data=(query + " FORMAT CSV").encode('utf-8'),
                    params={'database': ch_config['database']},
                    timeout=30
                )
                if response.status_code != 200:
                    raise Exception(f"HTTP {response.status_code}: {response.text}")
                return [line.split(',') for line in response.text.strip().split('\n')]

        else:
            # Use native protocol
            from clickhouse_driver import Client

            url_clean = url.replace('https://', '').replace('http://', '')
            if ':' in url_clean:
                host, port = url_clean.split(':')
                port = int(port)
            else:
                host = url_clean
                port = 9000

            client = Client(
                host=host,
                port=port,
                user=ch_config['user'],
                password=ch_config['password'],
                database=ch_config['database']
            )

            print("✓ Connected successfully!\n")

            # Define query execution function for native protocol
            def execute_query(query):
                return client.execute(query)

        # Test query: Get block range
        print("Checking canonical_beacon_block table...")
        result = execute_query("""
            SELECT
                min(execution_payload_block_number) as min_block,
                max(execution_payload_block_number) as max_block,
                count(*) as total_blocks
            FROM canonical_beacon_block
        """)

        if result:
            row = result[0]
            min_block, max_block, total_blocks = int(row[0]), int(row[1]), int(row[2])
            print(f"  Min block: {min_block:,}")
            print(f"  Max block: {max_block:,}")
            print(f"  Total blocks: {total_blocks:,}\n")

        # Test query: Get mempool tx count
        print("Checking mempool_transaction table...")
        result = execute_query("""
            SELECT
                count(*) as total_txs,
                toString(min(event_date_time)) as earliest,
                toString(max(event_date_time)) as latest
            FROM mempool_transaction
        """)

        if result:
            row = result[0]
            total_txs, earliest, latest = int(row[0]), row[1], row[2]
            print(f"  Total mempool txs: {total_txs:,}")
            print(f"  Earliest: {earliest}")
            print(f"  Latest: {latest}\n")

        # Test query: Sample IL metrics
        print("Testing IL metrics query (1000 blocks)...")
        test_start = max_block - 1000
        result = execute_query(f"""
            SELECT
                count(*) as blocks_processed,
                avg(execution_payload_transactions_count) as avg_txs_per_block
            FROM canonical_beacon_block
            WHERE execution_payload_block_number >= {test_start} AND execution_payload_block_number < {max_block}
        """)

        if result:
            row = result[0]
            blocks_processed, avg_txs = int(row[0]), float(row[1])
            print(f"  Blocks processed: {blocks_processed:,}")
            print(f"  Avg txs/block: {avg_txs:.2f}\n")

        print("="*60)
        print("✓ All tests passed!")
        print("="*60)
        print("\nYou're ready to run the batch processor:")
        print(f"  python batch_processor.py --start-block {test_start} --end-block {max_block} --batch-size 1000")

    except Exception as e:
        print(f"\n✗ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Check your config/config.yaml credentials")
        print("  2. Verify ClickHouse server is accessible")
        print("  3. Check if you need to use HTTP protocol (port 8123) vs native (port 9000)")
        sys.exit(1)


if __name__ == "__main__":
    test_connection()
