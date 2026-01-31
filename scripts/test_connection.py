#!/usr/bin/env python3
"""
Test ClickHouse connection and basic queries
"""

import sys
from pathlib import Path
import yaml
from clickhouse_driver import Client


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

    # Parse URL
    url = ch_config['url'].replace('https://', '').replace('http://', '')
    if ':' in url:
        host, port = url.split(':')
        port = int(port)
    else:
        host = url
        port = 9000

    try:
        client = Client(
            host=host,
            port=port,
            user=ch_config['user'],
            password=ch_config['password'],
            database=ch_config['database']
        )

        print("✓ Connected successfully!\n")

        # Test query: Get block range
        print("Checking canonical_beacon_block table...")
        result = client.execute("""
            SELECT
                min(block_number) as min_block,
                max(block_number) as max_block,
                count(*) as total_blocks
            FROM canonical_beacon_block
        """)

        if result:
            min_block, max_block, total_blocks = result[0]
            print(f"  Min block: {min_block:,}")
            print(f"  Max block: {max_block:,}")
            print(f"  Total blocks: {total_blocks:,}\n")

        # Test query: Get mempool tx count
        print("Checking mempool_transaction table...")
        result = client.execute("""
            SELECT
                count(*) as total_txs,
                min(seen_timestamp) as earliest,
                max(seen_timestamp) as latest
            FROM mempool_transaction
            LIMIT 1
        """)

        if result:
            total_txs, earliest, latest = result[0]
            print(f"  Total mempool txs: {total_txs:,}")
            print(f"  Earliest: {earliest}")
            print(f"  Latest: {latest}\n")

        # Test query: Sample IL metrics
        print("Testing IL metrics query (1000 blocks)...")
        test_start = max_block - 1000
        result = client.execute(f"""
            SELECT
                count(*) as blocks_processed,
                avg(length(transactions)) as avg_txs_per_block
            FROM canonical_beacon_block
            WHERE block_number >= {test_start} AND block_number < {max_block}
        """)

        if result:
            blocks_processed, avg_txs = result[0]
            print(f"  Blocks processed: {blocks_processed:,}")
            print(f"  Avg txs/block: {avg_txs:.2f}\n")

        print("="*60)
        print("✓ All tests passed!")
        print("="*60)
        print("\nYou're ready to run the batch processor:")
        print("  python batch_processor.py --start-block", test_start, "--end-block", max_block, "--batch-size 1000")

    except Exception as e:
        print(f"\n✗ Connection failed: {e}")
        print("\nTroubleshooting:")
        print("  1. Check your config/config.yaml credentials")
        print("  2. Verify ClickHouse server is accessible")
        print("  3. Check if you need to use HTTP protocol (port 8123) vs native (port 9000)")
        sys.exit(1)


if __name__ == "__main__":
    test_connection()
