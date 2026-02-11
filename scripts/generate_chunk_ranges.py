#!/usr/bin/env python3
"""
Generate Chunk Ranges for Manual Distribution

Creates a list of block ranges that can be run independently on different
machines or at different times. Outputs a CSV file you can use to track
progress.

Usage:
    python generate_chunk_ranges.py 19272000 21900000 10000
"""

import argparse
import csv
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description="Generate chunk ranges for distributed processing")
    parser.add_argument("start_block", type=int, help="Starting block number")
    parser.add_argument("end_block", type=int, help="Ending block number")
    parser.add_argument("chunk_size", type=int, help="Blocks per chunk")
    parser.add_argument("--output", type=str, default="chunk_ranges.csv",
                       help="Output CSV file")

    args = parser.parse_args()

    total_blocks = args.end_block - args.start_block
    num_chunks = (total_blocks + args.chunk_size - 1) // args.chunk_size

    print(f"Generating {num_chunks:,} chunk ranges...")
    print(f"Block range: {args.start_block:,} to {args.end_block:,}")
    print(f"Chunk size: {args.chunk_size:,} blocks")

    # Generate ranges
    ranges = []
    for chunk_id, chunk_start in enumerate(range(args.start_block, args.end_block, args.chunk_size)):
        chunk_end = min(chunk_start + args.chunk_size, args.end_block)
        ranges.append({
            'chunk_id': chunk_id,
            'start_block': chunk_start,
            'end_block': chunk_end,
            'num_blocks': chunk_end - chunk_start,
            'status': 'pending',
            'output_file': f'chunk_{chunk_id:04d}_{chunk_start}_{chunk_end}.parquet'
        })

    # Save to CSV
    output_file = Path(__file__).parent.parent / args.output
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['chunk_id', 'start_block', 'end_block', 'num_blocks', 'status', 'output_file'])
        writer.writeheader()
        writer.writerows(ranges)

    print(f"\nSaved {len(ranges):,} chunks to: {output_file}")
    print("\nTo run a specific chunk:")
    print(f"  python scripts/focil_censorship_analysis.py --start <start_block> --end <end_block>")
    print("\nOr use batch_runner.py:")
    print(f"  python scripts/batch_runner.py {args.start_block} {args.end_block} --chunk-size {args.chunk_size}")


if __name__ == "__main__":
    main()
