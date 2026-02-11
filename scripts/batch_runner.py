#!/usr/bin/env python3
"""
Batch Runner for FOCIL Analysis

Splits a large block range into manageable chunks and runs them sequentially
or in parallel. Each chunk saves to a separate parquet file.

Usage:
    python batch_runner.py 19272000 21900000 --chunk-size 10000
    python batch_runner.py 19272000 21900000 --chunk-size 10000 --parallel 4
"""

import argparse
import logging
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

# Add parent directory to path to import the main script
sys.path.insert(0, str(Path(__file__).parent))

from focil_censorship_analysis import analyze_block_range, load_config, print_summary
import pandas as pd

log = logging.getLogger(__name__)


def run_chunk(chunk_start: int, chunk_end: int, chunk_id: int, output_dir: Path):
    """Run analysis on a single chunk and save results."""
    try:
        log.info(f"[Chunk {chunk_id}] Processing blocks {chunk_start:,} to {chunk_end:,}")

        config = load_config()
        result_df = analyze_block_range(chunk_start, chunk_end, config)

        if result_df is None or len(result_df) == 0:
            log.warning(f"[Chunk {chunk_id}] No results produced")
            return None

        # Save chunk results
        output_file = output_dir / f"chunk_{chunk_id:04d}_{chunk_start}_{chunk_end}.parquet"
        result_df.to_parquet(output_file, index=False)

        log.info(f"[Chunk {chunk_id}] Saved {len(result_df):,} blocks to {output_file.name}")
        return output_file

    except Exception as e:
        log.error(f"[Chunk {chunk_id}] Failed: {e}", exc_info=True)
        return None


def main():
    parser = argparse.ArgumentParser(description="Batch process FOCIL analysis over large block ranges")
    parser.add_argument("start_block", type=int, help="Starting block number")
    parser.add_argument("end_block", type=int, help="Ending block number")
    parser.add_argument("--chunk-size", type=int, default=10000,
                       help="Blocks per chunk (default: 10000)")
    parser.add_argument("--parallel", type=int, default=1,
                       help="Number of parallel workers (default: 1 = sequential)")
    parser.add_argument("--output-dir", type=str, default="results/chunks",
                       help="Directory for chunk outputs")
    parser.add_argument("--resume", action="store_true",
                       help="Skip chunks that already have output files")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    # Validate inputs
    if args.end_block <= args.start_block:
        log.error("end_block must be greater than start_block")
        return 1

    total_blocks = args.end_block - args.start_block
    num_chunks = (total_blocks + args.chunk_size - 1) // args.chunk_size

    log.info("=" * 70)
    log.info("FOCIL BATCH RUNNER")
    log.info("=" * 70)
    log.info(f"Block range: {args.start_block:,} to {args.end_block:,}")
    log.info(f"Total blocks: {total_blocks:,}")
    log.info(f"Chunk size: {args.chunk_size:,}")
    log.info(f"Number of chunks: {num_chunks:,}")
    log.info(f"Parallel workers: {args.parallel}")
    log.info(f"Output directory: {args.output_dir}")

    # Create output directory
    output_dir = Path(__file__).parent.parent / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate chunk ranges
    chunks = []
    for chunk_id, chunk_start in enumerate(range(args.start_block, args.end_block, args.chunk_size)):
        chunk_end = min(chunk_start + args.chunk_size, args.end_block)

        # Check if chunk already exists (resume mode)
        output_file = output_dir / f"chunk_{chunk_id:04d}_{chunk_start}_{chunk_end}.parquet"
        if args.resume and output_file.exists():
            log.info(f"[Chunk {chunk_id}] Skipping (already exists): {output_file.name}")
            continue

        chunks.append((chunk_start, chunk_end, chunk_id, output_dir))

    if not chunks:
        log.info("All chunks already processed!")
        return 0

    log.info(f"Processing {len(chunks):,} chunks...")

    # Run chunks
    completed = 0
    failed = 0

    if args.parallel == 1:
        # Sequential processing
        for chunk_start, chunk_end, chunk_id, out_dir in chunks:
            result = run_chunk(chunk_start, chunk_end, chunk_id, out_dir)
            if result:
                completed += 1
            else:
                failed += 1
    else:
        # Parallel processing
        with ProcessPoolExecutor(max_workers=args.parallel) as executor:
            futures = {
                executor.submit(run_chunk, start, end, cid, out_dir): cid
                for start, end, cid, out_dir in chunks
            }

            for future in as_completed(futures):
                chunk_id = futures[future]
                try:
                    result = future.result()
                    if result:
                        completed += 1
                    else:
                        failed += 1
                except Exception as e:
                    log.error(f"[Chunk {chunk_id}] Exception: {e}")
                    failed += 1

    log.info("=" * 70)
    log.info("BATCH PROCESSING COMPLETE")
    log.info("=" * 70)
    log.info(f"Completed: {completed:,} chunks")
    log.info(f"Failed: {failed:,} chunks")
    log.info(f"Output directory: {output_dir}")
    log.info("")
    log.info("Next step: Combine results with combine_chunks.py")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
