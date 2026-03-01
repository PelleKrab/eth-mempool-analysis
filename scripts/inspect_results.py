import os
import re
import pandas as pd
import glob

results_dir = '/home/router/eth-mempool-analysis/results/bn_chunks'

# Get all parquet files
files = sorted(glob.glob(os.path.join(results_dir, '*.parquet')))

if not files:
    print("No parquet files found.")
    exit()

print(f"Found {len(files)} parquet files.")

# Parse filenames to get ranges
# Expected format: bn_chunk_XXXX_START_END.parquet
pattern = re.compile(r'chunk_(\d+)_(\d+)_(\d+)\.parquet')

chunk_ids = []
start_blocks = []
end_blocks = []

for f in files:
    match = pattern.search(os.path.basename(f))
    if match:
        chunk_ids.append(int(match.group(1)))
        start_blocks.append(int(match.group(2)))
        end_blocks.append(int(match.group(3)))

if start_blocks:
    min_start = min(start_blocks)
    max_end = max(end_blocks)
    min_chunk = min(chunk_ids)
    max_chunk = max(chunk_ids)
    
    print(f"Block Range Covered by Successful Results: {min_start} to {max_end}")
    print(f"Chunk ID Range: {min_chunk} to {max_chunk}")
    
    # Check for missing chunks
    expected_chunks = set(range(min_chunk, max_chunk + 1))
    found_chunks = set(chunk_ids)
    missing_chunks = sorted(list(expected_chunks - found_chunks))
    
    if missing_chunks:
        print(f"Found {len(missing_chunks)} missing chunks within the range.")
        if len(missing_chunks) < 20:
            print(f"Missing Chunk IDs: {missing_chunks}")
        else:
            print(f"Missing Chunk IDs (first 10): {missing_chunks[:10]} ...")
    else:
        print("No missing chunks within the range.")
        
else:
    print("Could not parse block ranges from filenames.")

# Inspect the first file
first_file = files[0]
try:
    df = pd.read_parquet(first_file)
    print(f"\n--- Inspection of {os.path.basename(first_file)} ---")
    print(f"Shape: {df.shape}")
    print("Columns:")
    print(df.columns.tolist())
    print("First 3 rows:")
    print(df.head(3))
    
    if 'block_number' in df.columns:
        print(f"\nBlock number range in file: {df['block_number'].min()} - {df['block_number'].max()}")

except Exception as e:
    print(f"Error reading file {first_file}: {e}")
