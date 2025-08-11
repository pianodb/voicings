import os
import polars as pl
from tqdm import tqdm
import heapq
import time

def aggregate_df(df):
    if 'fname' in df.columns:
        return (
            df.group_by("notes")
            .agg(
                pl.col("duration").sum().alias("duration"),
                pl.col("fname").n_unique().alias("frequency")
            )
        )
    else:
        return (
            df.group_by("notes")
            .agg(
                pl.col("duration").sum().alias("duration"),
                pl.col("frequency").sum().alias("frequency")
            )
        )


def prune_df(df, min_freq=2, top_k=None):
    if min_freq:
        good = df.filter(pl.col("frequency") >= min_freq)
        bad = df.filter(pl.col("frequency") < min_freq)
        return good, bad
    # if top_k:
    #     df = df.sort("frequency", descending=True).head(top_k)
    # return df


def tournament_merge(input_dir, prune_min_freq=2, prune_top_k=None, *, chunks=None):
    start_time = time.time()
    
    if input_dir is not None:
        # Load all parquet chunk paths
        files = [os.path.join(input_dir, f) 
                for f in os.listdir(input_dir) if f.endswith(".parquet")]
        
        print(f"Found {len(files)} parquet files to process")
        
        # First pass: aggregate each file individually
        initial_start = time.time()
        chunks = []
        for path in tqdm(files, desc="Initial aggregation"):
            df = pl.read_parquet(path)
            agg = aggregate_df(df)
            good, bad = prune_df(agg, prune_min_freq, prune_top_k)
            chunks.append(good)
        
        initial_time = time.time() - initial_start
        print(f"Initial aggregation took: {initial_time:.2f} seconds")
        print(f"Created {len(chunks)} chunks for tournament")
    else:
        print(f"Using {len(chunks)} provided chunks for tournament")
    


    # Merge tournament-style
    tournament_start = time.time()
    round_num = 1
    while len(chunks) > 1:
        round_start = time.time()
        new_chunks = []
        for i in range(0, len(chunks), 2):
            if i + 1 < len(chunks):
                merged = pl.concat([chunks[i], chunks[i+1]], how="vertical")
                merged = aggregate_df(merged)
                # merged = prune_df(merged, None, prune_top_k)
                new_chunks.append(merged)
            else:
                new_chunks.append(chunks[i])  # carry forward odd chunk
        chunks = new_chunks
        round_time = time.time() - round_start
        print(f"Round {round_num}: {len(chunks)} tables remaining (took {round_time:.2f}s)")
        round_num += 1
    
    tournament_time = time.time() - tournament_start
    print(f"Tournament merge took: {tournament_time:.2f} seconds")

    final = chunks[0].sort("duration", descending=True)
    
    total_time = time.time() - start_time
    print(f"Total tournament time: {total_time:.2f} seconds")
    
    return final


def dig_through_refuse_for_misses(input_dir, good_df: pl.DataFrame, prune_min_freq=2):
    refuse = []
    all_misses = []
    files = [os.path.join(input_dir, f) 
             for f in os.listdir(input_dir) if f.endswith(".parquet")]
    
    for path in tqdm(files, desc="Processing refuse (uncommon fragments)"):
        df = pl.read_parquet(path)
        agg = aggregate_df(df)
        good, bad = prune_df(agg, prune_min_freq, None)
        misses = bad.join(
            good_df,
            on='notes',
            how='semi'
        )
        very_bad = bad.join(
            good_df,
            on='notes',
            how='anti'
        )
        refuse.append(very_bad)
        all_misses.append(misses)
    
    print("Combining refuse fragments...")
    frequent_refuse = pl.concat(refuse, how="vertical")
    print("Grouping refuse fragments...")
    frequent_refuse = frequent_refuse.group_by("notes").agg(
        pl.col("duration").sum().alias("duration"),
        pl.col("frequency").sum().alias("frequency")
    )
    print("Sorting refuse fragments...")
    frequent_refuse = frequent_refuse.sort("duration", descending=True)
    print("Writing refuse fragments to file...")
    frequent_refuse.write_parquet("data/chords/frequent_refuse.parquet")

    infrequent_refuse = pl.concat(all_misses, how="vertical")
    print("Writing infrequent refuse fragments to file...")
    infrequent_refuse.write_parquet("data/chords/infrequent_refuse.parquet")
    print("Done")


def main_tournament_step():
    
    print("Starting tournament merge...")
    tournament_start = time.time()
    
    final_df = tournament_merge("data/fragments", prune_min_freq=2, prune_top_k=None)
    print("Tournament done.")
    
    write_start = time.time()
    final_df.write_parquet("data/chords/summary_tournament.parquet")
    write_time = time.time() - write_start
    
    print(f"File write took: {write_time:.2f} seconds")
    tournament_time = time.time() - tournament_start
    print(f"Tournament merge took: {tournament_time:.2f} seconds")


def main_refuse_step():
    print("Trying to read files...")
    best = pl.read_parquet("data/chords/summary_tournament.parquet")
    # Load frequent people from a file or define them

    print("File read complete.")
    dedup_start = time.time()
    dig_through_refuse_for_misses(
        "data/fragments",
        best
    )
    dedup_time = time.time() - dedup_start
    print(f"Deduplication took: {dedup_time:.2f} seconds")
    # begin: 4:17 4:46: down to 36; 4:49 (18) 


if __name__ == "__main__":

    overall_start = time.time()

    # produces data/chords/summary_tournament.parquet
    main_tournament_step()

    # produces data/chords/frequent_refuse.parquet
    # produces data/chords/infrequent_refuse.parquet
    main_refuse_step()

    # naively trying to aggregate infrequent_refuse is too slow -- even with duckdb!
    # NOTE: for a smart way to aggregate 
    # when we have a large number of unique values
    # see cyclic_agg_tournament.py

    overall_time = time.time() - overall_start
    print(f"Overall execution time: {overall_time:.2f} seconds")
