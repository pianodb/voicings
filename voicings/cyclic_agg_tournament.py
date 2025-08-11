# General Idea:

# We have a dataframe (~600M rows) too big to perform a groupby
# The data also likely has a lot of unique values

# Generally, we want to separate unique values from non-unique values, because unique values
# are clogging up the groupby and making it slow

# Heuristic: 
# 1. Split dataframe into chunks of k
# 2. For each chunk, find non-unique values
# 3. Filter original dataframe and extract out the non-unique values
# 4. There are usually not many non-unique values, so we can groupby on them
# 5. So now we have many values that are possibly unique, but hopefully less than in (1)
# 6. Shuffle (so that we get fresh batch of values)
# 7. We may now repeat 1-6.


# See also:
# - iterative heavy-hitter extraction
#    - In streaming algorithms, the idea is to find 
#    - the “frequent items” in sublinear memory before processing the whole dataset.
#    - Exactly right!
# - multi-pass aggregation
# - sampling + refinement

# - “peeling” in streaming graph algorithms
# - progressive filtering
# - multi-pass distinct elimination


import os
import polars as pl
from tqdm import tqdm

from voicings.chord_tournament import aggregate_df, prune_df



def cyclic_agg_1(df: pl.DataFrame, k: int = 30_000_000, prune_max_freq: int = 1):
    """
    1. Split the dataframe into chunks of k
    """
    # Split the dataframe into chunks of k
    chunks = list(df.iter_slices(k))
    print(f"Starting with {df.height} rows")
    del df

    print(f"Starting with {len(chunks)} chunks")

    # Now apply tournament pruning

    return chunks
    # chunks = [
    #     prune_df(aggregate_df(chunk), min_freq=prune_max_freq)[1]
    #     for chunk in chunks
    # ]
    # return chunks

def cyclic_agg_2(chunks: list[pl.DataFrame]):
    """
    2. For each chunk, find non-unique values
    """
    collector = []
    for chunk in tqdm(chunks, desc="Finding non-unique values"):
        # Find non-unique values
        notes = chunk.select('notes')
        non_unique = notes.filter(
            notes.is_duplicated()
        ).unique()
        collector.append(non_unique)
    return pl.concat(collector, how='vertical').unique()

def cyclic_agg_3(chunks: list[pl.DataFrame], known_duplicates: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    3. Filter original dataframe and extract out the non-unique values
    """
    # Filter original dataframe and extract out the non-unique values
    dup_collector = []
    uni_collector = []
    for chunk in tqdm(chunks, desc="Siphoning dataframe into duplicate/nonduplicate"):
        chunk = chunk.with_row_index('row_index')
        duplicate_out = chunk.join(
            known_duplicates,
            on='notes',
            how='semi'
        )
        possibly_unique_out = chunk.join(
            duplicate_out,
            on='row_index',
            how='anti'
        )
        dup_collector.append(duplicate_out.drop('row_index'))
        uni_collector.append(possibly_unique_out.drop('row_index'))
    return (
        pl.concat(dup_collector, how='vertical'),
        pl.concat(uni_collector, how='vertical')
    )

def cyclic_agg_4(df: pl.DataFrame):
    """
    4. Group by non-unique values and aggregate
    """
    # Group by non-unique values and aggregate
    return df.group_by('notes').agg(
        pl.col('duration').sum().alias('duration'),
        pl.col('frequency').sum().alias('frequency')
    ).sort('duration', descending=True)

def cyclic_agg_tournament(
        df: pl.DataFrame, 
        k: int = 30_000_000, 
        prune_max_freq: int = 1, 
        max_iterations=5, 
        prefix='data/chords/cyclic'
    ):
    """
    Perform a cyclic aggregation tournament on the dataframe.
    """
    print("Starting cyclic aggregation tournament")
    os.makedirs(prefix, exist_ok=True)
    ctr = 0

    remainder_out = df
    pre_height = remainder_out.height
    while pre_height > 0 and ctr < max_iterations:
        print("Iteration", ctr)
        chunks = cyclic_agg_1(remainder_out, k, prune_max_freq)
        print("Chunking done.")
        non_unique = cyclic_agg_2(chunks)
        print("Duplicate finding done.")
        step_out, remainder_out = cyclic_agg_3(chunks, non_unique)
        print("Dataframe siphoned into duplicate/nonduplicate.")
        step_agg = cyclic_agg_4(step_out)
        step_agg.write_parquet(f"{prefix}/agg_step_{ctr}.parquet")
        print("Written to file.")

        # Now we need to shuffle remainder_out to avoid bias in the next iteration
        remainder_out = remainder_out.sample(fraction=1, shuffle=True, seed=42)
        print("Done shuffling.")
        if pre_height <= k:
            # that means that we are done
            print("We processed the entire data in one batch; stopping.")
            break

        # Update counter
        ctr += 1
        pre_height = remainder_out.height


    remainder_out.write_parquet(f"{prefix}/remainder_{ctr}.parquet")

if __name__ == "__main__":

    # Phase 1: filter
    # sanity check: make sure that infrequent_refuse all has frequency 1
    df = pl.read_parquet(f"data/chords/infrequent_refuse.parquet")

    exceptions = df.filter(
        pl.col('frequency') > 1
    )
    if exceptions.height > 0:
        print("There are exceptions in infrequent_refuse:")
        print(exceptions)
        exit(0)

    # # begin cyclic aggregation tournament
    cyclic_agg_tournament(
        df,
        k=30_000_000,
        prune_max_freq=1,
        max_iterations=5,
        prefix='data/chords/cyclic-1'
    )


    # Phase 2: produce the 
    df = pl.read_parquet(f"data/chords/cyclic-1/remainder_5.parquet")

    cyclic_agg_tournament(
        df,
        k=130_000_000,
        prune_max_freq=1,
        max_iterations=5,
        prefix='data/chords/cyclic-2'
    )

