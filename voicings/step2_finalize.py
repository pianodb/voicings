import polars as pl

def collect_final_aggregation():
    # Collect all the data into one df

    collector = []

    fnames = (
        "data/chords/summary_tournament.parquet",
        "data/chords/frequent_refuse.parquet",
        "data/chords/cyclic-1/agg_step_0.parquet",
        "data/chords/cyclic-1/agg_step_1.parquet",
        "data/chords/cyclic-1/agg_step_2.parquet",
        "data/chords/cyclic-1/agg_step_3.parquet",
        "data/chords/cyclic-1/agg_step_4.parquet",
        "data/chords/cyclic-2/agg_step_0.parquet",
    )

    for fname in fnames:
        print(f"Reading {fname}...")
        df = pl.read_parquet(fname)
        collector.append(df)
    
    return pl.concat(collector, how='vertical_relaxed').group_by('notes').agg(
        pl.col('duration').sum().alias('duration'),
        pl.col('frequency').sum().alias('frequency')
    ).sort('duration', descending=True)

if __name__ == "__main__":
    df = collect_final_aggregation()
    print("Writing final aggregation to file...")
    df.write_parquet("data/chords/final/final_aggregation.parquet")
    print("Done.")