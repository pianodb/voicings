import polars as pl

from voicings.core.classify import classify_chords, untranspose_chords
from voicings.core.decipher import pretty_print_chords

if __name__ == "__main__":
    # Load the final aggregation DataFrame
    # This is the output of the aggregation process
    # It should contain the final chord data we want to pretty print
    df = pl.read_parquet("data/chords/final/final_aggregation.parquet")

    df = classify_chords(df)
    df = pretty_print_chords(df)

    df.write_parquet("data/chords/final/final_aggregation_rel.parquet")

    # df = pl.read_parquet("data/chords/final/final_aggregation_rel.parquet")
    # df = df.with_columns(
    #     pl.col("cls").list.unique(maintain_order=True).list.sort().alias("cls"),
    # )
    # df.write_parquet("data/chords/final/final_aggregation_rel_unique.parquet")