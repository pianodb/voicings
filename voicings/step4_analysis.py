
import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

from voicings.core.decipher import pretty_print_chords

def group_by_cls():
    """
    Group DataFrame by 'cls', considering only those with >=3 unique pitch classes.
    """
    df = pl.scan_parquet("data/chords/final/final_aggregation_rel.parquet")
    df = df.filter(
        pl.col("cls").list.len() >= 3
    ).group_by("cls").agg(
        pl.col("frequency").sum().alias("frequency"),
        pl.col("duration").sum().alias("duration")
    ).sort("frequency", descending=True).collect()
    print("Grouped by cls with >=3 unique pitch classes")
    df = pretty_print_chords(df, col="cls", octave=False)
    df.write_parquet("data/chords/final/most_popular_cls.parquet")

def group_by_rel():
    """
    Group DataFrame by 'rel', considering only those with >=3 unique pitch classes.
    """
    df = pl.scan_parquet("data/chords/final/final_aggregation_rel.parquet")
    df = df.filter(
        pl.col("cls").list.len() >= 3
    ).group_by("rel").agg(
        pl.col("frequency").sum().alias("frequency"),
        pl.col("duration").sum().alias("duration"),
        pl.col("cls").first().alias("cls")
    ).sort("frequency", descending=True).collect()
    print("Grouped by rel with >=3 unique pitch classes")
    # df = pretty_print_chords(df, col="rel", octave=False)
    df.write_parquet("data/chords/final/most_popular_rel.parquet")

def hard_analysis():
    """
    The meat of the analysis.
    """

    df = pl.read_parquet("data/chords/final/final_aggregation_rel.parquet")
    df = df.select("notes_str", "rel", "cls", "frequency", "duration")

    # Get the 20 most common cls values
    top_cls = (df
               .group_by("cls")
               .agg(pl.col("frequency").sum().alias("total_frequency"))
               .sort("total_frequency", descending=True)
               .head(20))
    
    # Convert to pandas for seaborn
    top_cls_pd = top_cls.to_pandas()
    
    # Create histogram
    plt.figure(figsize=(12, 6))
    sns.barplot(data=top_cls_pd, x="cls", y="total_frequency")
    plt.title("20 Most Common Chord Classes (cls)")
    plt.xlabel("Chord Class")
    plt.ylabel("Total Frequency")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    # write to data/figures
    plt.savefig("data/figures/top_20_chord_classes.png")
    
    print("Top 20 chord classes:")
    print(top_cls)

if __name__ == "__main__":
    # hard_analysis()
    # group_by_cls()
    group_by_rel()