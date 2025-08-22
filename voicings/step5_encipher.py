import polars as pl
import os

from tqdm import tqdm

from voicings.core.feasible import is_feasible
from voicings.core.encipher import pack_notes, pl_add_digest, pl_add_pcid, unpack_notes


def encipher_chords():
    """
    Pack chords into compact representation.
    """


    pcid_df = pl.read_parquet("data/chords/final/most_popular_cls.parquet")
    pcid_df = pl_add_pcid(pcid_df, col='cls').select('frequency', 'duration', 'pcid')

    # change types
    pcid_df = pcid_df.with_columns(
        pl.col('pcid').cast(pl.Int16),
        pl.col('frequency').cast(pl.Int32),
        pl.col('duration').cast(pl.Float32),
    )
    pcid_df.write_parquet("data/chords/export/most_popular_cls_packed.parquet")
    pcid_df.write_csv("data/chords/export/most_popular_cls_packed.csv")
    print(pcid_df)

    # exit(0)

    rel_df = pl.read_parquet("data/chords/final/most_popular_rel.parquet")
    rel_df = pl_add_pcid(rel_df, col='cls')
    rel_df = pl_add_digest(rel_df, col='rel').select('frequency', 'duration', 'pcid', 'digest')

    # change types
    rel_df = rel_df.with_columns(
        pl.col('digest').cast(pl.Utf8),
        pl.col('frequency').cast(pl.Int32),
        pl.col('duration').cast(pl.Float32),
        pl.col('pcid').cast(pl.Int16),
    ).filter(
        pl.col('digest').is_not_null() # & (pl.col('digest').str.len_chars() > 0)
    ) # .with_row_index("voicing_id")
    rel_df.write_parquet("data/chords/export/most_popular_rel_packed.parquet")
    # rel_df.write_csv("data/chords/export/most_popular_rel_packed.csv")
    print(rel_df)

    return pcid_df, rel_df



def desperate_measures(df):
    """
    I'm a little awed by how difficult it actually is to set up a database for a csv of only 175 MB.
    What if we just serve a lot of CSV files instead? lol
    """

    # df = pl.read_parquet("data/chords/export/most_popular_rel_packed.parquet")

    os.makedirs("data/chords/grouped", exist_ok=True)
    for subset in tqdm(df.partition_by("pcid")):
        pcid = subset['pcid'][0]

        # require >= 5 instances
        # if subset.height > 10000:
        subset = subset.filter(pl.col('frequency') >= 5)

        if subset.height > 10000:
            subset = subset.filter(pl.col('frequency') >= 10)
        

        # with feasibiltiy criterion: 178 MB -> 125 MB
        subset = subset.filter(
            pl.col('digest').map_elements(
                is_feasible,
                return_dtype=pl.Boolean
            )
        )
        subset.drop('pcid').write_csv(f"data/chords/grouped/{pcid}.csv")


if __name__ == "__main__":
    pcid_df, rel_df = encipher_chords()
    # voicings.pianodb.org/api/v1/raw
    desperate_measures(rel_df)