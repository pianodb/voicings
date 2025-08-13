import os

import polars as pl
from tqdm import tqdm

from voicings.core.feasible import is_feasible

def desperate_measures():
    """
    I'm a little awed by how difficult it actually is to set up a database for a csv of only 175 MB.
    What if we just serve a lot of CSV files instead? lol
    """

    df = pl.read_parquet("data/chords/export/most_popular_rel_packed.parquet")

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
    # voicings.pianodb.org/api/v1/raw
    desperate_measures()

