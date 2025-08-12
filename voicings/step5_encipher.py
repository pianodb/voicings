import polars as pl

from voicings.core.encipher import pack_notes, pl_add_digest, pl_add_pcid, unpack_notes


if __name__ == "__main__":
    # Example usage
    original = [0, 31, 36, 40, 43, 100, 127]
    packed = pack_notes(original)
    print(packed)
    unpacked = unpack_notes(packed)
    print(unpacked)


    pcid_df = pl.read_parquet("data/chords/final/most_popular_cls.parquet")
    pcid_df = pl_add_pcid(pcid_df, col='cls').select('frequency', 'duration', 'pcid')

    # change types
    pcid_df = pcid_df.with_columns(
        pl.col('pcid').cast(pl.Int16),
        pl.col('frequency').cast(pl.Int32),
        pl.col('duration').cast(pl.Float32),
    )
    # pcid_df.write_parquet("data/chords/export/most_popular_cls_packed.parquet")
    pcid_df.write_csv("data/chords/export/most_popular_cls_packed.csv")
    print(pcid_df)

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
        pl.col('digest').is_not_null() & pl.col('digest').str.len_chars() > 0
    ) # .with_row_index("voicing_id")
    # rel_df.write_parquet("data/chords/export/most_popular_rel_packed.parquet")
    rel_df.write_csv("data/chords/export/most_popular_rel_packed.csv")
    print(rel_df)