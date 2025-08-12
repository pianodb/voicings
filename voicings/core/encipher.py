import base64

from tqdm import tqdm

import polars as pl

from voicings.core.pl_tqdm import w_pbar

_base64_alphabet = "1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz-._~"
"""
base64 is modified so that it starts with 1-9, then 0, then A-Z, a-z. 
Then the special characters -._~ which are URL safe.
This is 66 characters long, allows a voicing to have a gap of up to 5.5 octaves.
"""

def pack_notes(notes):
    """
    Pack notes into base64 based on diff
    """
    result = ""
    if not notes:
        return result

    prev = notes[0]
    for note in notes[1:]:
        diff = note - prev
        if diff <= 0:
            raise ValueError("Notes must be in ascending order")
        if diff > len(_base64_alphabet):
            # invalid
            return None
        # here, valid diff is in range 1 to len(_base64)
        # need to subtract one
        result += _base64_alphabet[diff - 1]
        prev = note
    return result

def unpack_notes(inp: str):
    """
    
    """
    cur = 0
    result = [0]
    for c in inp:
        diff = _base64_alphabet.index(c) - 1
        cur += diff
        result.append(cur)
    return result

def pl_add_digest(df, col='rel', out="digest"):
    """
    Add column 'digest', which is a string representation of the packed pitches in 'rel'.
    Uses slightly modified base64. Balance between readable and compact.
    """
    def to_hex(notes):
        return pack_notes(list(notes))

    with tqdm(total=len(df), desc="Packing pitches to hex") as pbar:
        df = df.with_columns(
            pl.col(col).map_elements(w_pbar(pbar, to_hex), return_dtype=pl.Utf8).alias(out)
        )
    return df

def pack_pitch_class(notes: list[int]) -> int:
    """
    Pack a list of pitch classes.
    Note we assume that we just care about pitch class (not octave) and order doesn't matter.
    We can also ignore the root.
    That means we can encode with just 11 bits! So everything fits within the range of (0, 2047).
    """
    packed = 0
    for note in notes:
        if note < 0 or note > 11:
            raise ValueError("Pitch class must be in range 0-11")
        if note == 0:
            continue # skip 0: since the root is always assumed
        packed |= (1 << (note-1))
    return packed

def unpack_pitch_class(pcid: int) -> list[int]:
    """
    Unpack a packed pitch class integer into a list of pitch classes.
    """
    if pcid < 0 or pcid > 2047:
        raise ValueError("PCID must be in range 0-2047")
    notes = [0] # always include root
    for i in range(1, 12):
        if pcid & (1 << (i-1)):
            notes.append(i)
    return notes

def pl_add_pcid(df, col='cls', out="pcid"):
    """
    Add column 'pcid', which is an integer representation of the packed pitch classes in 'cls'.
    PCID = pitch class ID
    """
    def to_pcid(notes):
        return pack_pitch_class(list(notes))

    with tqdm(total=len(df), desc="Packing pitch classes to PCID") as pbar:
        df = df.with_columns(
            pl.col(col).map_elements(w_pbar(pbar, to_pcid), return_dtype=pl.Int64).alias(out)
        )
    return df


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