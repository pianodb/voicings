from typing import Callable
import polars as pl
from tqdm import tqdm

from voicings.core.pl_tqdm import w_pbar

def classify_chords(df: pl.DataFrame):
    """
    Adds the "bass" column, which is the lowest note.
    Adds the "rel" column, which is the notes relative to the bass.
    Adds the "cls" column, deduplicates 
    """

    def notes_to_cls(notes):
        bass = min(notes)
        diffs = [note - bass for note in notes]
        return diffs
        # take mod 12 (octave deduplicated)
        # return sorted(set(diff % 12 for diff in diffs))


    with tqdm(total=df.height, desc="Classifying notes") as pbar:
        df = df.with_columns(
            pl.col("notes").map_elements(w_pbar(pbar, notes_to_cls), return_dtype=pl.List(pl.Int64)).alias("rel"),
            pl.col("notes").list.min().alias("bass"),
        ).with_columns(
            pl.col("rel").list.eval(
                pl.element() % 12
            ).list.unique().list.sort().alias("cls")
        )
    return df

def list_eval_ref(
    list_col,
    ref_col,
    op: Callable[[pl.Expr, pl.Expr], pl.Expr],
) -> pl.Expr:
    return pl.concat_list(pl.struct(list_col, ref_col)).list.eval(
        op(
            pl.element().struct.field(list_col).explode(),
            pl.element().struct.field(ref_col),
        )
    )

def untranspose_chords(df: pl.DataFrame):
    """
    Untransposes the chords by subtracting the bass from all notes.
    This is useful for comparing chords without regard to their transposition.

    Adds columns "bass" and "untransposed"
    """
    df = df.with_columns(
        pl.col("notes").list.min().alias("bass"),
    )
    
    # .with_columns(
    #     pl.col("notes").list.eval(
    #         pl.element() - pl.col("bass")
    #     ).alias("untransposed")
    # )

    # https://github.com/pola-rs/polars/issues/7210
    df = df.with_columns(
        list_eval_ref(
            'notes',
            'bass',
            lambda notes, bass: notes - bass
        ).alias("untransposed")
    )
    return df
