
from dataclasses import dataclass

import polars as pl
from tqdm import tqdm

from voicings.core.pl_tqdm import w_pbar

def int_to_note_name(note: int, octave=True) -> str:
    """Convert an integer note value to a string representation."""
    note_names = [
        "C", "Db", "D", "Eb", "E", "F", "Gb", "G", "Ab", "A", "Bb", "B"
    ]
    octave = note // 12
    note_name = note_names[note % 12]
    if octave:
        return f"{note_name}{octave}"
    return note_name

@dataclass(unsafe_hash=True)
class Voicing:
    notes: tuple[int]
    at: int = None
    duration: float = None

    def __repr__(self):
        notes_str = ' '.join(int_to_note_name(n) for n in self.notes)
        if self.duration is not None:
            return f"Voicing({notes_str}, duration={self.duration:.3f})"
        return f"Voicing({notes_str})"

def pretty_print_chords(df: pl.DataFrame, col='notes', octave=True) -> pl.DataFrame:
    """
    Adds the column "notes_str" to the DataFrame
    """
    def to_better_name(notes):
        return " ".join(int_to_note_name(n, octave=octave) for n in notes)

    with tqdm(total=df.height, desc="Pretty printing chords") as pbar:
        df = df.with_columns(
            pl.col(col).map_elements(w_pbar(pbar, to_better_name), return_dtype=pl.Utf8).alias("notes_str")
        )
    
    return df