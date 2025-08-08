
from dataclasses import dataclass


def int_to_note_name(note: int) -> str:
    """Convert an integer note value to a string representation."""
    note_names = [
        "C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"
    ]
    octave = note // 12
    note_name = note_names[note % 12]
    return f"{note_name}{octave}"

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