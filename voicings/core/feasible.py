from typing import List, Union

from voicings.core.encipher import unpack_notes


def is_feasible(notes: Union[str, List[int]], maximum_span=17):
    """
    Check if a 
    """

    if isinstance(notes, str):
        notes = unpack_notes(notes)

    # assume we have LH and RH:
    # assume the maximum hand span is a 11th (ie. 17 semitones)
    # hence: assume that all notes fall within:
    # (bass + 17), (soprano - 17)
    bass = notes[0]
    soprano = notes[-1]
    for note in notes:
        if not (bass <= note <= bass + maximum_span or soprano - maximum_span <= note <= soprano):
            return False
    return True
