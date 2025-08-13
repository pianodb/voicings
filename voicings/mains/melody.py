from typing import List, Union
from voicings.core.encipher import unpack_notes


def is_melodic(notes: Union[str, List[int]], maximum_span=17):
    """
    Check if purely melodic: that is:

    RH plays one note separated from LH by large gap. 
    """

    if isinstance(notes, str):
        notes = unpack_notes(notes)

    # assume we have LH and RH:
    # assume the maximum hand span is a 11th (ie. 17 semitones)
    # hence: assume that all notes fall within:
    # (bass + 17), (soprano - 17)

    highest = notes[-1]
    if highest <= maximum_span:
        return False # no - we are clustered
    
    LH_max = 0
    RH = set()
    for note in notes:
        if note > maximum_span:
            RH.add(note)
        else:
            LH_max = max(LH_max, note)
    
    if len(RH) == 1:
        if RH.pop() > LH_max + 24:
            # separated by two octaves from LH, and only one note in RH
            return True
    return False