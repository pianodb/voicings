import symusic.types as smt

from voicings.core.decipher import Voicing

def all_chords_for_score(score: smt.Score) -> list[Voicing]:
    """
    Get all chords (sets of pitches sounding together) for a given score.
    Returns a list of Voicing objects with duration information, sorted by time.
    """
    # Build a list of (time, pitch, on/off) events
    events = []

    for track in score.tracks:
        for note in track.notes:
            events.append((note.start, note.pitch, 'on'))
            events.append((note.end, note.pitch, 'off'))
    # Sort events by time, with 'off' before 'on' at the same time
    events.sort(key=lambda x: (x[0], 0 if x[2] == 'off' else 1))

    chords = []
    sounding = set()
    last_time = None
    current_chord_start = None
    
    for time, pitch, evtype in events:
        if time != last_time and last_time is not None:
            if sounding:
                # If we have a current chord, finalize it with duration
                if current_chord_start is not None:
                    duration = time - current_chord_start
                    chords.append(Voicing(tuple(sorted(sounding)), current_chord_start, duration))
                current_chord_start = time
            elif current_chord_start is not None:
                # Silence period - no chord sounding
                current_chord_start = None
                
        if evtype == 'on':
            sounding.add(pitch)
            # Start timing if this is the first note of a new chord
            if len(sounding) == 1 and current_chord_start is None:
                current_chord_start = time
        else:
            sounding.discard(pitch)
            
        last_time = time
    
    # Add the final chord if any (duration until end of score)
    if sounding and current_chord_start is not None:
        # Calculate duration until the last event time
        final_duration = last_time - current_chord_start if last_time else 0
        chords.append(Voicing(tuple(sorted(sounding)), current_chord_start, final_duration))
    
    return chords

def all_chord_frequencies_for_score(score: smt.Score):
    """
    Get all chord frequencies for a given score.
    Returns a list of (time, frequency) tuples, sorted by time.
    """
    chords = all_chords_for_score(score)
    # for chord in chords:
    #     chord.at = None
    chord_frequencies = {}
    for chord in chords:
        if chord.notes not in chord_frequencies:
            chord_frequencies[chord.notes] = 0
        chord_frequencies[chord.notes] += chord.duration
    # return dict(sorted(chord_frequencies.items(), key=lambda x: x[1], reverse=True))

    # turn these back into Voicings for better repr
    voicing_frequencies = {}
    for notes, duration in sorted(chord_frequencies.items(), key=lambda x: x[1], reverse=True):
        voicing_frequencies[Voicing(notes)] = duration
    return voicing_frequencies

def merge_chord_frequencies(freqs_a, freqs_b):
    """
    Merge two chord frequency dictionaries.
    Returns a new dictionary with combined frequencies.
    """
    merged = {}
    for chord, freq in freqs_a.items():
        merged[chord] = merged.get(chord, 0) + freq
    for chord, freq in freqs_b.items():
        merged[chord] = merged.get(chord, 0) + freq
    return merged

if __name__ == "__main__":
    from symusic import Score
    score = Score("data/music/bach_846.mid")
    
    # Show individual chords with durations
    chords = all_chords_for_score(score)

    print("\nChord frequencies:")
    chord_freqs = all_chord_frequencies_for_score(score)
    print(chord_freqs)