from symusic import Score

import io
from tqdm import tqdm

from voicings.core.chords import all_chord_frequencies_for_score, merge_chord_frequencies
from voicings.core.untar import yield_midi_from_tar


if __name__ == "__main__":
    all_freqs = {}
    for i, midi_file in tqdm(enumerate(yield_midi_from_tar('C:/conjunct/bigdata/aria-midi/aria-midi-v1-ext.tar.gz'))):
        # print("Midi file name:", midi_file.name)
        to_bytes = midi_file.read()

        # score = Score(io.BytesIO(to_bytes))
        score = Score.from_midi(to_bytes)

        freqs = all_chord_frequencies_for_score(score)
        all_freqs = merge_chord_frequencies(all_freqs, freqs)
        
        if i >= 100:
            break
    all_freqs = dict(sorted(all_freqs.items(), key=lambda x: x[1], reverse=True))
    pass