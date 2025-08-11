import os
import glob
import multiprocessing as mp
from tqdm import tqdm
import polars as pl
from symusic import Score
from voicings.core.chords import all_chords_for_score


def process_midi_file(file_path):
    try:
        with open(file_path, 'rb') as f:
            midi_bytes = f.read()
        score = Score.from_midi(midi_bytes)
        chords = all_chords_for_score(score)
        chords = [c for c in chords if len(c.notes) >= 3]

        collector = {
            'fname': [],
            'notes': [],
            'duration': [],
        }

        for chord in chords:
            collector['fname'].append(file_path) # os.path.basename(file_path))
            collector['notes'].append(chord.notes)
            collector['duration'].append(chord.duration)

        return collector

    except Exception as e:
        # print(f"Error processing {file_path}: {e}")
        return {
            'fname': [file_path],
            'notes': [None],
            'duration': [None],
        }


def process_batch(batch_id, midi_files, aggregate_mode=True, output_dir="data/fragments"):
    collector = {
        'fname': [],
        'notes': [],
        'duration': [],
    }

    for midi_path in midi_files:
        result = process_midi_file(midi_path)
        for key in collector:
            collector[key].extend(result[key])

    df = pl.DataFrame(collector, schema={
        'fname': pl.Utf8,
        'notes': pl.List(pl.Int32),
        'duration': pl.Float64,
    })

    if aggregate_mode:
        df = df.group_by('fname', 'notes').agg(
            pl.col('duration').sum().alias('duration')
        )

    os.makedirs(output_dir, exist_ok=True)
    df.write_parquet(os.path.join(output_dir, f"fragment_{batch_id}.parquet"))


def collect_chords_directory_parallel(
    midi_root: str,
    batch_size: int = 1000,
    n_processes: int = None,
    aggregate_mode: bool = True,
    output_dir: str = "data/fragments"
):
    all_midi_files = sorted(glob.glob(os.path.join(midi_root, "**", "*.mid"), recursive=True))

    print(f"Found {len(all_midi_files)} MIDI files.")

    # Split into batches
    batches = [
        all_midi_files[i:i + batch_size]
        for i in range(0, len(all_midi_files), batch_size)
    ]

    if n_processes is None:
        n_processes = min(mp.cpu_count(), len(batches))

    print(f"Processing {len(batches)} batches using {n_processes} processes...")

    with mp.Pool(n_processes) as pool:
        args = [
            (i, batch, aggregate_mode, output_dir)
            for i, batch in enumerate(batches)
        ]
        
        # Use tqdm with explicit configuration for better visibility
        results = []
        with tqdm(total=len(args), desc="Processing batches", unit="batch") as pbar:
            for result in pool.imap_unordered(_process_batch_wrapper, args):
                results.append(result)
                pbar.update(1)
                pbar.refresh()


def _process_batch_wrapper(args):
    """Wrapper function to unpack arguments for process_batch."""
    return process_batch(*args)


if __name__ == "__main__":
    # Example usage:

    # print PID of main process
    print(f"Main process PID: {os.getpid()}")

    collect_chords_directory_parallel(
        midi_root="C:/conjunct/bigdata/aria-midi/aria-midi-v1-ext/data",
        batch_size=1000,
        n_processes=4,
        aggregate_mode=True,
        output_dir="data/fragments"
    )
