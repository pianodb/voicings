import tarfile
import os
import uuid
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
import polars as pl

def process_chunk(batch, aggregate_mode, fragment_dir):
    from symusic import Score
    from voicings.core.chords import all_chords_for_score
    # from your_module import Score, all_chords_for_score  # Import inside if needed for pickling

    fname_list, notes_list, duration_list = [], [], []

    for fname, midi_bytes in tqdm(batch):
        try:
            score = Score.from_midi(midi_bytes)
            chords = all_chords_for_score(score)
            chords = [ch for ch in chords if len(ch.notes) >= 3]

            for ch in chords:
                fname_list.append(fname)
                notes_list.append(ch.notes)
                duration_list.append(ch.duration)
        except Exception:
            continue

    if not fname_list:
        return None  # Skip empty results

    df = pl.DataFrame({
        'fname': fname_list,
        'notes': notes_list,
        'duration': duration_list,
    })

    if aggregate_mode:
        df = df.group_by('fname', 'notes').agg(
            pl.col('duration').sum().alias('duration')
        )

    # Write to unique file
    fragment_path = os.path.join(fragment_dir, f"fragment_{uuid.uuid4().hex}.parquet")
    df.write_parquet(fragment_path)
    return fragment_path

def chunked(iterable, size):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk

def collect_chords(n=10000, aggregate_mode=True, chunk_size=1000, fragment_dir="data/fragments"):
    os.makedirs(fragment_dir, exist_ok=True)
    tar_path = 'C:/conjunct/bigdata/aria-midi/aria-midi-v1-ext.tar.gz'

    tasks = []
    processed = 0

    with tarfile.open(tar_path, mode='r:gz') as tar:
        midi_files = (
            (member.name, tar.extractfile(member).read())
            for member in tar
            if member.name.endswith('.mid')
        )

        for batch in chunked(midi_files, chunk_size):
            if processed >= n:
                break
            batch = batch[:n - processed]  # limit to n
            processed += len(batch)
            tasks.append((batch, aggregate_mode, fragment_dir))

    # Use multiprocessing — one process per chunk
    with Pool(cpu_count()) as pool:
        results = list(tqdm(pool.starmap(process_chunk, tasks), total=len(tasks)))

    print(f"✅ Finished processing {processed} MIDI files.")
    print(f"🗂️ Fragments written to: {fragment_dir}")

if __name__ == "__main__":
    collect_chords(n=10000, aggregate_mode=True, chunk_size=1000, fragment_dir="data/fragments")