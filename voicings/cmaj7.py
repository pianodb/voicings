from symusic import Score

import io
from tqdm import tqdm
import polars as pl
import multiprocessing as mp
from functools import partial
import tarfile
import queue
import threading

from voicings.core.chords import all_chord_frequencies_for_score, all_chords_for_score, merge_chord_frequencies
from voicings.core.untar import yield_midi_from_tar


def process_midi_file(midi_data_tuple):
    """
    Process a single MIDI file and return chord data.
    
    Args:
        midi_data_tuple: (fname, midi_bytes) tuple
        
    Returns:
        dict with lists of chord data for this file
    """
    fname, midi_bytes = midi_data_tuple
    
    try:
        score = Score.from_midi(midi_bytes)
        chords = all_chords_for_score(score)
        
        # Keep only those with >=3 notes
        chords = [chord for chord in chords if len(chord.notes) >= 3]
        
        collector = {
            'fname': [],
            'notes': [],
            'duration': [],
            'at': [],
        }
        
        for chord in chords:
            collector['fname'].append(fname)
            collector['notes'].append(chord.notes)
            collector['duration'].append(chord.duration)
            collector['at'].append(chord.at)
            
        return collector
    except Exception as e:
        print(f"Error processing {fname}: {e}")
        return {
            'fname': [],
            'notes': [],
            'duration': [],
            'at': [],
        }


def collect_midi_files_from_tar(tar_path, max_files=100):
    """
    Extract MIDI files from tar.gz and return list of (fname, midi_bytes) tuples.
    """
    midi_files = []
    
    with tarfile.open(tar_path, 'r:gz') as tar:
        for i, member in enumerate(tar.getmembers()):
            if i >= max_files:
                break
            if member.name.endswith('.mid'):
                f = tar.extractfile(member)
                if f:
                    midi_bytes = f.read()
                    midi_files.append((member.name, midi_bytes))
    
    return midi_files


def tar_producer(tar_path, file_queue, max_files=100):
    """
    Producer function that reads MIDI files from tar and puts them in queue.
    Runs in a separate thread to stream files without loading all into memory.
    """
    try:
        with tarfile.open(tar_path, 'r:gz') as tar:
            count = 0
            for member in tar.getmembers():
                if count >= max_files:
                    break
                if member.name.endswith('.mid'):
                    f = tar.extractfile(member)
                    if f:
                        midi_bytes = f.read()
                        file_queue.put((member.name, midi_bytes))
                        count += 1
    except Exception as e:
        print(f"Error in tar producer: {e}")
    finally:
        # Signal end of files
        file_queue.put(None)


def collect_chords_streaming_multiprocessing(n_files=100, n_processes=None, queue_size=50):
    """
    Memory-efficient multiprocessing version that streams files from tar.
    
    Args:
        n_files: Number of MIDI files to process
        n_processes: Number of processes to use (default: CPU count)
        queue_size: Maximum number of files to buffer in memory
    
    Returns:
        Polars DataFrame with chord data
    """
    if n_processes is None:
        n_processes = mp.cpu_count()
    
    print(f"Using {n_processes} processes to process {n_files} files (streaming)")
    
    # Create a queue to hold MIDI files
    file_queue = queue.Queue(maxsize=queue_size)
    
    # Start producer thread to read from tar
    producer_thread = threading.Thread(
        target=tar_producer,
        args=('C:/conjunct/bigdata/aria-midi/aria-midi-v1-ext.tar.gz', file_queue, n_files)
    )
    producer_thread.start()
    
    # Process files as they become available
    results = []
    processed_count = 0
    
    with mp.Pool(n_processes) as pool:
        active_tasks = []
        
        with tqdm(total=n_files, desc="Processing MIDI files") as pbar:
            while processed_count < n_files:
                # Get next file from queue (or None if done)
                try:
                    midi_data = file_queue.get(timeout=1.0)
                    if midi_data is None:  # End signal
                        break
                    
                    # Submit task to process pool
                    future = pool.apply_async(process_midi_file, (midi_data,))
                    active_tasks.append(future)
                    
                except queue.Empty:
                    pass  # Continue to check for completed tasks
                
                # Check for completed tasks
                completed_tasks = []
                for i, task in enumerate(active_tasks):
                    if task.ready():
                        try:
                            result = task.get()
                            results.append(result)
                            processed_count += 1
                            pbar.update(1)
                        except Exception as e:
                            print(f"Error processing file: {e}")
                            processed_count += 1
                            pbar.update(1)
                        completed_tasks.append(i)
                
                # Remove completed tasks
                for i in reversed(completed_tasks):
                    active_tasks.pop(i)
            
            # Wait for remaining tasks to complete
            for task in active_tasks:
                try:
                    result = task.get()
                    results.append(result)
                    processed_count += 1
                    pbar.update(1)
                except Exception as e:
                    print(f"Error processing file: {e}")
    
    # Wait for producer thread to finish
    producer_thread.join()
    
    # Combine all results
    combined_collector = {
        'fname': [],
        'notes': [],
        'duration': [],
        'at': [],
    }
    
    for result in results:
        for key in combined_collector:
            combined_collector[key].extend(result[key])
    
    df = pl.DataFrame(combined_collector)
    return df


def collect_chords_multiprocessing(n_files=100, n_processes=None):
    """
    Multiprocessing version of collect_chords.
    
    Args:
        n_files: Number of MIDI files to process
        n_processes: Number of processes to use (default: CPU count)
    
    Returns:
        Polars DataFrame with chord data
    """
    if n_processes is None:
        n_processes = mp.cpu_count()
    
    print(f"Using {n_processes} processes to process {n_files} files")
    
    # First, extract all MIDI files from the tar
    print("Extracting MIDI files from tar.gz...")
    midi_files = collect_midi_files_from_tar(
        'C:/conjunct/bigdata/aria-midi/aria-midi-v1-ext.tar.gz', 
        n_files
    )
    
    print(f"Found {len(midi_files)} MIDI files, processing...")
    
    # Process files in parallel
    with mp.Pool(n_processes) as pool:
        # Use tqdm to show progress
        results = list(tqdm(
            pool.imap(process_midi_file, midi_files),
            total=len(midi_files),
            desc="Processing MIDI files"
        ))
    
    # Combine all results
    combined_collector = {
        'fname': [],
        'notes': [],
        'duration': [],
        'at': [],
    }
    
    for result in results:
        for key in combined_collector:
            combined_collector[key].extend(result[key])
    
    df = pl.DataFrame(combined_collector)
    return df



def collect_chords(top_k=100, aggregate_mode=True, batch_size=10000):
    # 8 seconds for 100 midi files
    # = 1,186,253

    collector = {
        'fname': [],
        'notes': [],
        'duration': [],
        # 'at': [],
    }
    # all_freqs = {}
    # n = 100

    def _save_batch():
        nonlocal current_fragment, collector
        df = pl.DataFrame(collector)
        if aggregate_mode:
            # Care only about total duration of each chord
            df = df.group_by('fname', 'notes').agg(
                pl.col('duration').sum().alias('duration')
            )
        df.write_parquet(f'data/fragments/fragment_{current_fragment}.parquet')
        del df  # Free memory
        current_fragment += 1

    current_fragment = 0

    for i, (fname, midi_file) in tqdm(enumerate(yield_midi_from_tar('C:/conjunct/bigdata/aria-midi/aria-midi-v1-ext.tar.gz')), total=top_k):
        # print("Midi file name:", midi_file.name)
        to_bytes = midi_file.read()

        # score = Score(io.BytesIO(to_bytes))
        score = Score.from_midi(to_bytes)

        # freqs = all_chord_frequencies_for_score(score)
        # all_freqs = merge_chord_frequencies(all_freqs, freqs)
        
        chords = all_chords_for_score(score)

        # keep only those with >=3 notes
        chords = [chord for chord in chords if len(chord.notes) >= 3]
        if i >= top_k:
            break
        for chord in chords:
            collector['fname'].append(fname)
            collector['notes'].append(chord.notes)
            collector['duration'].append(chord.duration)
            # collector['at'].append(chord.at)

        if (i + 1) % batch_size == 0:
            # Save current batch to disk
            _save_batch()
            collector = {
                'fname': [],
                'notes': [],
                'duration': [],
                # 'at': [],
            }
    # Save any remaining chords
    if collector['fname']:
        _save_batch()

    # aggregate mode: 
    if aggregate_mode:
        # Care only about total duration of each chord
        df = df.group_by('fname', 'notes').agg(
            pl.col('duration').sum().alias('duration')
        )
    # return df



if __name__ == "__main__":
    # df = collect_chords()
    # Original sequential version
    collect_chords(20000) 
    # 1.76M rows for 1000 files
    # will need about 15 MB

    # with n-gram mode: 7.2 MB (half size reduction)

    # = 15 GB for full dataset
    
    # Memory-efficient multiprocessing version
    # df = collect_chords_streaming_multiprocessing(n_files=1000, n_processes=4)
    # print(f"Collected {len(df)} chord records")
