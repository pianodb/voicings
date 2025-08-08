import tarfile
import io

def yield_midi_from_tar(tar_gz):
    """Yield MIDI files from a tar.gz archive."""

    if isinstance(tar_gz, str):
        tar_gz = open(tar_gz, 'rb')

    try:
        with tarfile.open(fileobj=tar_gz, mode='r:gz') as tar:
            for member in tar: # .getmembers():
                if member.name.endswith('.mid'):
                    f = tar.extractfile(member)
                    yield f
                    # if f:
                        # yield io.BytesIO(f.read())
    finally:
        if isinstance(tar_gz, str):
            tar_gz.close()

if __name__ == "__main__":
    for midi_file in yield_midi_from_tar('C:/conjunct/bigdata/aria-midi/aria-midi-v1-ext.tar.gz'):
        print("Midi file name:", midi_file.name)
        to_bytes = midi_file.read()
        break
