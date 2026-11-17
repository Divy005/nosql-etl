"""Raw line reader and batch partitioner.

The batcher reads input files in order, decoding each line as latin-1,
stripping only trailing newline/carriage-return, and grouping lines into
fixed-size batches.  It never parses, filters, or modifies content.
"""

from collections.abc import Generator
from pathlib import Path


def read_batches(
    input_files: list[Path],
    batch_size: int,
) -> Generator[tuple[int, int, list[str]], None, None]:
    """Yield (batch_id, first_line_no, lines) tuples across all input files.

    - batch_id starts at 1 and increments for every full or partial batch.
    - line numbers are global across files, starting at 1.
    - Only the trailing line terminator (\\n or \\r\\n) is stripped.
    - The last partial batch is yielded.
    - Empty input (no files or all files empty) yields nothing.

    Args:
        input_files: Ordered list of file paths to read.
        batch_size: Maximum number of lines per batch (must be >= 1).

    Yields:
        (batch_id, first_line_no, lines) where lines is a list of raw
        decoded strings with trailing newlines stripped.
    """
    if batch_size < 1:
        raise ValueError(f"batch_size must be >= 1, got {batch_size}")

    batch_id = 0
    global_line_no = 0
    current_batch: list[str] = []
    first_line_no = 1

    for filepath in input_files:
        with open(filepath, "rb") as f:
            for raw_bytes in f:
                global_line_no += 1
                line = raw_bytes.decode("latin-1").rstrip("\r\n")

                if not current_batch:
                    first_line_no = global_line_no

                current_batch.append(line)

                if len(current_batch) >= batch_size:
                    batch_id += 1
                    yield (batch_id, first_line_no, current_batch)
                    current_batch = []

    # Yield the last partial batch if any
    if current_batch:
        batch_id += 1
        yield (batch_id, first_line_no, current_batch)
