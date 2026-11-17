"""Unit tests for the controller batcher module."""

from pathlib import Path

from controller.batcher import read_batches


def _write_file(path: Path, lines: list[str]) -> None:
    """Write lines to a file with latin-1 encoding and \\n terminators."""
    with open(path, "wb") as f:
        for line in lines:
            f.write((line + "\n").encode("latin-1"))


class TestBatchIds:
    """Batch IDs start at 1 and increment sequentially."""

    def test_single_file_single_batch(self, tmp_path: Path) -> None:
        f = tmp_path / "a.log"
        _write_file(f, ["line1", "line2", "line3"])

        batches = list(read_batches([f], batch_size=10))
        assert len(batches) == 1
        assert batches[0][0] == 1  # batch_id

    def test_multiple_batches(self, tmp_path: Path) -> None:
        f = tmp_path / "a.log"
        _write_file(f, [f"line{i}" for i in range(10)])

        batches = list(read_batches([f], batch_size=3))
        assert [b[0] for b in batches] == [1, 2, 3, 4]  # 3+3+3+1

    def test_exact_batch_boundary(self, tmp_path: Path) -> None:
        f = tmp_path / "a.log"
        _write_file(f, [f"line{i}" for i in range(6)])

        batches = list(read_batches([f], batch_size=3))
        assert [b[0] for b in batches] == [1, 2]  # exactly 3+3, no partial


class TestLineNumbering:
    """Line numbers are global across files, starting at 1."""

    def test_single_file(self, tmp_path: Path) -> None:
        f = tmp_path / "a.log"
        _write_file(f, ["a", "b", "c", "d", "e"])

        batches = list(read_batches([f], batch_size=2))
        assert batches[0][1] == 1  # first_line_no of batch 1
        assert batches[1][1] == 3  # first_line_no of batch 2
        assert batches[2][1] == 5  # first_line_no of batch 3

    def test_across_two_files(self, tmp_path: Path) -> None:
        """Line numbers continue across file boundaries."""
        f1 = tmp_path / "a.log"
        f2 = tmp_path / "b.log"
        _write_file(f1, ["a", "b", "c"])
        _write_file(f2, ["d", "e"])

        batches = list(read_batches([f1, f2], batch_size=2))
        # Batch 1: lines 1,2  Batch 2: lines 3,4  Batch 3: line 5
        assert batches[0] == (1, 1, ["a", "b"])
        assert batches[1] == (2, 3, ["c", "d"])
        assert batches[2] == (3, 5, ["e"])


class TestPartialLastBatch:
    """The last partial batch is always yielded."""

    def test_partial(self, tmp_path: Path) -> None:
        f = tmp_path / "a.log"
        _write_file(f, ["a", "b", "c", "d", "e"])

        batches = list(read_batches([f], batch_size=3))
        assert len(batches) == 2
        assert len(batches[0][2]) == 3
        assert len(batches[1][2]) == 2  # partial batch
        assert batches[1][2] == ["d", "e"]


class TestEmptyInput:
    """Empty input files or no files should yield nothing."""

    def test_empty_file(self, tmp_path: Path) -> None:
        f = tmp_path / "empty.log"
        f.write_bytes(b"")

        batches = list(read_batches([f], batch_size=10))
        assert batches == []

    def test_no_files(self) -> None:
        batches = list(read_batches([], batch_size=10))
        assert batches == []

    def test_multiple_empty_files(self, tmp_path: Path) -> None:
        f1 = tmp_path / "a.log"
        f2 = tmp_path / "b.log"
        f1.write_bytes(b"")
        f2.write_bytes(b"")

        batches = list(read_batches([f1, f2], batch_size=10))
        assert batches == []


class TestTotalLineCount:
    """Total lines across all batches must equal total lines in input."""

    def test_total_count(self, tmp_path: Path) -> None:
        f1 = tmp_path / "a.log"
        f2 = tmp_path / "b.log"
        _write_file(f1, [f"a{i}" for i in range(7)])
        _write_file(f2, [f"b{i}" for i in range(5)])

        batches = list(read_batches([f1, f2], batch_size=3))
        total = sum(len(b[2]) for b in batches)
        assert total == 12

    def test_batch_size_1(self, tmp_path: Path) -> None:
        f = tmp_path / "a.log"
        _write_file(f, ["x", "y", "z"])

        batches = list(read_batches([f], batch_size=1))
        assert len(batches) == 3
        assert all(len(b[2]) == 1 for b in batches)
        assert [b[0] for b in batches] == [1, 2, 3]


class TestLineTerminatorStripping:
    """Only trailing \\n and \\r\\n are stripped; content is preserved."""

    def test_crlf_stripped(self, tmp_path: Path) -> None:
        f = tmp_path / "crlf.log"
        f.write_bytes(b"hello\r\nworld\r\n")

        batches = list(read_batches([f], batch_size=10))
        assert batches[0][2] == ["hello", "world"]

    def test_lf_stripped(self, tmp_path: Path) -> None:
        f = tmp_path / "lf.log"
        f.write_bytes(b"hello\nworld\n")

        batches = list(read_batches([f], batch_size=10))
        assert batches[0][2] == ["hello", "world"]

    def test_internal_whitespace_preserved(self, tmp_path: Path) -> None:
        f = tmp_path / "ws.log"
        f.write_bytes(b"  hello  world  \n")

        batches = list(read_batches([f], batch_size=10))
        assert batches[0][2] == ["  hello  world  "]

    def test_only_one_terminator_stripped(self, tmp_path: Path) -> None:
        # Only the final \r\n / \n is the terminator; an extra \r is line content.
        f = tmp_path / "cr.log"
        f.write_bytes(b"a\r\r\nb\n\nc")

        batches = list(read_batches([f], batch_size=10))
        assert batches[0][2] == ["a\r", "b", "", "c"]


class TestLatin1Decoding:
    """Binary data is decoded as latin-1 losslessly."""

    def test_high_bytes(self, tmp_path: Path) -> None:
        f = tmp_path / "latin1.log"
        f.write_bytes(b"\xe9\xf1\xfc line\n")

        batches = list(read_batches([f], batch_size=10))
        assert batches[0][2] == ["\xe9\xf1\xfc line"]
