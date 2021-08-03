from packing.text import RotatingLog
import pytest


@pytest.fixture
def log(tmp_path):
    l = RotatingLog("log", str(tmp_path), log_lines=10)
    yield l, tmp_path


def test_append_line(log):
    log, outdir = log
    log.append("test line")
    with (outdir / "log_0.log").open() as f:
        assert f.read() == "test line\n"


def test_read_lines(log):
    log, outdir = log
    exp = []
    for i in range(10):
        l = f"test line {i}"
        log.append(l)
        exp.append(l)
    assert list(log.read()) == exp


def test_rotate(log):
    log, outdir = log
    exp = []
    for i in range(10):
        l = f"test line {i}"
        log.append(l)
        exp.append(l)
    assert not (outdir / "log_1.log").exists(), "Overflowed"
    assert (outdir / "log_0.log").exists(), "No Outf"
    log.append("overflow")
    print(list(outdir.glob("*")))
    assert (outdir / "log_1.log").exists()
    assert (outdir / "log_0.log").exists()
    with (outdir / "log_0.log").open() as f:
        assert f.read() == "overflow\n"


def test_read_logf(log):
    log, outdir = log
    exp = []
    for i in range(11):
        l = f"test line {i}"
        log.append(l)
        exp.append(l)
    assert not (outdir / "log_1.log").exists(), "Overflowed"
    assert (outdir / "log_0.log").exists(), "No Outf"
    log.append("overflow")
    print(list(outdir.glob("*")))
    assert (outdir / "log_1.log").exists()
    assert (outdir / "log_0.log").exists()
    with (outdir / "log_0.log").open() as f:
        assert f.read() == "overflow\n"
