from packing.text import RotatingLog
import pytest
import time


@pytest.fixture
def log(tmp_path):
    l = RotatingLog("log", str(tmp_path), log_lines=10)
    yield l, tmp_path


def test_append_line(log):
    log, outdir = log
    log.append("test line")
    with (outdir / "log_0.log").open() as f:
        assert f.read() == "test line\n"


def test_append_line_timestamp(mocker, log):
    log, outdir = log
    log.timestamp = True
    mocked_time = mocker.patch("time.time")
    mocked_time.return_value = 1630322465.354646
    log.append("test line")
    mocked_time.assert_called_once()
    with (outdir / "log_0.log").open() as f:
        assert f.read() == "1630322465#test line\n"


def test_read_lines(log):
    log, outdir = log
    exp = []
    for i in range(10):
        l = f"test line {i}"
        log.append(l)
        exp.append((i, l))
    assert list(log.read()) == exp


def test_read_lines_timestamp(mocker, log):
    log, outdir = log
    log.timestamp = True
    mocked_time = mocker.patch("time.time")
    mocked_time.return_value = 1630322465.354646
    exp = []
    for i in range(10):
        l = f"test line {i}"
        log.append(l)
        exp.append((i, time.localtime(1630322465), l))
    assert len(mocked_time.call_args_list) == 10
    assert list(log.read()) == exp


def test_read_lines_fake_timestamp(mocker, log):
    log, outdir = log
    log.timestamp_interval = 60
    mocked_time = mocker.patch("time.time")
    mocked_time.return_value = 1630322465
    exp = []
    for i in range(10):
        l = f"test line {i}"
        log.append(l)
        exp.append((i, time.localtime(1630322465 - 600 + i * 60), l))
    mocked_time.assert_has_calls(() * 10)
    assert list(log.read()) == exp


def test_timestampify_errorhandling(log):
    log, outdir = log
    log.timestamp = True

    assert log.timestampify("notimestamp") == (None, "notimestamp")
    assert log.timestampify("this#that") == (None, "this#that")


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
    assert (outdir / "log_1.log").exists()
    assert (outdir / "log_0.log").exists()
    with (outdir / "log_0.log").open() as f:
        assert f.read() == "overflow\n"


def test_rotate_simple(log):
    log, outdir = log
    exp = []
    for i in range(30):
        l = f"test line {i}"
        log.append(l)
        exp.append(l)
    assert (outdir / "log_0.log").exists()
    assert (outdir / "log_1.log").exists()
    assert not (outdir / "log_2.log").exists()


def test_rotate_init(tmp_path):
    with (tmp_path / "log_0.log").open("w") as f:
        f.write("")
    l = RotatingLog("log", str(tmp_path), log_lines=10)
    assert not (tmp_path / "log_0.log").exists()
    assert (tmp_path / "log_1.log").exists()
    l.append("test")
    assert (tmp_path / "log_0.log").exists()
    assert (tmp_path / "log_1.log").exists()


def test_rotate_no_keep(log):
    log, outdir = log
    log.keep_logs = 0
    exp = []
    for i in range(10):
        l = f"test line {i}"
        log.append(l)
        exp.append(l)
    assert not (outdir / "log_1.log").exists(), "Overflowed"
    assert (outdir / "log_0.log").exists(), "No Outf"
    log.append("overflow")
    assert log.pos == 1
    assert not (outdir / "log_1.log").exists()
    assert (outdir / "log_0.log").exists()
    with (outdir / "log_0.log").open() as f:
        assert f.read() == "overflow\n"


def test_rotate_no_log(log):
    log, outdir = log
    log.keep_logs = 0
    log.append("")
    (outdir / "log_0.log").unlink()
    log.rotate_logs()
    assert log.pos == 0
    assert not (outdir / "log_0.log").exists()


def test_read_no_logf(log):
    log, outdir = log
    exp = []
    for i in range(10):
        l = f"test line {i}"
        log.append(l)
        exp.append((i, l))

    resp = list(log.read())
    assert resp == exp


def test_empty_log_line(log):
    log, outdir = log
    exp = []
    for i in range(8):
        l = f"test line {i}"
        log.append(l)
        exp.append((i, l))

    log.append("")
    exp.append((8, ""))

    resp = list(log.read())
    assert resp == exp


def test_read_logf(log):
    log, outdir = log
    exp = []
    for i in range(10):
        l = f"test line {i}"
        log.append(l)
        exp.append((i, l))

    resp = list(log.read(str(outdir / "log_0.log")))
    assert resp == exp


regions = [(2, 0), (2, 2), (5, 5), (4, 10), (17, 0), (15, 1)]


@pytest.mark.parametrize("n,skip", regions)
def test_read_regions(n, skip, log):
    log, outdir = log
    exp = []
    for i in range(17):
        l = f"test line {i}"
        log.append(l)
        exp.append((i, l))

    resp = list(log.read(n=n, skip=skip))
    assert len(resp) == n
    exp = exp[len(exp) - n - skip : len(exp) - skip]
    from devtools import debug

    debug(resp)
    assert resp == exp


def test_read_too_large(log):
    log, outdir = log
    exp = []
    for i in range(17):
        l = f"test line {i}"
        log.append(l)
        exp.append((i, l))

    resp = list(log.read(n=19))
    assert len(resp) == 17
    assert resp == exp


def test_skip_too_large(log):
    log, outdir = log
    for i in range(17):
        l = f"test line {i}"
        log.append(l)

    resp = list(log.read(n=2, skip=16))
    assert len(resp) == 1
    resp = list(log.read(n=2, skip=17))
    assert len(resp) == 0
