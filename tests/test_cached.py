from packing.cached import CachingPackedRotatingLog
from devtools import debug
import pytest


def test_oversize_buffer(tmp_path):
    with pytest.raises(ValueError):
        p = CachingPackedRotatingLog(
            "log",
            str(tmp_path),
            2,
            2,
            8,
            buffer_size=11,
            log_lines=10,
        )


@pytest.fixture
def packer(tmp_path):
    p = CachingPackedRotatingLog(
        "log",
        str(tmp_path),
        2,
        2,
        8,
        buffer_size=6,
        log_lines=10,
    )
    yield p, tmp_path


def test_append_ram(packer):
    packer, tmp_path = packer
    assert packer.buf == bytearray(packer.line_size * packer.buffer_size)
    packer.append(floats=[1, 2], ints=[1, 2], bools=[True, False] * 4)
    packed = packer.pack(floats=[1, 2], ints=[1, 2], bools=[True, False] * 4)
    assert bytes(packer.buf[: len(packed)]) == packed
    assert packer.pos == 1
    assert packer.buffer_pos == 1
    packer.append(floats=[3, 2], ints=[3, 2], bools=[True, False] * 4)
    assert packer.pos == 2
    unp = packer.unpack(packer.buf[len(packed) : len(packed) * 2])
    assert unp.floats == pytest.approx([3, 2])
    for i in range(3):
        packer.append(floats=[3, 2], ints=[3, 2], bools=[True, False] * 4)
    assert packer.pos == 5
    assert packer.buffer_pos == 5
    assert not (tmp_path / "log_0.bin").exists()


def test_read_ram(packer, equal):
    packer, tmp_path = packer
    exp = []
    for i in range(4):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([floats, floats, bools])
    assert packer.buffer_pos == 4

    resp = list(packer.read())
    debug(exp, resp)
    assert equal(exp, resp)


def test_read_logf(packer, equal):
    packer, tmp_path = packer
    exp = []
    for i in range(packer.buffer_size):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([floats, floats, bools])
    packer.flush()
    p = tmp_path / "log_0.bin"
    resp = list(packer.read(str(p)))
    assert equal(exp, resp)


def test_rotate_logs(packer):
    packer, tmp_path = packer
    for i in range(6):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
    packer.flush()
    assert (tmp_path / "log_0.bin").exists(), "Failed to make file"
    assert not (tmp_path / "log_1.bin").exists()
    packer.rotate_logs()
    assert not (tmp_path / "log_0.bin").exists(), "Still got old file"
    assert (tmp_path / "log_1.bin").exists()
    packer.rotate_logs()
    assert not (tmp_path / "log_1.bin").exists(), "Still got old file"


def test_rotate_logs_no_keep(packer):
    packer, tmp_path = packer
    packer.keep_logs = 0
    for i in range(6):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
    packer.flush()
    assert (tmp_path / "log_0.bin").exists(), "Failed to make file"
    assert not (tmp_path / "log_1.bin").exists()
    packer.rotate_logs()
    assert not (tmp_path / "log_0.bin").exists()
    assert not (tmp_path / "log_1.bin").exists()


def test_rotate_trigger(packer, equal):
    packer, tmp_path = packer
    exp = []
    for i in range(packer.log_lines):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([floats, floats, bools])

    log0 = tmp_path / "log_0.bin"
    log1 = tmp_path / "log_1.bin"
    assert log0.exists(), "Failed to make file"
    assert not log1.exists()
    packer.append(floats=floats, bools=[True, False, False, True] * 2, ints=floats)
    assert not log0.exists()
    assert log1.exists()
    resp = list(packer.read(str(log1)))
    debug(resp)
    assert equal(exp, resp)
    assert list(packer.read(n=1))[-1].bools == tuple([True, False, False, True] * 2)


def test_read_no_logf(packer):
    packer, tmp_path = packer
    for i in range(6):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
    packer.flush()
    resp = list(packer.read(str(tmp_path / "packer_5.bin")))
    assert resp == []


regions = [
    (2, 0),
    (2, 2),
    (5, 5),
    (4, 10),
    (17, 0),
    (15, 1),
]


@pytest.mark.parametrize("n,skip", regions)
def test_read_regions(n, skip, packer, equal):
    packer, tmp_path = packer
    exp = []
    for i in range(17):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([floats, floats, bools])

    resp = list(packer.read(n=n, skip=skip))

    exp = exp[len(exp) - n - skip : len(exp) - skip]
    assert equal(exp, resp)
