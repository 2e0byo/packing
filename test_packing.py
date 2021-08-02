import packing
import pytest
from devtools import debug


def test_pack_bools_all():
    resp = packing.pack_bools([True] * 8)
    assert resp == 0b11111111


def test_pack_bools_none():
    resp = packing.pack_bools([False] * 8)
    assert resp == 0


def test_pack_bools_mix():
    resp = packing.pack_bools([True, False] * 4)
    assert resp == 0b01010101


packtests = [
    [(12.7, 13.6), [True, False, True, False]],
    [(), [True, False] * 10],
    [(12.6, 12.7, -9.6), []],
]


@pytest.mark.parametrize("floats, bools", packtests)
def test_pack_unpack(floats, bools):
    packer = packing.PackedReadings(
        name="packer", outdir="/tmp", log_size=100, floats=len(floats), bools=len(bools)
    )
    resp = packer.pack(floats, bools)
    assert resp
    rfloats, rbools = packer.unpack(resp)
    assert rbools == bools
    if floats:
        assert rfloats == pytest.approx(floats)
    else:
        assert rfloats == floats


@pytest.fixture(scope="function")
def packer(tmp_path):
    p = packing.PackedReadings(
        name="packer",
        outdir=str(tmp_path),
        log_size=10,
        floats=2,
        bools=1,
        buffer_size=5,
        keep_logs=5,
    )
    yield p, tmp_path


def test_rotate_logs(packer):
    packer, tmp_path = packer
    for _ in range(5):
        packer.append([1, 2], [True])
    packer.write_log()
    assert (tmp_path / "packer_0.bin").exists(), "Failed to make file"
    assert not (tmp_path / "packer_1.bin").exists()
    packer.rotate_logs()
    assert not (tmp_path / "packer_0.bin").exists(), "Still got old file"
    assert (tmp_path / "packer_1.bin").exists()
    packer.rotate_logs()
    assert not (tmp_path / "packer_1.bin").exists(), "Still got old file"


def test_rotate_logs_no_keep(packer):
    packer, tmp_path = packer
    packer.keep_logs = 0
    for _ in range(5):
        packer.append([1, 2], [True])
    packer.write_log()
    assert (tmp_path / "packer_0.bin").exists(), "Failed to make file"
    assert not (tmp_path / "packer_1.bin").exists()
    packer.rotate_logs()
    assert not (tmp_path / "packer_0.bin").exists()
    assert not (tmp_path / "packer_1.bin").exists()


def test_read_logf(packer):
    from devtools import debug

    packer, tmp_path = packer
    for _ in range(5):
        packer.append([1, 2], [True])
    packer.write_log()
    resp = list(packer.read(str(tmp_path / "packer_0.bin")))
    assert len(resp) == 5


def test_read_no_logf(packer):
    packer, tmp_path = packer
    for _ in range(5):
        packer.append([1, 2], [True])
    packer.write_log()
    resp = list(packer.read(str(tmp_path / "packer_5.bin")))
    assert resp == []


logf_regions = [(5, 0), (3, 0), (5, 0), (3, 2)]


@pytest.mark.parametrize("n, skip", logf_regions)
def test_read_logf_regions(n, skip, packer):
    packer, tmp_path = packer
    exp = []
    for i in range(10):
        floats, bools = [i, i + 1], [True if i % 2 else False]
        packer.append(floats, bools)
        exp.append([floats, bools])
    packer.write_log()
    resp = list(packer.read(str(tmp_path / "packer_0.bin"), n, skip))
    exp = exp[len(exp) - n - skip : len(exp) - skip]
    assert len(exp) == n, "Error in test"
    assert len(resp) == n
    for i, x in enumerate(resp):
        floats, bools = x
        assert floats == pytest.approx(exp[i][0])
        assert bools == exp[i][1]


def test_append_logs(packer):
    packer, tmp_path = packer
    for _ in range(5):
        packer.append([1, 2], [True])
    assert not (tmp_path / "packer_0.bin").exists(), "Already there"
    packer.append([1, 2], [True])
    assert (tmp_path / "packer_0.bin").exists()
    for _ in range(5):
        packer.append([1, 2], [True])
    assert (tmp_path / "packer_1.bin").exists()
    read = list(packer.read(str(tmp_path / "packer_1.bin")))
    assert len(read) == packer.log_size, "rotated log wrong size"


def test_append_ram(packer):
    packer, tmp_path = packer
    assert packer.buf == bytearray(packer.line_size * packer.buffer_size)
    packer.append([1, 2], [True])
    packed = packer.pack([1, 2], [True])
    assert bytes(packer.buf[: len(packed)]) == packed
    assert packer.pos == 1
    packer.append([3, 4], [False])
    assert packer.pos == 2
    unp = packer.unpack(packer.buf[len(packed) : len(packed) * 2])
    assert unp[0] == pytest.approx([3, 4])
    assert unp[1] == [False]
    for i in range(3):
        packer.append([3, 4], [False])
    assert packer.pos == 5


def test_read_ram(packer):
    packer, tmp_path = packer
    exp = []
    for i in range(4):
        floats, bools = [i, i + 1], [True if i % 2 else False]
        packer.append(floats, bools)
        exp.append([floats, bools])

    resp = list(packer.read())
    for i, x in enumerate(resp):
        floats, bools = x
        assert floats == pytest.approx(exp[i][0])
        assert bools == exp[i][1]


def test_read_file(packer):
    packer, tmp_path = packer
    exp = []
    for i in range(8):
        floats, bools = [i, i + 1], [True if i % 2 else False]
        packer.append(floats, bools)
        exp.append([floats, bools])

    resp = list(packer.read())
    assert len(resp) == 8
    for i, x in enumerate(resp):
        floats, bools = x
        assert floats == pytest.approx(exp[i][0])
        assert bools == exp[i][1]


regions = [(2, 0), (2, 2), (5, 5), (4, 10), (17, 0), (16, 1)]


@pytest.mark.parametrize("n,skip", regions)
def test_read_regions(n, skip, packer):
    packer, tmp_path = packer
    exp = []
    for i in range(17):
        floats, bools = [i, i + 1], [True if i % 2 else False]
        packer.append(floats, bools)
        exp.append([floats, bools])

    resp = list(packer.read(n=n, skip=skip))
    exp = exp[len(exp) - n - skip : len(exp) - skip]
    assert len(resp) == len(exp)
    for i, x in enumerate(resp):
        floats, bools = x
        assert floats == pytest.approx(exp[i][0])
        assert bools == exp[i][1]


def test_read_too_large(packer):
    packer, tmp_path = packer
    exp = []
    for i in range(17):
        floats, bools = [i, i + 1], [True if i % 2 else False]
        packer.append(floats, bools)
        exp.append([floats, bools])

    resp = list(packer.read(n=19))
    assert len(resp) == 17
