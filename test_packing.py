import packing
import pytest


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
    for _ in range(2):
        floats, bools = [1, 2], [True]
        packer.append(floats, bools)
        exp.append([floats, bools])
        floats, bools = [3, 4], [False]
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
    for _ in range(4):
        floats, bools = [1, 2], [True]
        packer.append(floats, bools)
        exp.append([floats, bools])
        floats, bools = [3, 4], [False]
        packer.append(floats, bools)
        exp.append([floats, bools])

    resp = list(packer.read())
    for i, x in enumerate(resp):
        floats, bools = x
        assert floats == pytest.approx(exp[i][0])
        assert bools == exp[i][1]
