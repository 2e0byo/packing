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


@pytest.fixture
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
