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


@pytest.fixture
def packer():
    p = packing.PackedReadings(2, 1)
    yield p


packtests = [
    [(12.7, 13.6), [True, False, True, False]],
    [(), [True, False] * 10],
    [(12.6, 12.7, -9.6), []],
]


@pytest.mark.parametrize("floats, bools", packtests)
def test_pack_unpack(floats, bools):
    print(floats, bools)
    packer = packing.PackedReadings(len(floats), len(bools))
    resp = packer.pack(floats, bools)
    assert resp
    rfloats, rbools = packer.unpack(resp)
    assert rbools == bools
    if floats:
        assert rfloats == pytest.approx(floats)
    else:
        assert rfloats == floats


def test_unpack(packer):
    pass
