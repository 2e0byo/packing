import packing

print(dir(packing))
from packing.util import pack_bools, unpack_bools


def test_pack_bools_all():
    resp = pack_bools([True] * 8)
    assert resp == 0b11111111
    assert unpack_bools(resp) == [True] * 8


def test_pack_bools_none():
    resp = pack_bools([False] * 8)
    assert resp == 0
    assert unpack_bools(resp) == [False] * 8


def test_pack_bools_mix():
    resp = pack_bools([True, False] * 4)
    assert resp == 0b01010101
    assert unpack_bools(resp) == [True, False] * 4
