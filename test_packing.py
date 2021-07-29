import packing


def test_pack_bools_all():
    resp = packing.pack_bools([True] * 8)
    assert resp == 0b11111111


def test_pack_bools_none():
    resp = packing.pack_bools([False] * 8)
    assert resp == 0


def test_pack_bools_mix():
    resp = packing.pack_bools([True, False] * 4)
    assert resp == 0b01010101
