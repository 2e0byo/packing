from packing.packed import PackedRotatingLog
from devtools import debug
import pytest


@pytest.fixture
def packer(tmp_path):
    p = PackedRotatingLog("log", str(tmp_path), 2, 2, 8, log_lines=10)
    yield p, tmp_path


def test_equal(equal):
    a = [((9, 8, 8), (8, 9), (True, False))]
    b = [((9, 8, 8), (8, 9), (False, True))]
    assert equal(a, a)
    with pytest.raises(AssertionError):
        assert equal(a, b)
    c = [((9, 9, 8), (8, 9), (True, False))]
    with pytest.raises(AssertionError):
        assert equal(a, c)


def test_pack_unpack(packer, equal):
    packer, tmp_path = packer
    exp = [[[45, 76.9], [123478, 123498], [True, False] * 4]]
    packed = packer.pack(*exp[0])
    assert equal(exp, [packer.unpack(packed)])


def test_pack_unpack_nobool(packer, equal):
    packer, tmp_path = packer
    packer.bools = 0
    exp = [((45, 76.9), (12345, 6789), ())]
    packed = packer.pack(*exp[0])
    assert equal(exp, [packer.unpack(packed)])


def test_pack_unpack_nofloat(packer, equal):
    packer, tmp_path = packer
    packer.floats = 0
    exp = [((), (1235, 6789), (True, False) * 4)]
    packed = packer.pack(*exp[0])
    assert equal(exp, [packer.unpack(packed)])


def test_pack_unpack_noint(packer, equal):
    packer, tmp_path = packer
    packer.ints = 0
    exp = [((123.89, 12378.8), (), (True, False) * 4)]
    packed = packer.pack(*exp[0])
    assert equal(exp, [packer.unpack(packed)])


regions = [(2, 0), (2, 2), (5, 5), (4, 10), (17, 0), (15, 1)]


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
    assert len(resp) == len(exp)
    assert equal(exp, resp)
