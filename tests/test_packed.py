from packing.packed import PackedRotatingLog
from devtools import debug
import pytest


@pytest.fixture
def packer(tmp_path):
    p = PackedRotatingLog("log", str(tmp_path), 2, 2, 8, log_lines=10)
    yield p, tmp_path


@pytest.fixture
def equal():
    def eq(exp, resp):
        for eline, rline in zip(exp, resp):
            assert eline[2] == rline[2], "Bools not equal"
            assert eline[1] == rline[1], "Ints not equal"
            for i, x in enumerate(eline[0]):
                assert rline[0][i] == pytest.approx(x)
        return True

    yield eq


def test_equal(equal):
    a = [[[9, 8, 8], [8, 9], [True, False]]]
    b = [[[9, 8, 8], [8, 9], [False, True]]]
    assert equal(a, a)
    with pytest.raises(AssertionError):
        assert equal(a, b)
    c = [[[9, 9, 8], [8, 9], [True, False]]]
    with pytest.raises(AssertionError):
        assert equal(a, c)


def test_pack_unpack(packer, equal):
    packer, tmp_path = packer
    exp = [[[45, 76.9], [True, False] * 4]]
    packed = packer.pack(*exp[0])
    assert equal(exp, [packer.unpack(packed)])
