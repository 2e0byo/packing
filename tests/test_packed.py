from packing.packed import PackedRotatingLog
from devtools import debug
import pytest
import time


@pytest.fixture
def packer(tmp_path):
    p = PackedRotatingLog("log", str(tmp_path), 2, 2, 8, log_lines=10)
    yield p, tmp_path


def test_rotate_init(tmp_path):
    with (tmp_path / "log_0.bin").open("w") as f:
        f.write("")
    p = PackedRotatingLog(
        "log", str(tmp_path), 2, 2, 8, log_lines=10, incorporate=False
    )
    assert not (tmp_path / "log_0.bin").exists(), "Already written"
    assert (tmp_path / "log_1.bin").exists()
    p.append(floats=[8.7] * 2, ints=[7] * 2, bools=[False] * 8)
    assert (tmp_path / "log_0.bin").exists()
    assert (tmp_path / "log_1.bin").exists()


def test_equal(equal):
    a = [(1, (9, 8, 8), (8, 9), (True, False))]
    b = [((2, 9, 8, 8), (8, 9), (False, True))]
    assert equal(a, a)
    with pytest.raises(AssertionError):
        assert equal(a, b)
    c = [(1, (9, 9, 8), (8, 9), (True, False))]
    with pytest.raises(AssertionError):
        assert equal(a, c)


def test_logf(packer):
    packer, tmp_path = packer
    assert packer.logf() == str(tmp_path / "log_0.bin")


def test_pack_unpack(packer, equal):
    packer, tmp_path = packer
    exp = [[1, [45, 76.9], [123478, 123498], [True, False] * 4]]
    packed = packer.pack(*exp[0][1:])
    assert equal(exp, [[1, *packer.unpack(packed)]])


def test_pack_unpack_timestamp(mocker, packer, equal):
    packer, tmp_path = packer
    packer.timestamp = True
    mocked_time = mocker.patch("time.time")
    mocked_time.return_value = 1630322465.354646
    exp = [
        [1, [45, 76.9], [123478, 123498], [True, False] * 4, time.localtime(1630322465)]
    ]
    packed = packer.pack(*exp[0][1:-1])
    assert equal(exp, [[1, *packer.unpack(packed)]])
    assert mocked_time.called_once()


def test_pack_unpack_nobool(packer, equal):
    packer, tmp_path = packer
    packer.bools = 0
    exp = [(1, (45, 76.9), (12345, 6789), ())]
    packed = packer.pack(*exp[0][1:])
    assert equal(exp, [[1, *packer.unpack(packed)]])


def test_pack_unpack_nofloat(packer, equal):
    packer, tmp_path = packer
    packer.floats = 0
    exp = [(1, (), (1235, 6789), (True, False) * 4)]
    packed = packer.pack(*exp[0][1:])
    assert equal(exp, [[1, *packer.unpack(packed)]])


def test_pack_unpack_noint(packer, equal):
    packer, tmp_path = packer
    packer.ints = 0
    exp = [(1, (123.89, 12378.8), (), (True, False) * 4)]
    packed = packer.pack(*exp[0][1:])
    assert equal(exp, [[1, *packer.unpack(packed)]])


regions = [(2, 0), (2, 2), (5, 5), (4, 10), (17, 0), (15, 1)]


@pytest.mark.parametrize("n,skip", regions)
def test_read_regions(n, skip, packer, equal):
    packer, tmp_path = packer
    exp = []
    for i in range(17):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([i, floats, floats, bools])

    resp = list(packer.read(n=n, skip=skip))
    exp = exp[len(exp) - n - skip : len(exp) - skip]
    assert equal(exp, resp)


def seq():
    n = 1000

    def _seq():
        nonlocal n
        n += 60
        return n

    return _seq


@pytest.mark.parametrize("n,skip", regions)
def test_read_regions_timestamp(n, skip, packer, equal, mocker):
    packer, tmp_path = packer
    packer.timestamp = True
    mocked_time = mocker.patch("time.time", side_effect=seq())

    exp = []
    timestamp = seq()
    for i in range(17):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([i, floats, floats, bools, time.localtime(timestamp())])

    assert len(mocked_time.call_args_list) == 17
    resp = list(packer.read(n=n, skip=skip))
    exp = exp[len(exp) - n - skip : len(exp) - skip]
    assert equal(exp, resp)


@pytest.mark.parametrize("n,skip", regions)
def test_read_regions_auto_timestamp(n, skip, packer, equal, mocker):
    packer, tmp_path = packer
    packer.timestamp = False
    packer.timestamp_interval = 60
    mocked_time = mocker.patch("time.time")
    mocked_time.return_value = 1000 + 16 * 60

    exp = []
    timestamp = seq()
    for i in range(17):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([i, floats, floats, bools, time.localtime(timestamp())])

    resp = list(packer.read(n=n, skip=skip))
    assert len(mocked_time.call_args_list) == n
    exp = exp[len(exp) - n - skip : len(exp) - skip]
    assert equal(exp, resp)


def test_read_too_large(packer, equal):
    packer, tmp_path = packer
    exp = []
    for i in range(17):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([i, floats, floats, bools])
    resp = list(packer.read(n=19))
    assert equal(exp, resp)


def test_skip_too_large(packer):
    packer, tmp_path = packer
    for i in range(17):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
    resp = list(packer.read(n=2, skip=16))
    assert len(resp) == 1
    resp = list(packer.read(n=2, skip=17))
    assert len(resp) == 0


def test_incorporate(packer, equal):
    packer, tmp_path = packer
    packer.keep_logs = 2
    exp = []
    for i in range(25):
        floats, bools = [i, i + 1], [True if i % 2 else False] * 8
        packer.append(floats=floats, bools=bools, ints=floats)
        exp.append([i, floats, floats, bools])

    resp = list(packer.read(n=26))
    assert equal(exp, resp)

    results = []
    for _ in range(2):
        del packer
        packer = PackedRotatingLog(
            "log", str(tmp_path), 2, 2, 8, log_lines=10, keep_logs=2
        )

        resp = list(packer.read(n=26))
        try:
            equal(exp, resp)
            results.append(True)
        except AssertionError:
            results.append(False)

    assert all(results)
    packer.append(floats=floats, bools=bools, ints=floats)
    exp.append([i + 1, floats, floats, bools])
    resp = list(packer.read(n=26))
    assert equal(exp, resp)
