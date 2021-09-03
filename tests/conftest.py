import pytest


@pytest.fixture
def equal():
    def eq(exp, resp):
        assert len(exp) == len(resp), "Different lengths"
        for eline, rline in zip(exp, resp):
            assert eline[0] == rline[0], "Index not equal"
            assert tuple(eline[3]) == rline[3], "Bools not equal"
            assert tuple(eline[2]) == rline[2], "Ints not equal"
            for i, x in enumerate(eline[1]):
                assert rline[1][i] == pytest.approx(x)
        return True

    yield eq
