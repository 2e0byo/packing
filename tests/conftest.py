import pytest


@pytest.fixture
def equal():
    def eq(exp, resp):
        assert len(exp) == len(resp), "Different lengths"
        for eline, rline in zip(exp, resp):
            print(eline)
            assert tuple(eline[2]) == rline[2], "Bools not equal"
            assert tuple(eline[1]) == rline[1], "Ints not equal"
            for i, x in enumerate(eline[0]):
                assert rline[0][i] == pytest.approx(x)
        return True

    yield eq
