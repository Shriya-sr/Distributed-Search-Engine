import pytest
from distributed_search.common.versions import version_dir

def test_version_dir():
    assert version_dir(0) == "v0"
    assert version_dir(3) == "v3"

@pytest.mark.parametrize("bad", [-1, "1", True])
def test_reject_bad_versions(bad):
    with pytest.raises(ValueError):
        version_dir(bad)
