"""Small helper used by the local starter tests."""

def version_dir(version: int) -> str:
    """Return a local serving directory name for a nonnegative version."""
    if not isinstance(version, int) or isinstance(version, bool) or version < 0:
        raise ValueError("version must be a nonnegative integer")
    return f"v{version}"
