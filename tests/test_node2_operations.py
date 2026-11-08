from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts" / "node2"


def run_script(name, *args):
    return subprocess.run(
        ["bash", str(SCRIPTS / name), *args],
        capture_output=True,
        text=True,
        timeout=15,
    )


def test_all_node2_scripts_have_valid_bash_syntax():
    scripts = sorted(SCRIPTS.glob("*.sh"))
    assert scripts, "No Node 2 scripts found"

    for script in scripts:
        result = subprocess.run(
            ["bash", "-n", str(script)],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0, (
            f"{script.name}: {result.stderr}"
        )


def test_recovery_rejects_missing_arguments():
    result = run_script("recover-snapshot.sh")

    assert result.returncode != 0
    assert "Usage:" in result.stdout


def test_recovery_rejects_invalid_snapshot_id():
    result = run_script(
        "recover-snapshot.sh",
        "search.db.pages",
        "not-a-number",
        "view",
    )

    assert result.returncode != 0
    assert "Snapshot ID must be numeric" in result.stdout


def test_recovery_rejects_invalid_action():
    result = run_script(
        "recover-snapshot.sh",
        "search.db.pages",
        "123",
        "delete",
    )

    assert result.returncode != 0
    assert "Action must be view or rollback" in result.stdout


def test_recovery_rejects_invalid_table_name():
    result = run_script(
        "recover-snapshot.sh",
        "invalid-table",
        "123",
        "view",
    )

    assert result.returncode != 0
    assert "Usage:" in result.stdout
