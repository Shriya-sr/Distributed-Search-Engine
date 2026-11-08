import json
import os

import pytest

from distributed_search.jobs.runner import run_spark_job


def make_executable(tmp_path, name, content):
    executable = tmp_path / name
    executable.write_text(content)
    executable.chmod(0o755)
    return executable


def make_app(tmp_path):
    app = tmp_path / "example_job.py"
    app.write_text("print('example job')\n")
    return app


def test_successful_job_is_recorded(tmp_path):
    executable = make_executable(
        tmp_path,
        "fake-spark-submit",
        "#!/bin/sh\nprintf 'job completed\\n'\nexit 0\n",
    )

    result = run_spark_job(
        app=make_app(tmp_path),
        master="spark://localhost:7077",
        spark_submit=str(executable),
        log_dir=tmp_path / "logs",
    )

    assert result.status == "SUCCEEDED"
    assert result.return_code == 0

    metadata = json.loads(
        (tmp_path / "logs" / f"{result.job_id}.json").read_text()
    )
    assert metadata["status"] == "SUCCEEDED"
    assert "job completed" in open(result.stdout_path).read()


def test_failed_job_is_recorded(tmp_path):
    executable = make_executable(
        tmp_path,
        "fake-spark-submit",
        "#!/bin/sh\nprintf 'job failed\\n' >&2\nexit 2\n",
    )

    result = run_spark_job(
        app=make_app(tmp_path),
        master="spark://localhost:7077",
        spark_submit=str(executable),
        log_dir=tmp_path / "logs",
    )

    assert result.status == "FAILED"
    assert result.return_code == 2
    assert "job failed" in open(result.stderr_path).read()


def test_timed_out_job_is_recorded(tmp_path):
    executable = make_executable(
        tmp_path,
        "fake-spark-submit",
        "#!/bin/sh\nsleep 2\n",
    )

    result = run_spark_job(
        app=make_app(tmp_path),
        master="spark://localhost:7077",
        spark_submit=str(executable),
        log_dir=tmp_path / "logs",
        timeout=0.1,
    )

    assert result.status == "TIMED_OUT"
    assert "timeout" in open(result.stderr_path).read().lower()


def test_missing_application_is_rejected(tmp_path):
    executable = make_executable(
        tmp_path,
        "fake-spark-submit",
        "#!/bin/sh\nexit 0\n",
    )

    with pytest.raises(FileNotFoundError):
        run_spark_job(
            app=tmp_path / "missing.py",
            master="spark://localhost:7077",
            spark_submit=str(executable),
            log_dir=tmp_path / "logs",
        )


def test_empty_master_is_rejected(tmp_path):
    with pytest.raises(ValueError):
        run_spark_job(
            app=make_app(tmp_path),
            master="",
            spark_submit="/bin/true",
            log_dir=tmp_path / "logs",
        )
