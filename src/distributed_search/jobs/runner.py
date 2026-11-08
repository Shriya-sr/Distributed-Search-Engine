"""Submit Spark applications and persist execution metadata."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import uuid

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


@dataclass
class JobResult:
    job_id: str
    status: str
    return_code: int | None
    metadata_path: str
    stdout_path: str
    stderr_path: str


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _spark_submit_path(explicit_path: str | None = None) -> str:
    """Resolve the spark-submit executable."""
    if explicit_path:
        return explicit_path

    configured = os.environ.get("SPARK_SUBMIT")
    if configured:
        return configured

    spark_home = os.environ.get("SPARK_HOME")
    if spark_home:
        candidate = Path(spark_home) / "bin" / "spark-submit"
        if candidate.is_file():
            return str(candidate)

    return shutil.which("spark-submit") or "spark-submit"


def run_spark_job(
    app: str | Path,
    master: str,
    app_args: Sequence[str] = (),
    conf: dict[str, str] | None = None,
    spark_submit: str | None = None,
    log_dir: str | Path = "artifacts/spark-jobs",
    timeout: float | None = None,
) -> JobResult:
    """Submit a Spark application and record its result.

    The function returns a JobResult for successful, failed and timed-out
    submissions. Invalid input raises ValueError or FileNotFoundError.
    """
    app_path = Path(app).expanduser().resolve()

    if not app_path.is_file():
        raise FileNotFoundError(f"Spark application not found: {app_path}")

    if not master.strip():
        raise ValueError("Spark master URL cannot be empty")

    if timeout is not None and timeout <= 0:
        raise ValueError("timeout must be greater than zero")

    job_id = uuid.uuid4().hex
    output_dir = Path(log_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    metadata_path = output_dir / f"{job_id}.json"
    stdout_path = output_dir / f"{job_id}.stdout.log"
    stderr_path = output_dir / f"{job_id}.stderr.log"

    command = [
        _spark_submit_path(spark_submit),
        "--master",
        master,
    ]

    for key, value in sorted((conf or {}).items()):
        command.extend(["--conf", f"{key}={value}"])

    command.append(str(app_path))
    command.extend(str(arg) for arg in app_args)

    started_at = _now()
    start_clock = datetime.now(timezone.utc)

    status = "FAILED"
    return_code: int | None = None
    stdout = ""
    stderr = ""

    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=timeout,
        )
        stdout = completed.stdout
        stderr = completed.stderr
        return_code = completed.returncode
        status = "SUCCEEDED" if return_code == 0 else "FAILED"

    except subprocess.TimeoutExpired as exc:
        stdout = _text(exc.stdout)
        stderr = _text(exc.stderr)
        stderr += "\nSpark job exceeded its configured timeout."
        status = "TIMED_OUT"

    except OSError as exc:
        stderr = str(exc)
        status = "FAILED"
        return_code = 127

    finished_at = _now()
    duration = (
        datetime.now(timezone.utc) - start_clock
    ).total_seconds()

    stdout_path.write_text(stdout, encoding="utf-8")
    stderr_path.write_text(stderr, encoding="utf-8")

    metadata = {
        "job_id": job_id,
        "application": str(app_path),
        "master": master,
        "command": command,
        "status": status,
        "return_code": return_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_seconds": duration,
        "stdout_path": str(stdout_path),
        "stderr_path": str(stderr_path),
    }

    metadata_path.write_text(
        json.dumps(metadata, indent=2) + "\n",
        encoding="utf-8",
    )

    return JobResult(
        job_id=job_id,
        status=status,
        return_code=return_code,
        metadata_path=str(metadata_path),
        stdout_path=str(stdout_path),
        stderr_path=str(stderr_path),
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Submit and track a Spark application from Node 3."
    )
    parser.add_argument("--master", required=True)
    parser.add_argument("--spark-submit")
    parser.add_argument("--log-dir", default="artifacts/spark-jobs")
    parser.add_argument("--timeout", type=float)
    parser.add_argument(
        "--conf",
        action="append",
        default=[],
        metavar="KEY=VALUE",
    )
    parser.add_argument("app")
    parser.add_argument("app_args", nargs=argparse.REMAINDER)

    args = parser.parse_args()

    configuration = {}
    for item in args.conf:
        if "=" not in item:
            parser.error(f"Invalid --conf value: {item}")
        key, value = item.split("=", 1)
        configuration[key] = value

    app_args = args.app_args
    if app_args and app_args[0] == "--":
        app_args = app_args[1:]

    try:
        result = run_spark_job(
            app=args.app,
            master=args.master,
            app_args=app_args,
            conf=configuration,
            spark_submit=args.spark_submit,
            log_dir=args.log_dir,
            timeout=args.timeout,
        )
    except (ValueError, FileNotFoundError) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    print(json.dumps(asdict(result), indent=2))
    return 0 if result.status == "SUCCEEDED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
