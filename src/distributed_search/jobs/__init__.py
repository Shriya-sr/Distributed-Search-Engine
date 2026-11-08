"""Spark job submission and tracking utilities."""

__all__ = ["JobResult", "run_spark_job"]


def __getattr__(name):
    if name in __all__:
        from .runner import JobResult, run_spark_job
        return {
            "JobResult": JobResult,
            "run_spark_job": run_spark_job,
        }[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
