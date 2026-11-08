# Node 3 Spark Job Runner

Node 3 submits Spark applications to the team's Spark Master. The Python
runner records each job's status, metadata, standard output, and standard
error under `artifacts/spark-jobs/`.

## Submit a Spark application

From the repository root, with the project virtual environment activated:

    ./scripts/node3/submit-job.sh path/to/application.py

Pass application arguments after the application path:

    ./scripts/node3/submit-job.sh path/to/application.py arg1 arg2

Runner options can be supplied before the application path:

    ./scripts/node3/submit-job.sh --timeout 300 --log-dir artifacts/spark-jobs path/to/application.py

The Master URL is built from `NODE3_HOST` and `SPARK_MASTER_PORT` in the
local `.env` file. `SPARK_MASTER_URL` can override it.

## Run the Worker stop/restart demonstration

Ensure the Spark Master and Node 3 Worker are running, then execute:

    ./scripts/node3/fault-tolerance-demo.sh

The script checks the Master, stops only Node 3's Worker, confirms that the
Master remains available, restarts the Worker, and waits for it to register
as ALIVE again.

The script does not stop or restart the Spark Master.

## Scope and limitation

This demonstration verifies local Worker recovery and Master availability.
It does not prove that a running distributed job survives a worker failure.
That test requires Node 2's Worker to be registered and a suitable
distributed application to be running during the failure.

## Configuration

The scripts load the repository-root `.env` file. Keep that file local and
do not commit it. Relevant settings include:

- `NODE3_HOST`
- `SPARK_HOME`
- `JAVA_HOME`
- `SPARK_MASTER_PORT`
- `SPARK_MASTER_WEBUI_PORT`
- `SPARK_WORKER_PORT`
- `SPARK_WORKER_WEBUI_PORT`
- `SPARK_WORKER_CORES`
- `SPARK_WORKER_MEMORY`
