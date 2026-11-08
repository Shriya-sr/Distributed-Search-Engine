#!/usr/bin/env bash

# Shared Spark environment configuration.
# Values are supplied through the local .env file.

export SPARK_MASTER_HOST="${NODE3_HOST:?NODE3_HOST must be configured}"
export SPARK_MASTER_PORT="${SPARK_MASTER_PORT:-7077}"
export SPARK_MASTER_WEBUI_PORT="${SPARK_MASTER_WEBUI_PORT:-8080}"

export SPARK_LOCAL_IP="${SPARK_LOCAL_IP:-${NODE_HOST:-$NODE3_HOST}}"

export SPARK_WORKER_PORT="${SPARK_WORKER_PORT:-7078}"
export SPARK_WORKER_WEBUI_PORT="${SPARK_WORKER_WEBUI_PORT:-8081}"

export SPARK_WORKER_CORES="${SPARK_WORKER_CORES:-2}"
export SPARK_WORKER_MEMORY="${SPARK_WORKER_MEMORY:-2g}"
