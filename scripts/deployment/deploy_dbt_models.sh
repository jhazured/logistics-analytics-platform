#!/usr/bin/env bash
set -euo pipefail

export DBT_PROFILES_DIR="${DBT_PROFILES_DIR:-$(pwd)/dbt}"

dbt deps --project-dir dbt
dbt build --project-dir dbt --target "${DBT_TARGET:-dev}" --full-refresh
