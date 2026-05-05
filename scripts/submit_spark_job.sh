#!/bin/bash
#
# Submit Spark Streaming job to Dataproc
# Usage: ./scripts/submit_spark_job.sh <environment> [--async]
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

usage() {
    echo "Usage: $0 <environment> [--async]"
    echo ""
    echo "Arguments:"
    echo "  environment    Target environment (dev, prod)"
    echo "  --async        Submit job asynchronously"
    exit 1
}

get_terraform_output() {
    local env=$1
    local output_name=$2
    cd "${PROJECT_ROOT}/terraform/environments/${env}"
    terraform output -raw "$output_name" 2>/dev/null || echo ""
}

main() {
    if [[ $# -lt 1 ]]; then
        usage
    fi

    local environment=$1
    local async_flag=""
    shift

    while [[ $# -gt 0 ]]; do
        case $1 in
            --async) async_flag="--async"; shift ;;
            *) log_error "Unknown option: $1"; usage ;;
        esac
    done

    # Validate environment
    if [[ "$environment" != "dev" && "$environment" != "prod" ]]; then
        log_error "Invalid environment: $environment"
        exit 1
    fi

    log_info "Getting configuration from Terraform..."

    # Get configuration
    local project_id=$(get_terraform_output "$environment" "project_id")
    local region=$(get_terraform_output "$environment" "region")
    local cluster_name=$(get_terraform_output "$environment" "dataproc_cluster_name")
    local bucket=$(get_terraform_output "$environment" "data_bucket_name")
    local pubsub_subscription=$(get_terraform_output "$environment" "pubsub_subscription_id")

    if [[ -z "$project_id" || -z "$cluster_name" ]]; then
        log_error "Could not retrieve configuration. Is infrastructure deployed?"
        exit 1
    fi

    log_info "Submitting Spark Streaming job..."
    log_info "  Project: $project_id"
    log_info "  Region: $region"
    log_info "  Cluster: $cluster_name"

    # Submit the job
    gcloud dataproc jobs submit pyspark \
        "gs://${bucket}/spark-jobs/spark_streaming.py" \
        --project="$project_id" \
        --region="$region" \
        --cluster="$cluster_name" \
        --py-files="gs://${bucket}/spark-jobs/src.zip" \
        --properties="spark.streaming.stopGracefullyOnShutdown=true,spark.sql.streaming.metricsEnabled=true" \
        --labels="environment=${environment},component=streaming" \
        $async_flag \
        -- \
        --project-id="$project_id" \
        --subscription="$pubsub_subscription"

    log_info "Job submitted successfully!"
}

main "$@"
