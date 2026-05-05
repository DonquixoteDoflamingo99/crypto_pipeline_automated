#!/bin/bash
#
# Deployment script for Crypto Pipeline
# Usage: ./scripts/deploy.sh <environment> [--apply]
#

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TERRAFORM_DIR="${PROJECT_ROOT}/terraform/environments"

# Functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    echo "Usage: $0 <environment> [--apply]"
    echo ""
    echo "Arguments:"
    echo "  environment    Target environment (dev, prod)"
    echo "  --apply        Apply changes (default: plan only)"
    echo ""
    echo "Examples:"
    echo "  $0 dev                 # Plan dev environment"
    echo "  $0 dev --apply         # Apply dev environment"
    echo "  $0 prod --apply        # Apply prod environment"
    exit 1
}

check_prerequisites() {
    log_info "Checking prerequisites..."

    # Check Terraform
    if ! command -v terraform &> /dev/null; then
        log_error "Terraform is not installed. Please install Terraform >= 1.5.0"
        exit 1
    fi

    # Check gcloud
    if ! command -v gcloud &> /dev/null; then
        log_error "Google Cloud SDK is not installed"
        exit 1
    fi

    # Check authentication
    if ! gcloud auth application-default print-access-token &> /dev/null; then
        log_warn "Not authenticated with GCP. Running 'gcloud auth application-default login'"
        gcloud auth application-default login
    fi

    log_info "Prerequisites check passed"
}

deploy_infrastructure() {
    local env=$1
    local apply=${2:-false}
    local env_dir="${TERRAFORM_DIR}/${env}"

    if [[ ! -d "$env_dir" ]]; then
        log_error "Environment directory not found: $env_dir"
        exit 1
    fi

    log_info "Deploying to ${env} environment..."

    cd "$env_dir"

    # Initialize Terraform
    log_info "Initializing Terraform..."
    terraform init -upgrade

    # Validate configuration
    log_info "Validating Terraform configuration..."
    terraform validate

    # Plan
    log_info "Planning infrastructure changes..."
    terraform plan -out=tfplan

    # Apply if requested
    if [[ "$apply" == "true" ]]; then
        log_info "Applying infrastructure changes..."
        terraform apply tfplan
        log_info "Infrastructure deployed successfully!"
    else
        log_info "Plan complete. Run with --apply to apply changes."
    fi

    # Clean up plan file
    rm -f tfplan
}

upload_spark_jobs() {
    local env=$1

    log_info "Uploading Spark jobs to GCS..."

    # Get bucket name from Terraform output
    cd "${TERRAFORM_DIR}/${env}"
    local bucket=$(terraform output -raw data_bucket_name 2>/dev/null || echo "")

    if [[ -z "$bucket" ]]; then
        log_warn "Could not get bucket name. Skipping Spark job upload."
        return
    fi

    # Package source code
    cd "$PROJECT_ROOT"
    zip -r /tmp/src.zip src/

    # Upload to GCS
    gsutil cp /tmp/src.zip "gs://${bucket}/spark-jobs/"
    gsutil cp src/streaming/spark_streaming.py "gs://${bucket}/spark-jobs/"

    log_info "Spark jobs uploaded to gs://${bucket}/spark-jobs/"
}

upload_airflow_dags() {
    local env=$1

    log_info "Uploading Airflow DAGs..."

    # Get Composer bucket from Terraform output
    cd "${TERRAFORM_DIR}/${env}"
    local dag_bucket=$(terraform output -raw composer_dag_gcs_bucket 2>/dev/null || echo "")

    if [[ -z "$dag_bucket" ]]; then
        log_warn "Could not get Composer DAG bucket. Skipping DAG upload."
        return
    fi

    # Upload DAGs
    gsutil -m cp -r "${PROJECT_ROOT}/airflow/dags/*" "${dag_bucket}/"

    log_info "DAGs uploaded to ${dag_bucket}"
}

# Main
main() {
    if [[ $# -lt 1 ]]; then
        usage
    fi

    local environment=$1
    local apply="false"

    # Parse arguments
    shift
    while [[ $# -gt 0 ]]; do
        case $1 in
            --apply)
                apply="true"
                shift
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                ;;
        esac
    done

    # Validate environment
    if [[ "$environment" != "dev" && "$environment" != "prod" ]]; then
        log_error "Invalid environment: $environment. Must be 'dev' or 'prod'"
        exit 1
    fi

    # Production confirmation
    if [[ "$environment" == "prod" && "$apply" == "true" ]]; then
        log_warn "You are about to deploy to PRODUCTION!"
        read -p "Are you sure? (yes/no): " confirm
        if [[ "$confirm" != "yes" ]]; then
            log_info "Deployment cancelled"
            exit 0
        fi
    fi

    check_prerequisites
    deploy_infrastructure "$environment" "$apply"

    if [[ "$apply" == "true" ]]; then
        upload_spark_jobs "$environment"
        upload_airflow_dags "$environment"
    fi

    log_info "Deployment complete!"
}

main "$@"
