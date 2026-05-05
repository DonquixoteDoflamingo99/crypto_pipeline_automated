#!/bin/bash
#
# Local development environment setup
# Usage: ./scripts/setup_local.sh
#

set -euo pipefail

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

main() {
    cd "$PROJECT_ROOT"

    log_info "Setting up local development environment..."

    # Create virtual environment
    if [[ ! -d "venv" ]]; then
        log_info "Creating virtual environment..."
        python3 -m venv venv
    fi

    # Activate virtual environment
    log_info "Activating virtual environment..."
    source venv/bin/activate

    # Upgrade pip
    log_info "Upgrading pip..."
    pip install --upgrade pip

    # Install dependencies
    log_info "Installing dependencies..."
    pip install -r requirements.txt
    pip install -r requirements-dev.txt

    # Create .env file if it doesn't exist
    if [[ ! -f ".env" ]]; then
        log_info "Creating .env file from template..."
        cp .env.example .env
        log_warn "Please edit .env with your configuration"
    fi

    # Install pre-commit hooks
    if command -v pre-commit &> /dev/null; then
        log_info "Installing pre-commit hooks..."
        pre-commit install
    fi

    # Verify GCP authentication
    if command -v gcloud &> /dev/null; then
        if gcloud auth application-default print-access-token &> /dev/null; then
            log_info "GCP authentication: OK"
        else
            log_warn "GCP not authenticated. Run: gcloud auth application-default login"
        fi
    else
        log_warn "gcloud CLI not installed"
    fi

    log_info "Setup complete!"
    log_info ""
    log_info "To activate the environment:"
    log_info "  source venv/bin/activate"
    log_info ""
    log_info "To run tests:"
    log_info "  pytest"
}

main "$@"
