# Crypto Pipeline - GCP Edition

A production-ready data engineering pipeline that processes real-time cryptocurrency trading data using Google Cloud Platform services. Engineered to handle **50M+ daily transactions** with robust schema evolution and infrastructure-as-code deployment.

## Architecture Overview

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Crypto APIs    │────▶│   Pub/Sub       │────▶│   Dataproc      │────▶│   BigQuery      │
│  (Data Source)  │     │   (Ingestion)   │     │ (Spark Stream)  │     │  (Warehouse)    │
└─────────────────┘     └─────────────────┘     └─────────────────┘     └─────────────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  Cloud Storage  │
                                                │  (Checkpoints)  │
                                                └─────────────────┘

┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                           Cloud Composer (Airflow Orchestration)                         │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## Key Features

- **Real-time Streaming**: Pub/Sub + Dataproc Spark Streaming for low-latency processing
- **50M+ Daily Transactions**: Horizontally scalable architecture
- **Schema Evolution**: Automatic schema detection and BigQuery schema updates
- **Infrastructure as Code**: Complete Terraform modules for reproducible deployments
- **Airflow Orchestration**: Cloud Composer DAGs for workflow management
- **Multi-environment**: Dev/Prod environment separation

## Technology Stack

| Component | Technology |
|-----------|------------|
| Message Queue | Google Cloud Pub/Sub |
| Stream Processing | Dataproc + Apache Spark Streaming |
| Data Warehouse | BigQuery |
| Orchestration | Cloud Composer (Apache Airflow) |
| Object Storage | Cloud Storage |
| Infrastructure | Terraform |
| Containerization | Docker |
| Language | Python 3.11+ |

## Project Structure

```
crypto-pipeline-automated/
├── airflow/
│   ├── dags/                    # Airflow DAG definitions
│   └── plugins/                 # Custom Airflow plugins
├── docker/
│   ├── Dockerfile.spark         # Spark job container
│   └── Dockerfile.publisher     # Pub/Sub publisher container
├── scripts/
│   ├── deploy.sh                # Deployment automation
│   ├── submit_spark_job.sh      # Dataproc job submission
│   └── setup_local.sh           # Local development setup
├── src/
│   ├── streaming/               # Spark Streaming jobs
│   ├── schemas/                 # Schema definitions & evolution
│   ├── bigquery/                # BigQuery utilities
│   └── utils/                   # Shared utilities
├── terraform/
│   ├── modules/                 # Reusable Terraform modules
│   │   ├── pubsub/
│   │   ├── dataproc/
│   │   ├── bigquery/
│   │   ├── composer/
│   │   └── networking/
│   └── environments/            # Environment-specific configs
│       ├── dev/
│       └── prod/
├── requirements.txt
├── requirements-dev.txt
├── pyproject.toml
└── .env.example
```

## Quick Start

### Prerequisites

- Google Cloud SDK installed and configured
- Terraform >= 1.5.0
- Python >= 3.11
- Docker (optional, for local testing)

### 1. Clone and Setup

```bash
git clone <repository-url>
cd crypto-pipeline-automated
git checkout gcp-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate   # Windows

pip install -r requirements.txt
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your GCP project details
```

### 3. Deploy Infrastructure

```bash
cd terraform/environments/dev
terraform init
terraform plan
terraform apply
```

### 4. Start Pipeline

```bash
# Submit Spark Streaming job to Dataproc
./scripts/submit_spark_job.sh dev

# Deploy Airflow DAGs (handled by Cloud Composer)
gsutil -m cp -r airflow/dags/* gs://${COMPOSER_BUCKET}/dags/
```

## Configuration

### Environment Variables

| Variable | Description |
|----------|-------------|
| `GCP_PROJECT_ID` | Google Cloud Project ID |
| `GCP_REGION` | Primary region (e.g., us-central1) |
| `PUBSUB_TOPIC` | Pub/Sub topic for crypto data |
| `BIGQUERY_DATASET` | BigQuery dataset name |
| `DATAPROC_CLUSTER` | Dataproc cluster name |
| `CHECKPOINT_BUCKET` | GCS bucket for Spark checkpoints |

## Schema Evolution

The pipeline supports automatic schema evolution:

1. **Backward Compatible**: New optional fields are added automatically
2. **Schema Registry**: Schema versions tracked in BigQuery metadata table
3. **Dead Letter Queue**: Invalid records sent to DLQ topic for investigation

## Monitoring

- **Cloud Monitoring**: Dashboards for pipeline metrics
- **Cloud Logging**: Centralized log aggregation
- **Alerting**: PagerDuty/Slack integration via Cloud Monitoring

## License

MIT License
