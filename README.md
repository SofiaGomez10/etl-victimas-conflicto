# ETL Pipeline for Victims of Armed Conflict in Colombia

This project implements an automated ETL (Extract, Transform, Load) pipeline for processing and analyzing data related to victims of armed conflicts in Colombia. The pipeline integrates multiple data sources, performs data quality validation using Great Expectations, and loads the consolidated data into a MySQL data warehouse for further analysis and visualization with Metabase.

## Project Overview

The ETL pipeline is orchestrated using Apache Airflow and containerized with Docker. It processes data from three sources:
- **Source 1**: Cali victims data (from SQLite)
- **Source 2**: Santander victims data (CSV)
- **Source 3**: API data from datos.gov.co

The workflow includes ingestion, transformation, concatenation, consolidation, validation, and loading into MySQL.

## Architecture

```
Data Sources → Airflow DAG → Transformations → Great Expectations → MySQL DW → Metabase
```
<img width="606" height="326" alt="image" src="https://github.com/user-attachments/assets/b38abf62-2ea7-41bb-9cec-5812e91c383a" />


## Prerequisites

- Docker and Docker Compose
- At least 4GB RAM available for containers
- Git

## Setup and Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd etl-victimas-conflicto
   ```

2. **Start the services:**
   ```bash
   docker-compose up -d
   ```

   This will start:
   - Airflow webserver (http://localhost:8080)
   - Airflow scheduler
   - MySQL databases (Airflow metadata and data warehouse)
   - Metabase (http://localhost:3000)

3. **Access the interfaces:**
   - Airflow UI: http://localhost:8080 (admin/admin)
   - Metabase: http://localhost:3000

## Running the Pipeline

1. In the Airflow UI, enable the DAG `dag_armed_conflict_victims`
2. Trigger the DAG manually or wait for the scheduled run (daily at 6:00 AM UTC)

## Project Structure

```
etl-victimas-conflicto/
│
├── dags/
│   └── dag_conflict_victims.py          # Airflow DAG definition
│
├── scripts/
│   ├── ingest_source1.py                # Ingest from Cali (SQLite)
│   ├── ingest_source2.py                # Ingest from Santander (CSV)
│   ├── ingest_source3.py                # Ingest from API
│   ├── transform_source1.py             # Transform Cali data
│   ├── transform_source2.py             # Transform Santander data
│   ├── transform_source3.py             # Transform API data
│   ├── concat_sources.py                # Concatenate transformed sources
│   ├── consolidate.py                   # Final data consolidation
│   ├── validate.py                      # Great Expectations validation
│   └── load.py                          # Load to MySQL
│
├── data/
│   ├── raw/                             # Raw input data
│   └── processed/                       # Processed/transformed data
│
├── great_expectations/                  # Data validation configurations
│
├── notebooks/                           # Initial Jupyter notebooks
│   ├── eda.ipynb
│   ├── extraction_transformation.ipynb
│   └── load_visualization.ipynb
│
├── logs/                                # Airflow execution logs
│
├── docker-compose.yml                   # Docker services configuration
├── Dockerfile                           # Airflow container definition
├── requirements.txt                     # Python dependencies
└── README.md
```

## Technologies Used

- **Apache Airflow**: Workflow orchestration
- **Python**: Data processing (pandas, numpy)
- **MySQL**: Data storage
- **Great Expectations**: Data quality validation
- **Docker**: Containerization
- **Metabase**: Data visualization
- **Jupyter**: Exploratory data analysis

## Data Sources

- Cali victims data (SQLite database)
- Santander victims data (CSV file)
- Colombian government API (datos.gov.co)

## Development Notes

- The pipeline runs daily at 6:00 AM UTC
- Data validation ensures quality before loading
- Notebooks in `notebooks/` are for exploratory analysis and are not part of the automated pipeline
- All data processing is containerized for reproducibility
