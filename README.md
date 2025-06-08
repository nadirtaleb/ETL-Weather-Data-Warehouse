# ETL Weather Data Warehouse

A containerized ETL pipeline that processes weather data from public APIs, transforms it using dbt, and visualizes it through a Streamlit dashboard.

## Project Structure

```
.
├── airflow/                 # Airflow DAGs and configurations
├── dbt/                    # dbt models and transformations
├── dashboard/              # Streamlit dashboard application
├── tests/                  # Unit and integration tests
├── docker/                 # Docker configuration files
└── scripts/               # Utility scripts
```

## Features

- Daily weather data ingestion from public APIs
- Data transformation and quality checks using dbt
- Interactive dashboard for weather trend analysis
- Automated testing and CI/CD pipeline
- Containerized development environment

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- PostgreSQL 13+

## Setup

1. Clone the repository
2. Create a `.env` file based on `.env.example`
3. Run `docker-compose up -d` to start the services
4. Access the services:
   - Airflow UI: http://localhost:8080
   - Streamlit Dashboard: http://localhost:8501
   - PostgreSQL: localhost:5432

## Development

- Airflow DAGs are located in `airflow/dags/`
- dbt models are in `dbt/models/`
- Dashboard code is in `dashboard/`
- Tests are in `tests/`

## Testing

Run tests using:
```bash
pytest tests/
```

## License

MIT 
