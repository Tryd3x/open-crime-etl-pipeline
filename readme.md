# Chicago Crime Data Pipeline

A data engineering project to build a cost-aware, resilient ETL pipeline that ingests, processes, and manages Chicago crime data from the Socrata Open Data API.

---

## Project Overview

This pipeline orchestrates the extraction of crime data from [chicago.data.gov](https://data.cityofchicago.org/) via the Socrata API, loading raw data into S3 and transforming/storing it in dual database environments (Snowflake and PostgreSQL). The design prioritizes cost efficiency, reliability, and scalability.

---

## Features

- **Data Ingestion**: Weekly incremental and full loads from Socrata API using Apache Airflow  
- **Storage Strategy**:  
  - Raw data stored in AWS S3 as the landing zone  
  - Dual database setup: Snowflake (primary during trial) and Dockerized PostgreSQL (fallback for cost control)  
- **Pipeline Metadata**: Tracks ingestion status, supports fallback and task recovery  
- **Reliability**: Fallback logic prevents duplicate data loads and resumes failed tasks  
- **Monitoring & Visualization** (planned): API endpoints and Streamlit dashboards for real-time performance and data visualization  
- **Testing & CI/CD** (planned): Unit and integration tests for pipeline components, with automated deployment workflows

---

## Tech Stack

- Apache Airflow  
- Socrata Open Data API  
- AWS S3  
- Snowflake  
- PostgreSQL (Dockerized)  
- Streamlit (planned)  
- FastAPI (planned)  
- Python

---

## Development Roadmap

- Complete API endpoints for `/metrics` and `/query`  
- Build Streamlit dashboard for pipeline performance and crime data visualization  
- Implement CI/CD pipelines with automated testing  
- Optimize incremental loading and fallback mechanisms  

---

## Contributing

Contributions and feedback are welcome! Please open issues or submit pull requests for enhancements.

---

## License

[MIT License](LICENSE)
