# 🚗 VehicleDB Pipeline

This project contains an automated data pipeline that ingests vehicle model data from a public API and loads it into a PostgreSQL database. The pipeline helps track the latest car models, organized by vehicle type and manufacturing year.

## 🛠️ Key Features

- Extracts vehicle model data from API endpoints
- Transforms and standardizes key attributes (e.g. type, make, model year)
- Loads records into a normalized PostgreSQL database
- Supports historical tracking and delta updates

## 📊 Use Case

Ideal for analytics teams, car dealership dashboards, or mobility startups that need up-to-date vehicle specs for insights or recommendations.

## 🗂️ Stack

- Python (ETL logic)
- PostgreSQL (destination DB)
- Apache Airflow (pipeline orchestration)
- SQLAlchemy + Requests (API + DB integration)

## 🚀 Getting Started

1. Clone the repo and install dependencies:
   bash
   git clone https://github.com/yourusername/vehicleDB_pipeline.git
   cd vehicleDB_pipeline
   pip install -r requirements.txt
