# Medallion Architecture with Data Engineering Project

This project demonstrates an end-to-end data engineering pipeline that extracts data from multiple sources, stores raw data in the Bronze layer, cleans and transforms data in the Silver layer, and prepares analytics-ready datasets in the Gold layer for business insights and reporting.

## Data Architecture 

The data architecture for this project follows Medallion Architecture **Brronze**, **Silver**, and **Gold** Layers: 
![Data Architecture](architecture.png)

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

## Project Requirements

1. Kaggle
2. OS Module
3. SparkSession
4. duckdb

## Repository Structure
```
DEMovies/
|
|datasets/
|scripts/
|  |---bronze
|  |---silver
|  |---gold
|README.md
|.gitignore
```
---

