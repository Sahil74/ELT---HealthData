# Secure and Efficient Analysis of Global Health Data

## Introduction

This project demonstrates the development of a robust and scalable data pipeline designed to securely process, transform, and analyze global health data. The solution addresses challenges such as managing over one million records, ensuring data confidentiality, and enabling country-specific insights into diseases lacking available treatments or vaccines.

The workflow leverages Google Cloud Platform (GCP) services and Apache Airflow to efficiently manage the data lifecycle, from ingestion to reporting. By creating country-specific tables and views, the pipeline ensures secure access and meaningful analysis for healthcare stakeholders.


### ELT Process Diagram
![Alt text](/ELT_Process.png)




## Features

- Data Ingestion: Loads a large global health dataset from Google Cloud Storage (GCS) into BigQuery.
- Data Transformation: Generates country-specific tables to segregate data securely.
- Reporting Views: Builds aggregated views focusing on diseases with no treatments or vaccines available.
- Security: Implements access restrictions to ensure data confidentiality.


### Airflow DAG Graph
![Alt text](/Dag_graph.png)

## Documentation

- The detailed report for this project, including technical implementation and insights, is available as a PDF:

**Download the Report**
- [View the PDF](/Health_data_Report.pdf)
