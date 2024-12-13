from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.dummy_operator import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Responsible entity for maintaining the DAG
    'depends_on_past': False,  # Do not depend on the success of previous runs
    'email_on_failure': False,  # Disable email notifications on failure
    'email_on_retry': False,  # Disable email notifications on retry
    'retries': 1,  # Number of retry attempts
}

# Replace these placeholders with your actual project and dataset information
project_id = '<your_project_id>'  # Your GCP project ID
dataset_id = 'staging_dataset'  # Dataset for raw/staging data
transform_dataset_id = 'transform_dataset'  # Dataset for transformed data
reporting_dataset_id = 'reporting_dataset'  # Dataset for final reporting views
source_table = f'{project_id}.{dataset_id}.global_data'  # Fully qualified name of the source table

# List of countries for which individual tables and views will be created
countries = ['USA', 'India', 'Germany', 'Japan', 'France', 'Canada', 'Italy']

# DAG definition
with DAG(
    dag_id='health_data_pipeline',  # Unique identifier for the DAG
    default_args=default_args,
    description='Pipeline for loading and transforming health data from GCS to BigQuery',
    schedule_interval=None,  # No automatic schedule; manual triggering only
    start_date=datetime(2024, 1, 1),  # Start date for the DAG (adjust as needed)
    catchup=False,  # Prevent retroactive DAG runs
    tags=['bigquery', 'gcs', 'health-data'],
) as dag:

    # Task 1: Verify that the CSV file exists in the GCS bucket
    file_exists = GCSObjectExistenceSensor(
        task_id='file_exists',  # Unique task ID for checking file presence
        bucket='<your_bucket_name>',  # Replace with the name of your GCS bucket
        object='path_to_global_health_data.csv',  # Path to the CSV file in the GCS bucket
        timeout=300,  # Maximum time (in seconds) to wait for file detection
        poke_interval=30,  # Time interval (in seconds) between checks
        mode='poke',  # Task will wait until the file is detected
    )

    # Task 2: Load the CSV file from GCS into a BigQuery staging table
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',  # Unique task ID for loading data into staging
        bucket='<your_bucket_name>',  # Replace with the GCS bucket name
        source_objects=['path_to_global_health_data.csv'],  # Path to the CSV file in GCS
        destination_project_dataset_table=source_table,  # Destination table in BigQuery
        source_format='CSV',  # Format of the source file
        allow_jagged_rows=True,  # Allow inconsistent row lengths in the CSV
        ignore_unknown_values=True,  # Skip unknown values in the CSV file
        write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it exists
        skip_leading_rows=1,  # Skip the header row in the CSV file
        field_delimiter=',',  # Delimiter used in the CSV file
        autodetect=True,  # Automatically detect schema from the CSV
    )

    # Task 3: Create country-specific tables and views
    create_table_tasks = []  # List to hold table creation tasks
    create_view_tasks = []  # List to hold view creation tasks

    for country in countries:
        # Task: Create a country-specific table in the transformation dataset
        create_table_task = BigQueryInsertJobOperator(
            task_id=f'{country.lower()}_health_data',  # Unique task ID for each country
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE TABLE `{project_id}.{transform_dataset_id}.{country.lower()}_health_data` AS
                        SELECT *
                        FROM `{source_table}`
                        WHERE country = '{country}'
                    """,
                    "useLegacySql": False,  # Use standard SQL (not legacy SQL)
                }
            },
        )

        # Task: Create a country-specific view in the reporting dataset
        create_view_task = BigQueryInsertJobOperator(
            task_id=f'{country.lower()}_view',  # Unique task ID for each country view
            configuration={
                "query": {
                    "query": f"""
                        CREATE OR REPLACE VIEW `{project_id}.{reporting_dataset_id}.{country.lower()}_view` AS
                        SELECT 
                            `Year` AS `year`, 
                            `Disease Name` AS `disease_name`, 
                            `Disease Category` AS `disease_category`, 
                            `Prevalence Rate` AS `prevalence_rate`, 
                            `Incidence Rate` AS `incidence_rate`
                        FROM `{project_id}.{transform_dataset_id}.{country.lower()}_health_data`
                        WHERE `Availability of Vaccines Treatment` = FALSE
                    """,
                    "useLegacySql": False,  # Use standard SQL syntax
                }
            },
        )

        # Set task dependencies: Load -> Table Creation -> View Creation
        create_table_task.set_upstream(load_csv_to_bigquery)
        create_view_task.set_upstream(create_table_task)

        # Add tasks to respective lists for later dependencies
        create_table_tasks.append(create_table_task)
        create_view_tasks.append(create_view_task)

    # Final task: Signal successful completion of the DAG
    success_task = DummyOperator(
        task_id='success_task',  # Unique task ID for success marker
    )

    # Set dependencies: All view creation tasks must complete before success task
    for create_table_task, create_view_task in zip(create_table_tasks, create_view_tasks):
        create_table_task >> create_view_task >> success_task

    # Define the overall task flow
    file_exists >> load_csv_to_bigquery
