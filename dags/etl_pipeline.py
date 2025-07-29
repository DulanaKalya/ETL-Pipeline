from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json


# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    default_args=default_args,
    description='ETL pipeline for NASA APOD data',
    schedule='@daily',
    catchup=False,
    tags=['nasa', 'etl', 'postgres']
) as dag:
    
    # Step 1: Create the table if it doesn't exist
    @task
    def create_table():
        # Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id="my_postgres_connection")

        # SQL query to create the table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(500),
            explanation TEXT,
            url TEXT,
            date DATE UNIQUE,
            media_type VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        # Execute the table creation query
        postgres_hook.run(create_table_query)
        print("Table creation completed")

    # Step 2: Extract NASA API Data (APOD)
    extract_apod = HttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',
        endpoint='planetary/apod',
        method='GET',
        headers={'Content-Type': 'application/json'},
        # Use query parameters instead of data for GET request
        # Alternative: hardcode API key or use Airflow Variables
        extra_options={'params': {'api_key': '{{ conn.nasa_api.extra_dejson.api_key }}'}},  # Replace with your API key
        response_filter=lambda response: response.json(),
        log_response=True,
    )

    # Step 3: Transform the data
    @task
    def transform_apod_data(response):
        print(f"Raw response: {response}")
        
        # Handle the case where response might be nested
        if isinstance(response, list) and len(response) > 0:
            data = response[0]
        else:
            data = response
            
        apod_data = {
            'title': data.get('title', ''),
            'explanation': data.get('explanation', ''),
            'url': data.get('url', ''),
            'date': data.get('date', ''),
            'media_type': data.get('media_type', '')
        }
        
        print(f"Transformed data: {apod_data}")
        return apod_data

    # Step 4: Load the data into PostgreSQL
    @task
    def load_data_to_postgres(apod_data):
        # Initialize the PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id='my_postgres_connection')

        # Use INSERT ... ON CONFLICT to handle duplicates
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (date) DO UPDATE SET
            title = EXCLUDED.title,
            explanation = EXCLUDED.explanation,
            url = EXCLUDED.url,
            media_type = EXCLUDED.media_type;
        """

        # Execute the SQL Query
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))
        print(f"Data loaded successfully for date: {apod_data['date']}")

    # Step 5: Define the task dependencies
    create_table_task = create_table()
    
    # Chain the tasks properly
    create_table_task >> extract_apod
    
    # Transform the extracted data
    transformed_data = transform_apod_data(extract_apod.output)
    
    # Set up the dependency chain
    extract_apod >> transformed_data
    
    # Load the transformed data
    load_task = load_data_to_postgres(transformed_data)
    
    # Final dependency
    transformed_data >> load_task