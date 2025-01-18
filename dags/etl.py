from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


# Define the Dag

with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Create the table in Postgres if it does not exist
    @task
    def create_table():
        # inititalize the postgres hook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        # SQL to create the table
        create_table_query=""" 
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """
        postgres_hook.run(create_table_query)

    # Extract the NASA API data (APOD - Astronomy Picture of the Day)
    # https://api.nasa.gov/
    
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api', # Connection ID will be asigned in Airflow
        endpoint='planetary/apod', # NASA APOD endpoint
        method='GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key }}"}, # Will use the API Key from the connection in Airflow
        response_filter=lambda response:response.json(), # Convert the response to json
    )

    # Trasform the data by selecting the data to be saved

    @task
    def transform_apod_date(response):
        # Attempt to extract the fields we want, if not there return blank
        apod_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')
        }
        return apod_data

    # Load the data into Postgres

    @task
    def load_data_to_postgres(apod_data):
        # Initialize the postgres hook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        # SQL to insert the data
        insert_query = """ 
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """
        postgres_hook.run(insert_query, parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))


    # Verify the data with DBViewer

    # Define the task dependencies
    # First extract
    create_table() >> extract_apod
    api_response=extract_apod.output

    # Then transforn
    transformed_data=transform_apod_date(api_response)

    # Lastly load
    load_data_to_postgres(transformed_data)