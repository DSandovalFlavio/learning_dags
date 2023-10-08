# other imports
import json
import pathlib
import requests
import requests.exceptions as requests_exceptions

# airflow imports
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id="download_processing_rocket_launcher",
    start_date=days_ago(14),
    schedule_interval=None,
    tags=['dsandovalflavio', 'learning_dags']
    ) as dag:
    
    download_rocket_launches = BashOperator(
        task_id="download_rocket_launches",
        bash_command="curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'"
        )
    
    @task(task_id="process_rocket_launches")
    def process_rocket_launches():
        pathlib.Path("/home/dsandovalflavio/Learning/learning_dags/data/rocket_launches/processed").mkdir(parents=True, exist_ok=True)
        with open("/tmp/launches.json", "r") as f:
            launches = json.load(f)
            image_urls = [launch["image"] for launch in launches["results"]]
            for image_url in image_urls:
                try:
                    response = requests.get(image_url)
                    image_filename = image_url.split("/")[-1]
                    target_file = f"/home/dsandovalflavio/Learning/learning_dags/data/rocket_launches/processed/{image_filename}"
                    with open(target_file, "wb") as f:
                        f.write(response.content)
                    print(f"Downloaded {image_url} to {target_file}")
                except requests_exceptions.MissingSchema:
                    print(f"{image_url} appears to be an invalid URL.")
                except requests_exceptions.ConnectionError:
                    print(f"Could not connect to {image_url}.")
    get_pictures = process_rocket_launches()
    
    notify = BashOperator(
        task_id="notify",
        bash_command='echo "There are now $(ls /home/dsandovalflavio/Learning/learning_dags/data/rocket_launches/processed/ | wc -l) processed launch images."',
    )
    
    download_rocket_launches >> get_pictures >> notify