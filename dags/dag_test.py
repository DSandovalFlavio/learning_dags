from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define los argumentos del DAG
default_args = {
    'owner': 'usuario',
    'start_date': datetime(2023, 10, 8),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Crea un objeto DAG
dag = DAG(
    'mi_dag_de_testeo',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Frecuencia de ejecución (diaria en este caso)
    catchup=False,  # Evita la ejecución en retroceso de tareas
    description='Un DAG de prueba para Airflow',
    max_active_runs=1,  # Número máximo de instancias en ejecución del DAG
    tags=['test', 'ejemplo'],
)

# Función que se ejecutará como tarea
def imprimir_mensaje():
    print("¡Hola, este es un DAG de prueba!")

# Define las tareas
tarea_imprimir = PythonOperator(
    task_id='imprimir_mensaje',
    python_callable=imprimir_mensaje,
    dag=dag,
)

# Agrega una segunda tarea que simplemente duerme durante 5 segundos
from time import sleep

def dormir():
    sleep(5)

tarea_dormir = PythonOperator(
    task_id='dormir',
    python_callable=dormir,
    dag=dag,
)

# Define la secuencia de tareas
tarea_imprimir >> tarea_dormir

if __name__ == "__main__":
    dag.cli()
