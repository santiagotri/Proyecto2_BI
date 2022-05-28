# Utilidades de airflow
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Utilidades de python
from datetime  import datetime

# Funciones ETL
from utils.crear_tablas import crear_tablas
from utils.insert_queries import *

default_args= {
    'owner': 'Estudiante',
    'email_on_failure': False,
    'email': ['estudiante@uniandes.edu.co'],
    'start_date': datetime(2022, 5, 5) # inicio de ejecución
}

with DAG(
    "ETL",
    description='ETL',
    schedule_interval='@daily', # ejecución diaría del DAG
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1 crear las tablas en la base de datos postgres
    crear_tablas_db = PostgresOperator(
    task_id="crear_tablas_en_postgres",
    postgres_conn_id="postgres_localhost", # Nótese que es el mismo ID definido en la conexión Postgres de la interfaz de Airflow
    sql= crear_tablas()
    )

    # task: 2 poblar las tablas de dimensiones en la base de datos
    with TaskGroup('poblar_tablas') as poblar_tablas_dimensiones:

        # task: 2.1 poblar tabla city
        poblar_educacion_fecha = PostgresOperator(
            task_id="poblar_educacion_fecha",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_educacion_fecha(csv_path = "dimension_salud")
        )

        # task: 2.1 poblar tabla city
        poblar_educacion_departamento = PostgresOperator(
            task_id="poblar_educacion_departamento",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_educacion_departamento(csv_path = "dimension_salud")
        )

        # task: 2.1 poblar tabla city
        poblar_educacion_entidad = PostgresOperator(
            task_id="poblar_educacion_entidad",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_educacion_entidad(csv_path = "dimension_salud")
        )
        
        # task: 2.1 poblar tabla city
        poblar_educacion_tipo = PostgresOperator(
            task_id="poblar_educacion_tipo",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_educacion_tipo(csv_path = "dimension_salud")
        )


    # task: 3 poblar la tabla de hechos
    poblar_fact_educacion = PostgresOperator(
            task_id="construir_tabla_de_hechos_educacion",
            postgres_conn_id='postgres_localhost',
            sql=insert_query_educacion_fact(csv_path = "dimension_salud")
    )
    
    # flujo de ejecución de las tareas  
    crear_tablas_db >> poblar_tablas_dimensiones >> poblar_fact_educacion
