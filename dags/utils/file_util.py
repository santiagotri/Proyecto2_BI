import pandas as pd

def cargar_datos(name):
    df = pd.read_csv("/opt/airflow/data/Datos/" + name + ".csv", sep=',', encoding = 'utf-8', index_col=False)
    return df

def guardar_datos(df, nombre):
    df.to_csv('/opt/airflow/data/Datos/' + nombre + '.csv' , encoding = 'utf-8', sep=',', index=False)
