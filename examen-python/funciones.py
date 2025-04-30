""""
funciones a utilizar en prueba ADL Python, metodo DRY
Rodrigo Arriaza
20/06/2024
"""

import pandas as pd
from sqlalchemy import create_engine

def leer_tabla(tabla, engine):
    """
    Esta función lee tablas completas desde una base de datos y devuelve un DataFrame.
    """
    return pd.read_sql(f"SELECT * FROM {tabla}", engine)


def generar_reporte(df, filas, columnas, valores, funcion_agg, sort_by=None, ascending=True):
    """
    Funcionpara generar reportes y ordenar los resultados
    """
    pivot_df = pd.pivot_table(
        df,
        index=filas,
        columns=columnas,
        values=valores,
        aggfunc=funcion_agg,
        fill_value=0
    )
    
    if sort_by:
        pivot_df = pivot_df.sort_values(by=sort_by, ascending=ascending)

    return pivot_df

    

def filtro_2005(df, columna_fecha, fecha_inicio, fecha_fin):

    """"
    funcion para filtrar por fecha '2005'. Devuelve un DF filtrado

    
    """

     # pasa la columna datetime de obj a datetime para trabajar con ellas
    if df[columna_fecha].dtype == 'object':
        df[columna_fecha] = pd.to_datetime(df[columna_fecha])
    # filtra el DF por rango de fechas
    df_filtrado = df.loc[(df[columna_fecha] >= fecha_inicio) & (df[columna_fecha] <= fecha_fin)]
    
    return df_filtrado



#Funcion para guardar DF en la base de datos
def guardar_bdatos(df, nombre_tabla, engine, if_exists='fail'):
    try:
        df.to_sql(name=nombre_tabla, con=engine, if_exists=if_exists, index=False)
        print(f'DataFrame guardado como {nombre_tabla}.')
    except Exception as e:
        print(f'Ocurrió un error: {e}')