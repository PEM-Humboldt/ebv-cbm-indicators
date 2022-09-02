import os #para acceder a variables del environment
import pandas as pd 
import numpy as np
import datetime as dt
from dotenv import load_dotenv
    load_dotenv()
from sqlalchemy import create_engine

def funRegistro_Diario_Lluvias():
    #cargar credenciales para conectarse a la base de datos
    db_string = os.environ['DATABASE_URL']
    db = create_engine(db_string)

    #cargar tablas monitoreo metodologia 6 y metadatos del punto
    df_registros = pd.read_sql_query('SELECT * FROM kobo_data.medicion_lluvia_registros',con=db)
    df_metadata = pd.read_sql_query('SELECT * FROM kobo_data.medicion_lluvia',con=db)
    df_codigo  = pd.read_sql_query('SELECT * FROM kobo_data.codigo_punto',con=db)

    df= pd.merge(df_registros,df_metadata,left_on="id_formulario",right_on="id",how="left")
    df= pd.merge(df,df_codigo,on="tx_codigo",how="left")

    #Crear variables de interes a partir de fecha
    df['fecha']=pd.to_datetime(df['dt_fecha_hora'])
    df['anio']= df['fecha'].dt.year
    df['dt_mes']= df['fecha'].dt.month
    df['juliano']=df['fecha'].dt.strftime("%j")

    #Crear tabla con registros diarios agrupados
    df_registro_diario_lluvia = df.groupby(['tx_codigo','tx_vereda','ct_asociacion','anio','dt_mes','fecha'],dropna=False)['nm_precipitacion'].sum().reset_index()

    #Cargar tabla de parámetros para cáculo de indicador
    iap_parametros = pd.read_sql_query('SELECT * FROM kobo_data.historico_precipitacion',con=db)
    
    #unir tabla registros diarios agrupados con parametros historicos
    registro_diario_lluvia = pd.merge(df_registro_diario_lluvia , iap_parametros[['dt_mes',"nm_p10_historico",'nm_p90_historico']], on="dt_mes", how='left')

    #renombrar para exportar
    registro_diario_lluvia = registro_diario_lluvia.rename(columns={'tx_codigo':'codigo','tx_vereda':'nombre_vereda','ct_asociacion':'asociacion','nm_precipitacion':'precipitacion','dt_mes':'mes','nm_p10_historico':'p10','nm_p90_historico':'p90'})
    print(registro_diario_lluvia)

    #reagrupar por mes para calcular indicador
    df_registro_mes_lluvia = registro_diario_lluvia.groupby(['codigo','nombre_vereda','asociacion','anio','mes'],dropna=False)['precipitacion'].sum().reset_index()

    #agregar valores historicos
    df_mes_parametros = pd.merge(df_registro_mes_lluvia,iap_parametros,left_on="mes",right_on='dt_mes', how='left')

    db.execute('TRUNCATE TABLE indicadores.registro_diario_lluvia CASCADE')
    registro_diario_lluvia.to_sql('registro_diario_lluvia', con=db, schema="indicadores", if_exists='append', index=False)