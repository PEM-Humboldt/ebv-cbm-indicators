import os #para acceder a variables del environment
import pandas as pd 
import numpy as np
import datetime as dt
from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine

def funIndice_Anomalia_Lluvias():
    #cargar credenciales para conectarse a la base de datos
    db_string = os.environ['DATABASE_URL']
    db = create_engine(db_string)

    #cargar tablas monitoreo metodologia 6 y metadatos del punto
    registro_diario_lluvia = pd.read_sql_query('SELECT * FROM indicadores.registro_diario_lluvia',con=db)
    print(registro_diario_lluvia)

    #reagrupar por mes para calcular indicador
    df_registro_mes_lluvia = registro_diario_lluvia.groupby(['codigo','nombre_vereda','asociacion','anio','mes'],dropna=False)['precipitacion'].sum().reset_index()

    #Cargar tabla de parámetros para cáculo de indicador
    iap_parametros = pd.read_sql_query('SELECT * FROM kobo_data.historico_precipitacion',con=db)

    #agregar valores historicos
    df_mes_parametros = pd.merge(df_registro_mes_lluvia,iap_parametros,left_on="mes",right_on='dt_mes', how='left')

    #Calcular indicador de anomalia para valores mayores y menores al historico de precipitacion
    df_mes_parametros['iap_p'] = df_mes_parametros['nm_fe_p']*(df_mes_parametros['precipitacion']-df_mes_parametros['nm_mediana_precipitacion_historico'])/(df_mes_parametros['nm_promedio_p90_historico']-df_mes_parametros['nm_mediana_precipitacion_historico'])
    df_mes_parametros['iap_n'] = df_mes_parametros['nm_fe_n']*(df_mes_parametros['precipitacion']-df_mes_parametros['nm_mediana_precipitacion_historico'])/(df_mes_parametros['nm_promedio_p10_historico']-df_mes_parametros['nm_mediana_precipitacion_historico'])

    #Seleccionar el calculo correspondiente segun la logica de relacion con precipitacion historica
    df_mes_parametros['iap'] = df_mes_parametros.iap_p
    mask = df_mes_parametros.precipitacion < df_mes_parametros.nm_mediana_precipitacion_historico

    df_mes_parametros.loc[mask,'iap'] = df_mes_parametros.loc[mask,'iap_n']

    #definir puntos de corte segun van Rooy (1965)
    puntos_corte=[-np.inf,-3,-2,-1,-0.5,0.49999999,0.99999999,1.99999999,2.99999999,np.inf] #verificar si dan los limites cerrados y abiertos
    rangos=["Extremadamente seco","Muy seco","Moderadamente seco","Un poco seco","Aproximadamente normal","Un poco húmedo","Moderadamente húmedo","Muy húmedo","Extremadamente húmedo"]
    df_mes_parametros['iap_label'] = pd.cut(df_mes_parametros['iap'], puntos_corte , labels=rangos)

    #Seleccionar columnas para tabla a exportar
    indice_anomalia_lluvias = df_mes_parametros[['asociacion','anio','mes','iap','iap_label']] #tabla a exportar

    db.execute('TRUNCATE TABLE indicadores.indice_anomalia_lluvias CASCADE')
    indice_anomalia_lluvias.to_sql('indice_anomalia_lluvias', con=db, schema="indicadores", if_exists='append', index=False)
