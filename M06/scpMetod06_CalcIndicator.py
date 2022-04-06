#!/usr/bin/env python
# coding: utf-8

# In[3]:


#pip install SQLAlchemy
#pip install psycopg2-binary


# In[1]:


import sqlalchemy as db # para trabajar con la base de datos con sql
import os #para acceder a variables del environment
import pandas as pd
import numpy as np
import datetime as dt

from dotenv import load_dotenv
load_dotenv()
from sqlalchemy import create_engine


# In[2]:


#cargar credenciales para conectarse a la base de datos
db_string = os.environ['DATABASE_URL']
db = create_engine(db_string)


# In[4]:


#cargar tablas monitoreo 6 y metadatos del punto
df_registros = pd.read_sql_query('SELECT * FROM kobo_data.medicion_lluvia_registros',con=db)
df_metadata = pd.read_sql_query('SELECT * FROM kobo_data.medicion_lluvia',con=db)
df_codigo  = pd.read_sql_query('SELECT * FROM kobo_data.codigo_punto',con=db)


# In[6]:


df= pd.merge(df_registros,df_metadata,left_on="id_formulario",right_on="id",how="left")
df= pd.merge(df,df_codigo,on="tx_codigo",how="left")
#print(df.shape)


# In[7]:


#Crear variables de interes a partir de fecha
df['fecha']=pd.to_datetime(df['dt_fecha_hora'])
df['anio']= df['fecha'].dt.year
df['dt_mes']= df['fecha'].dt.month
df['juliano']=df['fecha'].dt.strftime("%j")

#Crear tabla con registros diarios agrupados
df_registro_diario_lluvia = df.groupby(['tx_codigo','tx_vereda','ct_asociacion','anio','dt_mes','fecha'],dropna=False)['nm_precipitacion'].sum().reset_index()
#print(df_registro_diario_lluvia)


# In[8]:


#Cargar tabla de parámetros para cáculo de indicador
iap_parametros = pd.read_sql_query('SELECT * FROM kobo_data.historico_precipitacion',con=db)
#print(iap_parametros)


# In[9]:


#unir tabla registros diarios agrupados con parametros historicos
registro_diario_lluvia = pd.merge(df_registro_diario_lluvia , iap_parametros[['dt_mes',"nm_p10_historico",'nm_p90_historico']], on="dt_mes", how='left')

#renombrar para exportar
registro_diario_lluvia = registro_diario_lluvia.rename(columns={'tx_codigo':'codigo','tx_vereda':'nombre_vereda','ct_asociacion':'asociacion','nm_precipitacion':'precipitacion','dt_mes':'mes','nm_p10_historico':'p10','nm_p90_historico':'p90'})
#print(registro_diario_lluvia)


# In[10]:

#Exportar a db
db.execute('TRUNCATE TABLE indicadores.registro_diario_lluvia CASCADE')
registro_diario_lluvia.to_sql('registro_diario_lluvia', con=db, schema="indicadores", if_exists='append', index=False)


# In[11]:


#reagrupar por mes para calcular indicador
df_registro_mes_lluvia = registro_diario_lluvia.groupby(['codigo','nombre_vereda','asociacion','anio','mes'],dropna=False)['precipitacion'].sum().reset_index()

#agregar valores historicos
df_mes_parametros = pd.merge(df_registro_mes_lluvia,iap_parametros,left_on="mes",right_on='dt_mes', how='left')

#Calcular indicador de anomalia para valores mayores y menores al historico de precipitacion
df_mes_parametros['iap_p'] = df_mes_parametros['nm_fe_p']*(df_mes_parametros['precipitacion']-df_mes_parametros['nm_mediana_precipitacion_historico'])/(df_mes_parametros['nm_promedio_p90_historico']-df_mes_parametros['nm_mediana_precipitacion_historico'])
df_mes_parametros['iap_n'] = df_mes_parametros['nm_fe_n']*(df_mes_parametros['precipitacion']-df_mes_parametros['nm_mediana_precipitacion_historico'])/(df_mes_parametros['nm_promedio_p10_historico']-df_mes_parametros['nm_mediana_precipitacion_historico'])

#print(df_mes_parametros)


# In[12]:


#Seleccionar el calculo correspondiente segun la logica de relacion con precipitacion historica
df_mes_parametros['iap'] = df_mes_parametros.iap_p
mask = df_mes_parametros.precipitacion < df_mes_parametros.nm_mediana_precipitacion_historico

df_mes_parametros.loc[mask,'iap'] = df_mes_parametros.loc[mask,'iap_n']

#definir puntos de corte segun van Rooy (1965)
puntos_corte=[-np.inf,-3,-2,-1,-0.5,0.49999999,0.99999999,1.99999999,2.99999999,np.inf] #verificar si dan los limites cerrados y abiertos
rangos=["Extremadamente seco","Muy seco","Moderadamente seco","Un poco seco","Aproximadamente normal","Un poco húmedo","Moderadamente húmedo","Muy húmedo","Extremadamente húmedo"]
df_mes_parametros['iap_label'] = pd.cut(df_mes_parametros['iap'], puntos_corte , labels=rangos)

#print(df_mes_parametros)


# In[ ]:





# In[13]:


#Seleccionar columnas para tabla a exportar
indice_anomalia_lluvias = df_mes_parametros[['asociacion','anio','mes','iap','iap_label']] #tabla a exportar

#print(indice_anomalia_lluvias)


# In[14]:


#Exportar a db
db.execute('TRUNCATE TABLE indicadores.indice_anomalia_lluvias CASCADE')
indice_anomalia_lluvias.to_sql('indice_anomalia_lluvias', con=db, schema="indicadores", if_exists='append', index=False)
