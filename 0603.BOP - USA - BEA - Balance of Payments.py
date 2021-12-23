#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import numpy as np
import pandas as pd

from alphacast import Alphacast
from dotenv import dotenv_values
API_KEY = dotenv_values(".env").get("API_KEY")
alphacast = Alphacast(API_KEY)

#A tener en cuenta

# • Number of requests per minute (100), and or
# • Data volume retrieved per minute (100 MB), and/or
# • Errors per minute (30)

BEA_API_KEY= 'XX'


# In[6]:


#Defino en una lista con los conceptos
indicadores = ['ExpGdsServIncRec','ExpGdsServ','ExpGds','ExpServ','PrimIncRec','InvIncRec','CompOfEmplRec',
               'SecIncRec','ImpGdsServIncPay','ImpGdsServ','ImpGds','ImpServ','PrimIncPay','InvIncPay','CompOfEmplPay',
               'SecIncPay','CapTransRecAndOthCred','CapTransPayAndOthDeb','FinAssetsExclFinDeriv','DiInvAssets',
               'PfInvAssets','OthInvAssets','ReserveAssets','FinLiabsExclFinDeriv','DiInvLiabs','PfInvLiabs',
               'OthInvLiabs','FinDeriv','StatDisc','SeasAdjDisc','BalCurrAcct','BalGdsServ','BalGds','BalServ',
               'BalPrimInc','BalSecInc','BalCapAcct','NetLendBorrCurrCapAcct','NetLendBorrFinAcct']


# In[7]:


#Itero y guardo la información en un dataframe temporal
#Se baja el trimestral ajustado y no ajustado para todos los años

for indice, indicador in enumerate(indicadores):
    response = requests.get('https://apps.bea.gov/api/data/?&UserID=' + BEA_API_KEY + 
                            '&method=GetData&DataSetName=ITA&Indicator=' + indicador + 
                            '&AreaOrCountry=AllCountries&Frequency=QSA,QNSA&Year=All&ResultFormat=JSON')
    df_temp = pd.DataFrame(eval(response.content.decode('utf-8'))['BEAAPI']['Results']['Data'])
    if indice == 0:
        df = df_temp.copy()
    else:
        df = df.append(df_temp, ignore_index=True)


# In[8]:


#Hay que mantener TimeSeriesDescription, TimePeriod, DataValue
df = df[['TimeSeriesDescription', 'TimePeriod', 'DataValue']]


# In[9]:


#Hago un reshape de Long to wide
df = df.pivot(index = ['TimePeriod'], columns = ['TimeSeriesDescription'], values = ['DataValue'])
#Se resetea el indice
df.reset_index(inplace=True)


# In[10]:


#Se elimina el multi index
df.columns = df.columns.droplevel()
#Se renombra la columna 0 porque al eliminar el multi index, se pierde el nombre
df.rename(columns = {df.columns[0]:'Date'}, inplace=True)


# In[11]:


#Cambio la denominación de los trimestres
dict_quarters = {'Q1':'-01-01', 'Q2': '-04-01', 'Q3':'-07-01', 'Q4':'-10-01'}
df['Date'] = df['Date'].replace(dict_quarters, regex=True)

#Paso la columna Date a datetime y la fijo como indice
df['Date'] = pd.to_datetime(df['Date'], format = '%Y-%m-%d').dt.strftime('%Y-%m-%d')
df.set_index('Date', inplace=True)
df.rename_axis(None, axis=1, inplace=True)


# In[12]:


#Remuevo la leyenda de que no están ajustadas estacionalmente
df.columns = df.columns.str.replace('; quarterly not seasonally adjusted', '')
#Reemplazo la leyenda del ajuste estacional
df.columns = df.columns.str.replace('; quarterly seasonally adjusted', ' - sa_orig')

df['country'] = 'USA'
alphacast.datasets.dataset(603).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)


# In[ ]:




