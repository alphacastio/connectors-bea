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

# In[2]:


BEA_API_KEY = dotenv_values(".env").get("BEA_API_KEY")


# In[3]:


# Table 1.1.2. Contributions to Percent Change in Real Gross Domestic Product (A) (Q)
response_T10102 = requests.get(f'https://apps.bea.gov/api/data/?&UserID={BEA_API_KEY}&'
                          'method=GetData&DataSetName=NIPA&TableName=T10102&Frequency=Q&Year=ALL&ResultFormat=JSON')


# In[4]:


df = pd.DataFrame(eval(response_T10102.content.decode('utf-8'))['BEAAPI']['Results']['Data'])


# In[5]:


df = df[['LineNumber', 'LineDescription', 'TimePeriod', 'DataValue']]


# In[6]:


#Hago el reemplazo de las fechas
dict_quarters = {'Q1':'-01-01', 'Q2':'-04-01', 'Q3':'-07-01', 'Q4':'-10-01'}
df['TimePeriod'] = df['TimePeriod'].replace(dict_quarters, regex=True)
df['TimePeriod'] = pd.to_datetime(df['TimePeriod'], format='%Y-%m-%d')

#Convierto LineNumber de string a integer
df['LineNumber'] = df['LineNumber'].astype(int)


# In[7]:


#Se harcodean los nombres para el anidamiento de los nombres de las columnas
#Sino hay que crear columnas auxiliares e ir anidando. La asignacion sigue el orden del LineNumber

categorias = {1:'Gross domestic product', 2:'Personal consumption expenditures', 
              3:'Personal consumption expenditures - Goods', 4:'Personal consumption expenditures - Goods - Durable goods',
             5: 'Personal consumption expenditures - Goods - Nondurable goods',
             6:'Personal consumption expenditures - Services', 7:'Gross private domestic investment',
             8:'Gross private domestic investment - Fixed investment',
              9:'Gross private domestic investment - Fixed investment - Nonresidential',
             10:'Gross private domestic investment - Fixed investment - Nonresidential - Structures',
             11:'Gross private domestic investment - Fixed investment - Nonresidential - Equipment',
             12:'Gross private domestic investment - Fixed investment - Nonresidential - Intellectual property products',
             13:'Gross private domestic investment - Fixed investment - Residential',
             14:'Gross private domestic investment - Change in private inventories',
             15:'Net exports of goods and services', 16:'Net exports of goods and services - Exports',
             17:'Net exports of goods and services - Exports - Goods',
             18:'Net exports of goods and services - Exports - Services',
             19:'Net exports of goods and services - Imports', 20:'Net exports of goods and services - Imports - Goods',
             21:'Net exports of goods and services - Imports - Services',
             22:'Government consumption expenditures and gross investment',
             23:'Government consumption expenditures and gross investment - Federal',
             24:'Government consumption expenditures and gross investment - Federal - National defense',
             25:'Government consumption expenditures and gross investment - Federal - Nondefense',
             26:'Government consumption expenditures and gross investment - State and local'}

df['Category'] = pd.NA

#Voy asignando los nombres en otra columna
for linea, categoria in categorias.items():
    df['Category'] = np.where(df['LineNumber'] == linea, categoria, df['Category'])


# In[8]:


#Asigno los valores del diccionario a una lista para ordernar las columnas
column_order = list(categorias.values())
df = df.pivot(index='TimePeriod', columns='Category', values='DataValue')

df = df[column_order]

df.index.names = ['Date']
df.rename_axis(None, axis=1, inplace=True)

df['country'] = 'USA'


alphacast.datasets.dataset(379).upload_data_from_df(df, 
    deleteMissingFromDB = True, onConflictUpdateDB = True, uploadIndex=True)