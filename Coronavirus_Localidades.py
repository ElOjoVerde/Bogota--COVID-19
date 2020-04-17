# -*- coding: utf-8 -*-
"""
Created on Fri Apr 10 19:22:47 2020

@author: Javier Medina
"""

import pandas as pd
import urllib.request
from io import StringIO
import numpy as np

#--------------------------------------------------------------------------------------------------------------------------

ruta = 'J:\Documents\GIT\Bogot----COVID-19' ##Ruta donde este guardado el GIT

#--------------------------------------------------------------------------------------------------------------------------
#LECTURA DE DATOS DESDE EL LINK
with urllib.request.urlopen("http://saludata.saludcapital.gov.co/osb/datos_abiertos_osb/enf-transmisibles/OSB_EnfTransm-COVID-19.csv") as url:
        data = url.read().decode('ANSI') #SE LLAMA EL CSV DESDE LA PAGINA WEB, ESTE ES LEIDO COMO FORMATO TEXTO PARA PODER ELIMINAR LAS 4 PRIMERAS LINEAS
        #Y LAS ULTIMAS TRES LINEAS
#--------------------------------------------------------------------------------------------------------------------------     
#SE ELIMINAN LAS 4 PRIMERAS LINEAS Y LAS ULTIMAS 3 LINEAS QUE CONTIENEN INFORMACIÓN IRRELEVANTE
Datos = data.split('\n',4)[-1] #ELIMINA LAS 4 PRIMERAS LINEAS
Datos = StringIO(Datos)
#--------------------------------------------------------------------------------------------------------------------------
#SE TRANSFORMA EL ARCHIVO DE TEXTO A UN DATAFRAME
Casos_T = pd.read_csv(Datos, sep=";")
Casos_T = Casos_T[:-3] #ELIMINA LAS 3 ULTIMAS LINEAS
#--------------------------------------------------------------------------------------------------------------------------
##COMIENZA LA TRANSFORMACIÓN DE LOS DATOS ORIGINALES, PARA LA CREACIÓN DE NUEVAS TABLAS
#--------------------------------------------------------------------------------------------------------------------------

#SE DEFINEN EL TIPO DE DATOS DE LAS COLUMNAS
Casos_T['ID de caso']=Casos_T['ID de caso'].astype('int')
Casos_T['Edad']=Casos_T['Edad'].astype('int')
Casos_T['Fecha de diagnóstico'] = Casos_T['Fecha de diagnóstico'].str.replace(" ", "")
Inciertos = Casos_T[Casos_T['Fecha de diagnóstico']=='INS']
Casos_T = Casos_T.drop(Casos_T[Casos_T['Fecha de diagnóstico'] == 'INS'].index)
Casos_T['Fecha de diagnóstico'] =  pd.to_datetime(Casos_T['Fecha de diagnóstico'],format='%d/%m/%Y')
Casos_T['LocCodigo'], Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.split(' - ', 1).str

#--------------------------------------------------------------------------------------------------------------------------

#SE GENERALIZA EL NOMBRE DE LAS LOCALIDADES PARA QUE COINCIDAN CON EL SHAPEFILE
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.upper()
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"Á", "A")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"É", "E")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"Í", "I")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"Ó", "O")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"Ú", "U")

#--------------------------------------------------------------------------------------------------------------------------

#SE ORGANIZA LA TABLA POR FECHA DE DIAGNOSTICO Y SE EXPORTA A FORMATO CSV
Casos_T=Casos_T.sort_values(by=['Fecha de diagnóstico', 'Localidad de residencia'])

filename = (str(ruta) + '/Casos Coronavirus/Casos_Coronavirus-Bogotá.csv')
Casos_T.to_csv(filename, index= False)

#--------------------------------------------------------------------------------------------------------------------------
##SE ORGANIZAN LOS DATOS DE MANERA TEMPORAL PARA CADA UNA DE LAS LOCALIDADES, PARA CONSTRUIR GRÁFICAS EVOLUTIVAS
#--------------------------------------------------------------------------------------------------------------------------

Casos_Loc=Casos_T[['Localidad de residencia', 'Fecha de diagnóstico', 'Sexo']]
Casos_Loc = pd.DataFrame(Casos_Loc.pivot_table(index=['Fecha de diagnóstico'], columns=['Localidad de residencia'], aggfunc='count'))
Casos_Loc = Casos_Loc.fillna(0)
Casos_Loc2 = Casos_Loc.reset_index()

Columns = pd.DataFrame(Casos_Loc2.columns)
Columns = Columns.drop(0, axis = 0)
Columns = Columns.reset_index(drop=True)
New_Columns = pd.DataFrame(Casos_T['Localidad de residencia'].unique())
New_Columns = New_Columns.sort_values(by=[0])
New_Columns = New_Columns.reset_index(drop=True)

Temporal=pd.DataFrame(columns=('Fecha de diagnóstico', 1))

for i in Columns[0]:
    for i2 in range(1, len(Casos_Loc2)):
        Casos_Loc2.loc[i2,i] = Casos_Loc2.loc[i2, i] + Casos_Loc2.loc[(i2-1), i]

Temporal['Fecha de diagnóstico']=Casos_Loc2[('Fecha de diagnóstico', '')]

for a in range(0,len(Columns)):
    Temporal[New_Columns.loc[a,0]]=Casos_Loc2[Columns.loc[a,0]]
    
Temporal = Temporal.drop(1, axis = 1)
Temporal = Temporal.set_index(Temporal['Fecha de diagnóstico'])

MinFecha=min(Temporal['Fecha de diagnóstico'])
MaxFecha=max(Temporal['Fecha de diagnóstico'])

FechaIndex=pd.DataFrame(pd.date_range(start=MinFecha, end=MaxFecha, freq='D'))
FechaIndex=FechaIndex.rename(columns={0:'Fecha'})
FechaIndex=pd.to_datetime(FechaIndex['Fecha'])

Temporal = Temporal.reindex(FechaIndex)
Temporal = Temporal.drop('Fecha de diagnóstico', axis = 1)
Temporal = Temporal.fillna(method='ffill')
Temporal = Temporal.T

filename_2 = (str(ruta) + '\Histórico por Localidad\Localidad_Coronavirus.csv')
Temporal.to_csv(filename_2)

#--------------------------------------------------------------------------------------------------------------------------
##SE TOTALIZAN LOS DATOS POR GRUPOS ETARIOS
#--------------------------------------------------------------------------------------------------------------------------

df=pd.DataFrame(columns=('Localidad de residencia', 'F 0 - 9 Años', 'F 10 - 19 Años', 'F 20 - 29 Años', 'F 30 - 39 Años', 
                         'F 40 - 49 Años', 'F 50 - 59 Años','F 60 - 69 Años','F 70 - 79 Años','F 80 - 89 Años',
                         'F 90 - 99 Años','M 0 - 9 Años', 'M 10 - 19 Años', 'M 20 - 29 Años', 'M 30 - 39 Años', 
                         'M 40 - 49 Años', 'M 50 - 59 Años','M 60 - 69 Años','M 70 - 79 Años','M 80 - 89 Años',
                         'M 90 - 99 Años', 'Total M','Total F', ))
rango_a=0
rango_b=9
H=0
M=0
ind = 0

for l in New_Columns[0]:
    print(l)
    df.loc[ind,'Localidad de residencia']=l
    
    for g in Casos_T['Sexo'].unique():
        print(g)
        
        while rango_b <= max(Casos_T['Edad']+10):
            print(str(rango_a) +' - '+str(rango_b))
            df.loc[ind, str(str(g) +' '+ str(rango_a)+' - '+str(rango_b)+' Años')]=len(Casos_T[(Casos_T['Localidad de residencia'] == l) & 
                            (Casos_T['Sexo'] == g) & (Casos_T['Edad']>=rango_a) & (Casos_T['Edad']<=rango_b)])
            rango_a = rango_a+10
            rango_b = rango_b+10
        rango_a=0
        rango_b=9
        
    ind = ind+1

df['Total M'] = df.iloc[:,11:-3].sum(axis=1)
df['Total F'] = df.iloc[:,1:10].sum(axis=1)
df['TOTAL'] = df.iloc[:,[-2,-1]].sum(axis=1)

filename_3 = (str(ruta) + '/Casos Coronavirus/Grupos_Etarios.csv')
df.to_csv(filename_3)

#--------------------------------------------------------------------------------------------------------------------------
##SE TOTALIZAN LOS DATOS POR GENERO, EDAD Y LOCALIDAD PARA PASARLOS AL SHAPEFILE
#--------------------------------------------------------------------------------------------------------------------------

#Filtro_2 = Casos_T[['Localidad de residencia', 'Sexo', 'Edad']]
#
#ShpData = Temporal.reset_index()
#ShpData = ShpData.rename(columns={'index':'Localidad', MaxFecha : 'Total'})
#ShpData = ShpData.iloc[:, [0,-1]]