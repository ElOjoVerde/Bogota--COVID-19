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

ruta = 'J:/Documents/GIT/Bogot----COVID-19' ##Ruta donde este guardado el GIT

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
Casos_T['Edad']=Casos_T['Edad'].fillna(9999)
Inciertos = Casos_T[(Casos_T['Fecha de diagnóstico']=='INS')|(Casos_T['Edad']==9999)]
Casos_T = Casos_T.drop(Casos_T[Casos_T['Edad'] == 9999].index)
Casos_T = Casos_T.drop(Casos_T[Casos_T['Fecha de diagnóstico'] == 'INS'].index)

Casos_T['ID de caso']=Casos_T['ID de caso'].astype('int')
Casos_T['Edad']=Casos_T['Edad'].astype('int')
#Casos_T['Fecha de diagnóstico'] = Casos_T['Fecha de diagnóstico'].str.replace(" ", "")
Inciertos = Casos_T[(Casos_T['Fecha de diagnóstico']=='INS')|(Casos_T['Edad']==9999)]
Casos_T['Fecha de diagnóstico'] = Casos_T['Fecha de diagnóstico'].str.replace(u"/20", "/2020")
Casos_T['Fecha de diagnóstico'] =  pd.to_datetime(Casos_T['Fecha de diagnóstico'],format='%d/%m/%Y')
#Casos_T['LocCodigo'], Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.split(' - ', 1).str

#--------------------------------------------------------------------------------------------------------------------------

#SE GENERALIZA EL NOMBRE DE LAS LOCALIDADES PARA QUE COINCIDAN CON EL SHAPEFILE
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.upper()
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"Á", "A")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"É", "E")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"Í", "I")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"Ó", "O")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"Ú", "U")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"SANTAFE", "SANTA FE")
Casos_T['Localidad de residencia'] = Casos_T['Localidad de residencia'].str.replace(u"LA CANDELARIA", "CANDELARIA")

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
Temporal['TOTAL'] = Temporal.iloc[:,0:21].sum(axis=1)
Temporal = Temporal.T
Temporal = Temporal.reset_index()
Temporal = Temporal.rename(columns={'index':'Localidad'})

filename_2 = (str(ruta) + '\Histórico por Localidad\Infectados_Localidad.csv')
Temporal.to_csv(filename_2, index=False)

#--------------------------------------------------------------------------------------------------------------------------
##SE TOTALIZAN LOS DATOS POR GRUPOS ETARIOS Y GENERO
#--------------------------------------------------------------------------------------------------------------------------

df=pd.DataFrame(columns=('Localidad de residencia', 'Años F 0_9', 'Años F 10_19', 'Años F 20_29', 'Años F 30_39', 
                         'Años F 40_49', 'Años F 50_59','Años F 60_69','Años F 70_79','Años F 80_89',
                         'Años F 90_99','Años F >100', 'Años M 0_9', 'Años M 10_19', 'Años M 20_29', 'Años M 30_39', 
                         'Años M 40_49', 'Años M 50_59','Años M 60_69','Años M 70_79','Años M 80_89',
                         'Años M 90_99', 'Años M >100'))
df = df.iloc[:,1:-1].astype(int)
rango_a=0
rango_b=9
H=0
M=0
ind = 0

for l in New_Columns[0]:
    df.loc[ind,'Localidad de residencia']=l
    
    for g in Casos_T['Sexo'].unique():
        
        while rango_b <= max(Casos_T['Edad']+10):
            if rango_b < 100:
                df.loc[ind, str('Años '+str(g)+' '+str(rango_a)+'_'+str(rango_b))]=len(Casos_T[(Casos_T['Localidad de residencia'] == l) & 
                                (Casos_T['Sexo'] == g) & (Casos_T['Edad']>=rango_a) & (Casos_T['Edad']<=rango_b)])
            else:
                df.loc[ind, str('Años ' + str(g) + ' >100')]=len(Casos_T[(Casos_T['Localidad de residencia'] == l) & (Casos_T['Edad']>=100) &
                                (Casos_T['Sexo'] == g)])
            rango_a = rango_a+10
            rango_b = rango_b+10
        rango_a=0
        rango_b=9
            
    ind = ind+1
df=df[['Localidad de residencia', 'Años F 0_9', 'Años F 10_19', 'Años F 20_29', 'Años F 30_39', 
                         'Años F 40_49', 'Años F 50_59','Años F 60_69','Años F 70_79','Años F 80_89',
                         'Años F 90_99','Años F >100', 'Años M 0_9', 'Años M 10_19', 'Años M 20_29', 'Años M 30_39', 
                         'Años M 40_49', 'Años M 50_59','Años M 60_69','Años M 70_79','Años M 80_89',
                         'Años M 90_99', 'Años M >100']]
df['Total_M'] = df.iloc[:,12:23].sum(axis=1)
df['Total_F'] = df.iloc[:,1:12].sum(axis=1)
df['TOTAL'] = df.iloc[:,[-2,-1]].sum(axis=1)

filename_3 = (str(ruta) + '/Casos Coronavirus/Grupos_Etarios_Sexo.csv')
df.to_csv(filename_3, index = False)

#--------------------------------------------------------------------------------------------------------------------------
##SE TOTALIZAN LOS DATOS POR GRUPOS ETARIOS
#--------------------------------------------------------------------------------------------------------------------------

df2=pd.DataFrame(columns=('Localidad de residencia', 'Años 0_9', 'Años 10_19', 'Años 20_29', 'Años 30_39', 
                         'Años 40_49', 'Años 50_59','Años 60_69','Años 70_79','Años 80_89',
                         'Años 90_99', 'Años>100'))

df2 = df2.iloc[:,1:-1].astype(int)
rango_a=0
rango_b=9
H=0
M=0
ind = 0

for l in New_Columns[0]:
    df2.loc[ind,'Localidad de residencia']=l
    while rango_b <= max(Casos_T['Edad']+10):
        if rango_b < 100:
            df2.loc[ind, str('Años '+str(rango_a)+'_'+str(rango_b))]=len(Casos_T[(Casos_T['Localidad de residencia'] == l) & 
                        (Casos_T['Edad']>=rango_a) & (Casos_T['Edad']<=rango_b)])
            df2.loc[ind, 'Fallecidos'] = len(Casos_T[(Casos_T['Localidad de residencia'] == l) & (Casos_T['Estado']=='Fallecido')])
            
        else:
            df2.loc[ind, str('Años>100')]=len(Casos_T[(Casos_T['Localidad de residencia'] == l) & (Casos_T['Edad']>=100)])
        rango_a = rango_a+10
        rango_b = rango_b+10
    rango_a=0
    rango_b=9
        
    ind = ind+1
df2=df2[['Localidad de residencia', 'Años 0_9', 'Años 10_19', 'Años 20_29', 'Años 30_39', 
                         'Años 40_49', 'Años 50_59','Años 60_69','Años 70_79','Años 80_89',
                         'Años 90_99', 'Años>100', 'Fallecidos']]
df2['Total'] = df2.iloc[:,1:12].sum(axis=1)
df2['Total_M'] = df['Total_M']
df2['Total_F'] = df['Total_F']

filename_4 = (str(ruta) + '/Casos Coronavirus/Grupos_Etarios.csv')
df2.to_csv(filename_4, index = False)

#--------------------------------------------------------------------------------------------------------------------------
##SE TOTALIZAN LOS DATOS POR GENERO, EDAD Y LOCALIDAD PARA PASARLOS AL SHAPEFILE
#--------------------------------------------------------------------------------------------------------------------------

import geopandas as gpd

Localidades_SHP = gpd.read_file(str(ruta) + "/shp localidades/Origen/Loca.shp")
Localidades_SHP = Localidades_SHP.merge(right=df2, how='left', left_on='LocNombre', right_on='Localidad de residencia').drop('Localidad de residencia', axis=1)
Localidades_SHP = Localidades_SHP[['LocNombre', 'LocAAdmini',  'LocCodigo', 'Años 0_9', 'Años 10_19','Años 20_29','Años 30_39','Años 40_49',
                                   'Años 50_59','Años 60_69','Años 70_79','Años 80_89','Años 90_99', 'Años>100', 'Fallecidos','Total_M', 'Total_F', 'Total', 
                                   'LocArea','SHAPE_Leng', 'SHAPE_Area', 'geometry']]
Localidades_SHP.to_file(str(ruta) + '/shp localidades/Resultados/GeoJSON/Localidades_Coronavirus.json', 'GeoJSON', encoding = 'utf-8')
Localidades_SHP = gpd.read_file(str(ruta) + '/shp localidades/Resultados/GeoJSON/Localidades_Coronavirus.json')
Localidades_SHP.to_file(str(ruta) + '/shp localidades/Resultados/ShapeFile/Localidades_Coronavirus.shp', encoding = 'utf-8')