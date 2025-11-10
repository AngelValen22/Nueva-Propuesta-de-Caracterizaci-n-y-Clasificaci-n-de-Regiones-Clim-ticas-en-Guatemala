import pandas as pd
import os


## Creacion de dataframe de salida de datos
columnas = {'fecha':[],'variable':[],'valor':[],'lon':[],'lat':[],'fuente':[]}
df_data_cru = pd.DataFrame(data=columnas)

## Lista de archivos enacs de temperatuta maxima
carpeta_archivos = '~/EPS_tesis_regiones_climaticas/datos/csv_enacs_tmax/'
lista_archivos = os.listdir(carpeta_archivos)


## carga y concatenacion de archivos 
for archivo_csv in lista_archivos:
    df_data = pd.read_csv(carpeta_archivos + archivo_csv)
    df_data_cru:pd.DataFrame = pd.concat([df_data_cru,pd.DataFrame(df_data)],ignore_index=True)
    
print(df_data_cru)
    
    

## Correccion de listas al momento de captura de datos en dataframe (en la columna "valor" se guarda un string con la siguente forma [numero] por lo esto corrige el mismo)
for index,row in df_data_cru.iterrows():
    valor = df_data_cru['valor'].values[index]
    remplazo = valor[1:-1]
    df_data_cru['valor'].values[index] = remplazo
    
 
## Arreglo de formato de fecha     
df_data_cru['fecha'] =  pd.to_datetime(df_data_cru['fecha']).dt.strftime('%Y-%m-%d')
df_data_cru['fuente'] = 'enacs'

print(df_data_cru)


## Salida en formato CSV para el estudio posterior                           
df_data_cru.to_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_desempaquetados/enacs_tamx.csv',index=False)  
