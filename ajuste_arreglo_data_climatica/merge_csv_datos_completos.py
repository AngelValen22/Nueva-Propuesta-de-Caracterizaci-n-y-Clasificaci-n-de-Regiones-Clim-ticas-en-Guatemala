import pandas as pd
import os



## Creacion de dataframe de salida de datos
columnas = {'fecha':[],'variable':[],'valor':[],'lon':[],'lat':[],'fuente':[]}
df_data_cru = pd.DataFrame(data=columnas)

## extraccion de archivos para concatenar 
carpeta_archivos = '/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datis/datos_desempaquetados/'
lista_archivos = os.listdir(carpeta_archivos)

## carga y concatenacion de archivos 
for archivo_csv in lista_archivos:
    df_data = pd.read_csv(carpeta_archivos + archivo_csv)
    df_data_cru:pd.DataFrame = pd.concat([df_data_cru,pd.DataFrame(df_data)],ignore_index=True)
    
## coreccion de nombre de precipitacion y coreccion de formato 
df_data_cru.replace('pre','prcp',inplace=True)
df_data_pivot:pd.DataFrame = df_data_cru.pivot_table(values= ['valor'], index = ['fecha','lat','lon','fuente'], columns = {'variable'}, aggfunc = 'first',sort=False)
df_data_pivot.columns = df_data_pivot.columns.droplevel(0)
df_data_pivot.reset_index(inplace=True)


## salida de archivo completo con data
df_data_pivot.to_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos/data_completa.csv',index=False)


    