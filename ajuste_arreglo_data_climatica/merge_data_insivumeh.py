import pandas as pd






df_data_historica = pd.read_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_insivumeh/CONSULTA_HISTORICA.csv',sep=';')
df_data_actual = pd.read_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_insivumeh/CONSULTA_ACTUALIDAD.csv',sep=';')


df_data_merge = pd.merge(df_data_historica,df_data_actual,how='outer')
df_data_merge.drop(columns=['NOMBRE_ESTACIÓN','ALTITUD'], inplace=True)

df_data_merge.rename(columns={'FECHA':'fecha', 'LATITUD':'lat', 'LONGITUD':'lon', 'TEMPERATURA_MÁXIMA':'tmx', 'TEMPERATURA_MÍNIMA':'tmn', 'TEMPERATURA_MEDIA':'tmp', 'PRECIPITACIÓN':'prcp'}, inplace=True)
print(df_data_merge)

df_data_merge['fuente'] = 'insivumeh'

df_data_merge.to_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_insivumeh/data_completa_insivumeh.csv',index=False)