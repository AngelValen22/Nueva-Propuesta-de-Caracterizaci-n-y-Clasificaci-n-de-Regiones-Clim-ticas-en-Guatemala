import pandas as pd


## carga de data CRU
df_data_cru = pd.read_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_desempaquetados/cru.csv',index_col=False)

## ajuste de valores en formato string a float
for index,row in df_data_cru.iterrows():
    valor = df_data_cru['valor'].values[index]
    remplazo = valor[1:-1]
    
    df_data_cru['valor'].values[index] = remplazo
    
    
## arreglo de formato de fecha 
df_data_cru['fecha'] =  pd.to_datetime(df_data_cru['fecha']).dt.strftime('%Y-%m-%d')

filtro_fecha = df_data_cru['fecha'] >= '1990-01-01'
df_data_cru_filtro_fecha:pd.DataFrame = df_data_cru[ filtro_fecha ]

filtro_fecha = df_data_cru_filtro_fecha['fecha'] <= '2021-01-01'
df_data_cru_filtro_fecha:pd.DataFrame = df_data_cru_filtro_fecha[ filtro_fecha ]

## arreglo de desimales de valor
df_data_cru_filtro_fecha['valor']=round(df_data_cru_filtro_fecha['valor'],2)
 
## archivo de salida
df_data_cru_filtro_fecha.to_csv('remplazo_cru.csv',index=False)
    