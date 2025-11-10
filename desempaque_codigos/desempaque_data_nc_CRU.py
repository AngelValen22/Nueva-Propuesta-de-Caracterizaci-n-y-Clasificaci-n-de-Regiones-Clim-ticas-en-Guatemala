import os
import netCDF4 as nc
import pandas as pd

## Creacion de dataframe de salida de datos
columnas = {'fecha':[],'variable':[],'valor':[],'lon':[],'lat':[],'fuente':[]}
df_data_cru = pd.DataFrame(data=columnas)


## Se guardan los nombres de los documentos .nc 
ruta_CRU = '/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_crudos_fuente/CRU/'
lista_archivos_CRU = os.listdir(ruta_CRU)

## Iretacion sobre los nombres de los archivos .nc
for nombre_archivos in lista_archivos_CRU:

    ## carga de data nc
    ds = nc.Dataset(ruta_CRU + nombre_archivos)

    ## Extraccion de prefijo para manejo de data .nc
    prefijo_nombre_archivo = nombre_archivos[21:-7]
    print(prefijo_nombre_archivo)
    

    ## variables de los archivos NetCDF
    longitud = ds.variables['lon']
    latitud = ds.variables['lat']
    tiempo = ds.variables['time']
    
    
    ## Conversion de dias julianos a formato de fecha UTC
    units = 'days since 1900-1-1' # formato de fecha selecionado para la conversion
    tiempo_utc = (nc.num2date( tiempo , units,has_year_zero=True)).astype('str')
    fechas =  pd.to_datetime(tiempo_utc).format('%Y-%m-%d')  # cambio de formato de fechas, 


    ## Iteracion sobre cada variables del archivo NetCDF
    ## Iteracion sobre longitudes del archivo y selección para guardarlas 
    for cant_longitud in longitud:
        if (cant_longitud >= -93) & (cant_longitud <= -87):
            
            ## Iteracion sobre latitudes del archivo y selección para guardarlas 
            for cant_latitud in latitud:
                if (cant_latitud >= 13) & (cant_latitud <= 19):
                    
                    ## Iteracion sobre mtodos los elemntos de tiempo para poder iterrar sobre elllos y extraerlos todos
                    for cant_tiempo in range(0,len(tiempo)):
                        
                        ##  Condiciones para delimiar coordenadas deseadas  
                        condicion_lon = (longitud[:] == cant_longitud)
                        condicion_lat = (latitud[:] == cant_latitud) 

                        ## Extraccion de datos del archivo NetCDF seleccionado con las condiciones obtenidas en pasos anteriores 
                        variable_climatica = ds.variables[ prefijo_nombre_archivo ][cant_tiempo, condicion_lat, condicion_lon ]
                        exa=variable_climatica[0].compressed()
                        print(exa)
                        
                        ## Captura en dataframe de informacion extraida de archivos NetCDF
                        nuevo_registro = {'fecha':tiempo_utc[cant_tiempo],'variable':prefijo_nombre_archivo,'valor':exa,'lon':cant_longitud,'lat':cant_latitud,'fuente':'cru'}
                        df_data_cru:pd.DataFrame = pd.concat([df_data_cru,pd.DataFrame([nuevo_registro])],ignore_index=True)
    


## Correccion de listas al momento de captura de datos en dataframe (en la columna "valor" se guarda un string con la siguente forma [numero] por lo esto corrige el mismo)
for index,row in df_data_cru.iterrows():
    valor = df_data_cru['valor'].values[index]
    remplazo = valor[1:-1]
    df_data_cru['valor'].values[index] = remplazo
    
 
## Arreglo de formato de fecha     
df_data_cru['fecha'] =  pd.to_datetime(df_data_cru['fecha']).dt.strftime('%Y-%m-%d')

## Filtro de fechas para el estudio
filtro_fecha = df_data_cru['fecha'] >= '1990-01-01'
df_data_cru_filtro_fecha:pd.DataFrame = df_data_cru[ filtro_fecha ]

filtro_fecha = df_data_cru_filtro_fecha['fecha'] <= '2021-01-01'
df_data_cru_filtro_fecha:pd.DataFrame = df_data_cru_filtro_fecha[ filtro_fecha ]


## Salida en formato CSV para el estudio posterior                           
df_data_cru_filtro_fecha.to_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_desempaquetados/cru.csv',index=False)  


