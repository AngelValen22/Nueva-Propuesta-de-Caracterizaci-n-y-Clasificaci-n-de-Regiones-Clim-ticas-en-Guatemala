import pandas as pd
import os 
import netCDF4 as nc
from multiprocessing import Process

# Create a new process
process = Process()

def salida_archivos_csv(nombre_archivo):
    
    ## Creacion de dataframe de salida de datos
    columnas = {'fecha':[],'variable':[],'valor':[],'lon':[],'lat':[],'fuente':[]}
    df_data = pd.DataFrame(data=columnas)

        
    ## carga de data nc
    ruta_chirps = '/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_crudos_fuente/CHIRPS/'
    ds = nc.Dataset(ruta_chirps + nombre_archivo)
    print(ds.variables)
  
    
    ## variables de los archivos NetCDF
    longitud = ds.variables['X']
    latitud = ds.variables['Y']
    tiempo = ds.variables['T']
    
  
    
    ## Conversion de dias julianos a formato de fecha UTC
    units = ' months since 1960-1-1' # formato de fecha selecionado para la conversion
    tiempo_utc = (nc.num2date( tiempo[:] ,units=units, calendar = '360_day')).astype(str)
    fechas =  pd.to_datetime(tiempo_utc).format('%Y-%m-%d')  # cambio de formato de fechas, 
    

    for cant_longitud in longitud:
        if (cant_longitud >= -92.2106) & (cant_longitud <= -88.2282):
            
            ## Iteracion sobre latitudes del archivo y selección para guardarlas 
            for cant_latitud in latitud:
                if (cant_latitud >= 13.7149) & (cant_latitud <= 17.8188):

                    
                    ## Iteracion sobre mtodos los elemntos de tiempo para poder iterrar sobre elllos y extraerlos todos
                    for cant_tiempo in range(0,len(tiempo)):
                        
                        ##  Condiciones para delimiar coordenadas deseadas  
                        condicion_lon = (longitud[:] == cant_longitud)
                        condicion_lat = (latitud[:] == cant_latitud) 

                        ## Extraccion de datos del archivo NetCDF seleccionado con las condiciones obtenidas en pasos anteriores 
                        variable_climatica = (ds.variables[ 'precipitation' ][cant_tiempo, condicion_lat, condicion_lon ])
                        exa=variable_climatica[0].compressed()
                        print(tiempo_utc[cant_tiempo],cant_longitud,cant_latitud,exa)

                        
                        ## Captura en dataframe de informacion extraida de archivos NetCDF
                        nuevo_registro = {'fecha':tiempo_utc[cant_tiempo],'variable':'prcp','valor':exa,'lon':cant_longitud,'lat':cant_latitud,'fuente':'chirps'}
                        df_data:pd.DataFrame = pd.concat([df_data,pd.DataFrame([nuevo_registro])],ignore_index=True)
                        
    df_data.to_csv('prec_chirps_' + str(nombre_archivo) +'.csv')
    
    


ruta_documentos = '/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos/datos_crudos_fuente/'



## Creacion de dataframe de salida de datos
columnas = {'fecha':[],'variable':[],'valor':[],'lon':[],'lat':[],'fuente':[]}
df_data = pd.DataFrame(data=columnas)


## Se guardan los nombres de los documentos .nc 
ruta_chirps = '/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos/datos_crudos_fuente/CHIRPS/'
lista_archivos_Chirps = os.listdir(ruta_chirps)

lista_p=[]

lista_p.append(Process(target=salida_archivos_csv, args=(lista_archivos_Chirps[0],)))
# lista_p.append(Process(target=salida_archivos_csv, args=(lista_archivos_Chirps[1],)))
# lista_p.append(Process(target=salida_archivos_csv, args=(lista_archivos_Chirps[2],)))
# lista_p.append(Process(target=salida_archivos_csv, args=(lista_archivos_Chirps[3])))
# lista_p.append(Process(target=salida_archivos_csv, args=(lista_archivos_Chirps[4])))
# lista_p.append(Process(target=salida_archivos_csv, args=(lista_archivos_Chirps[5])))

lista_p[0].start()
# lista_p[1].start()
# lista_p[2].start()
# lista_p[3].start()
# lista_p[4].start()
# lista_p[5].start()

# df_data = pd.read_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/prec_chirps_data.nc.csv',index_col=False)


# ## Correccion de listas al momento de captura de datos en dataframe (en la columna "valor" se guarda un string con la siguente forma [numero] por lo esto corrige el mismo)
# for index,row in df_data.iterrows():
#     valor = df_data['valor'].values[index]
#     remplazo = valor[1:-1]
#     df_data['valor'].values[index] = remplazo
    
 
# ## Arreglo de formato de fecha     
# df_data['fecha'] =  pd.to_datetime(df_data['fecha']).dt.strftime('%Y-%m-%d')

# df_data.drop(columns=['Unnamed: 0'],inplace=True)


# ## Salida en formato CSV para el estudio posterior                           
# df_data.to_csv('/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_desempaquetados/chirps.csv',index=False)  

