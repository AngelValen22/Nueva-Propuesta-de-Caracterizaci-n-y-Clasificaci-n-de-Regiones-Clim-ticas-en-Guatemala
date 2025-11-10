import pandas as pd
import os
import netCDF4 as nc
from multiprocessing import Process



def salida_archivos_csv(nombre_archivo,inicio,fin):

    ## Creacion de dataframe de salida de datos
    columnas = {'fecha':[],'variable':[],'valor':[],'lon':[],'lat':[],'fuente':[]}
    df_data = pd.DataFrame(data=columnas)


    ## carga de data nc
    #ruta_chirps = '/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_crudos_fuente/CHIRPS/'
    ds = nc.Dataset(nombre_archivo)


    ## variables de los archivos NetCDF
    longitud = ds.variables['X']
    latitud = ds.variables['Y']
    tiempo = ds.variables['T']


    # ## Conversion de dias julianos a formato de fecha UTC
    units = 'months since 1960-01-01' # formato de fecha selecionado para la conversion
    tiempo_utc = (nc.num2date( tiempo[:] , units,calendar='360_day')).astype(str)
    #fechas =  pd.to_datetime(tiempo_utc).format('%Y-%m-%d')  # cambio de formato de fechas

    for cant_longitud in longitud:
        if (cant_longitud >= -92.2106) & (cant_longitud <= -88.2282):

            ## Iteracion sobre latitudes del archivo y selección para guardarlas
            for cant_latitud in latitud:
                if (cant_latitud >= 13.7149) & (cant_latitud <= 17.8188):


                    ## Iteracion sobre mtodos los elemntos de tiempo para poder iterrar sobre elllos y extraerlos todos
                    for cant_tiempo in range(inicio,fin):

                        ##  Condiciones para delimiar coordenadas deseadas
                        condicion_lon = (longitud[:] == cant_longitud)
                        condicion_lat = (latitud[:] == cant_latitud)

                        ## Extraccion de datos del archivo NetCDF seleccionado con las condiciones obtenidas en pasos anteriores
                        variable_climatica = (ds.variables[ 'rfe' ][cant_tiempo, condicion_lat, condicion_lon ])
                        exa=variable_climatica[0].compressed()
                        print(tiempo_utc[cant_tiempo],cant_longitud,cant_latitud,exa)


                        ## Captura en dataframe de informacion extraida de archivos NetCDF
                        nuevo_registro = {'fecha':tiempo_utc[cant_tiempo],'variable':'prcp','valor':exa,'lon':cant_longitud,'lat':cant_latitud,'fuente':'chirps'}
                        df_data:pd.DataFrame = pd.concat([df_data,pd.DataFrame([nuevo_registro])],ignore_index=True)

    df_data.to_csv( 'prec_chirps_' + str(inicio) + '_' + str(fin) + '.csv',index=False)


nombre_archivo = '/home/angelvalenzuela/EPS_tesis_regiones_climaticas/datos_crudos_fuente/ENACS/prec.nc'
ds = nc.Dataset(nombre_archivo)




# ## variables de los archivos NetCDF
tiempo = ds.variables['T']

units = 'months since 1960-01-01' # formato de fecha selecionado para la conversion
tiempo_utc = (nc.num2date( tiempo[:] , units,calendar='360_day')).astype(str)
fechas =  pd.to_datetime(tiempo_utc).format('%Y-%m-%d')  # cambio de formato de fechas



numero_elementos_fecha = len(tiempo)
print(numero_elementos_fecha)


# Create a new process
process = Process()


lista_p=[]

## Se empieza desde 3287 debido a que es el numero de dias entre el 1981-01-01 y 1990-01-01, ya que para el estudio solo se tomara en cuenta desde 1990 hasta 2020

lista_p.append(Process(target=salida_archivos_csv, args=( nombre_archivo, 0, int(numero_elementos_fecha*(1/6)) , ))) ## 0
lista_p.append(Process(target=salida_archivos_csv, args=(nombre_archivo, int(numero_elementos_fecha*(1/6)) , int(numero_elementos_fecha*(2/6)), ))) ## 1
lista_p.append(Process(target=salida_archivos_csv, args=(nombre_archivo, int(numero_elementos_fecha*(2/6)), int(numero_elementos_fecha*(3/6)), ))) ## 2
lista_p.append(Process(target=salida_archivos_csv, args=(nombre_archivo, int(numero_elementos_fecha*(3/6)), int(numero_elementos_fecha*(4/6)), ))) ## 3
lista_p.append(Process(target=salida_archivos_csv, args=(nombre_archivo, int(numero_elementos_fecha*(4/6)), int(numero_elementos_fecha*(5/6)), ))) ## 4
lista_p.append(Process(target=salida_archivos_csv, args=(nombre_archivo, int(numero_elementos_fecha*(5/6)), int(numero_elementos_fecha*(6/6)), ))) ## 5


lista_p[0].start()
lista_p[1].start()
lista_p[2].start()
lista_p[3].start()
lista_p[4].start()
lista_p[5].start()






