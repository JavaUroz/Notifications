from statsmodels.tsa.arima.model import ARIMA
import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv


# Cargar librería para .env
load_dotenv()

# Establecer el connection string
connection_string = os.getenv('CONNECTION_STRING')

# Definir la consulta SQL
query = '''
      SELECT [icoart_CodGen] as [Código]
          ,[ico_Desc] as [Descripción]      
          ,SUM([ico_CantUM1]) as [Cantidad UM1]
          ,SUM([ico_CantUM2]) as [Cantidad UM2]
	      ,MONTH([icocco_FEmision]) AS [Mes]
	      ,YEAR([icocco_FEmision]) as [Año]
      FROM [SBDACEST].[dbo].[ItemComp]

      WHERE ico_tipoIt = 'A' AND
		    ico_Desc NOT IN ('-','.','--------------------------------------------------','Acoplado rural tipo tolva')
  
      GROUP BY  
	       YEAR([icocco_FEmision])
	      ,MONTH([icocco_FEmision])
	      ,DATENAME(MONTH, [icocco_FEmision])
	      ,[ico_tipoIt]
	      ,[icoart_CodGen]
          ,[ico_Desc]
          ,[ico_TipoArt]
      HAVING SUM([ico_CantUM1]) > 0 OR
		     SUM([ico_CantUM2]) > 0

      ORDER BY Año desc, Mes desc, [icoart_CodGen]
'''

def entrenar_modelo_arima(datos):
    # Entrenar un modelo ARIMA para cada código de artículo
    modelos = {}
    codigos_articulo = datos['Código'].unique()
    for codigo in codigos_articulo:
        datos_articulo = datos[datos['Código'] == codigo]
        serie_tiempo = datos_articulo[['Año', 'Mes', 'Cantidad UM1']]  # Usar Cantidad UM1 para ejemplo
        modelo = ARIMA(serie_tiempo['Cantidad UM1'], order=(1, 1, 1))  # Ajustar orden según análisis
        modelo_entrenado = modelo.fit()
        modelos[codigo] = modelo_entrenado
    return modelos

def estimar_requerimientos_futuros(datos, modelos, meses_a_prever):
    proyeccion = []
    codigos_articulo = datos['Código'].unique()
    for codigo in codigos_articulo:
        modelo = modelos[codigo]
        ultimo_anio = datos['Año'].max()
        ultimo_mes = datos[datos['Año'] == ultimo_anio]['Mes'].max()
        
        for i in range(1, meses_a_prever + 1):
            mes_proyectado = ultimo_mes + i if (ultimo_mes + i) <= 12 else (ultimo_mes + i) - 12
            anio_proyectado = ultimo_anio + (ultimo_mes + i - 1) // 12
            
            # Predecir cantidad UM1
            predicciones = modelo.predict(start=len(datos), end=len(datos) + i)
            cantidad_um1 = predicciones[-1]  # Tomar la última predicción como proyección
            
            # Obtener descripción y cantidad UM2
            datos_articulo = datos[datos['Código'] == codigo]
            descripcion = datos_articulo['Descripción'].iloc[0]
            cantidad_um2 = datos_articulo['Cantidad UM2'].mean()
            
            proyeccion.append({
                'Año': anio_proyectado,
                'Mes': mes_proyectado,
                'Código': codigo,
                'Descripción': descripcion,
                'Cantidad UM1': cantidad_um1,
                'Cantidad UM2': cantidad_um2
            })
    
    return pd.DataFrame(proyeccion)

# Ejemplo de uso
try:
    # Establecer la conexión con la base de datos
    conn = pyodbc.connect(connection_string)
    # Cargar datos y modelos
    df = pd.read_sql(query, conn)
    modelos = entrenar_modelo_arima(df)
    
    # Estimar requerimientos futuros
    meses_a_prever = 6
    proyeccion_requerimientos = estimar_requerimientos_futuros(df, modelos, meses_a_prever)
    
    # Mostrar y guardar resultados
    print("\nProyección de requerimientos futuros (utilizando ARIMA):")
    print(proyeccion_requerimientos)
    
    ruta_archivo_excel = 'Proyeccion_Requerimientos.xlsx'
    proyeccion_requerimientos.to_excel(ruta_archivo_excel, index=False)
    print(f"\nArchivo Excel guardado en: {ruta_archivo_excel}")

except pyodbc.Error as e:
    print('Ocurrió un error al conectar a la base de datos:', e)

finally:
    conn.close()
