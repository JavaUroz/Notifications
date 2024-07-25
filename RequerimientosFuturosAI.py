#-*- coding: utf-8 -*-
import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import numpy as np

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
FROM [SBDACEST].[dbo].[ItemComp] ic
WHERE [ico_tipoIt] = 'A' 
  AND [ico_Desc] NOT IN ('-','.','--------------------------------------------------','Fletes y acarreos','Tractor Zanello Mod. 540 C,')
  AND [ico_Desc] NOT LIKE '%Acoplado rural tipo tolva%'
  AND [ico_Desc] NOT LIKE '%Metal desplegado%'
  AND [icoart_CodGen] NOT IN ('0202748','0000001')
  --AND ic.icocco_FEmision >= DATEADD(YEAR, -5, GETDATE())  -- Filtrar últimos 5 años

  -- Subconsulta para filtrar códigos de artículo comprados en los últimos 5 años
  AND ic.[icoart_CodGen] IN (
      SELECT DISTINCT [icoart_CodGen]
      FROM [SBDACEST].[dbo].[ItemComp] ic_sub
      WHERE [ic_sub].[ico_tipoIt] = 'A' 
        AND [ic_sub].[icocco_FEmision] >= DATEADD(YEAR, -5, GETDATE())  -- Filtrar últimos 5 años
  )

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

def entrenar_modelo_sklearn(datos):
    modelos = {}
    codigos_articulo = datos['Código'].unique()
    codigos_con_datos_insuficientes = []
    
    for codigo in codigos_articulo:
        datos_articulo = datos.loc[datos['Código'] == codigo].copy()
        
        # Convertir 'Año' y 'Mes' a formato numérico para el modelo
        datos_articulo['Fecha'] = datos_articulo['Año'] * 100 + datos_articulo['Mes']
        
        # Ordenar por fecha para asegurar monotonicidad
        datos_articulo.sort_values(by='Fecha', inplace=True)
        
        # Verificar si hay suficientes datos para entrenar el modelo
        if len(datos_articulo) >= 2:
            # Dividir datos en X (fecha) y y (Cantidad UM1)
            X = datos_articulo[['Fecha']].values.reshape(-1, 1)
            y = datos_articulo['Cantidad UM1'].values
            
            # Crear y entrenar modelo de regresión lineal
            model = LinearRegression()
            model.fit(X, y)
            
            # Guardar modelo entrenado
            modelos[codigo] = model
        else:
            codigos_con_datos_insuficientes.append(codigo)
            # Calcular promedio ponderado por cantidad UM1
            if len(datos_articulo) == 1:
                # Si solo hay un mes de datos, usar ese valor directamente
                promedio_um1 = datos_articulo['Cantidad UM1'].iloc[0]
            else:
                # Calcular promedio ponderado
                total_cantidad_um1 = datos_articulo['Cantidad UM1'].sum()
                pesos = datos_articulo['Cantidad UM1'] / total_cantidad_um1
                promedio_um1 = np.sum(datos_articulo['Cantidad UM1'] * pesos)
            
            modelos[codigo] = promedio_um1
    
    # Imprimir mensajes de advertencia
    if codigos_con_datos_insuficientes:
        print("\nAdvertencia: Los siguientes códigos tienen datos insuficientes para entrenar un modelo de regresión lineal. Se utilizará un promedio ponderado por cantidad UM1 para la estimación:")
        for codigo in codigos_con_datos_insuficientes:
            print(f"Código {codigo}")
    
    return modelos

def estimar_requerimientos_futuros(datos, modelos, meses_a_prever):
    proyeccion = []
    codigos_articulo = datos['Código'].unique()
    for codigo in codigos_articulo:
        datos_articulo = datos.loc[datos['Código'] == codigo].copy()
        
        # Obtener el último año y mes disponibles
        ultimo_anio = datos_articulo['Año'].max()
        ultimo_mes = datos_articulo.loc[datos_articulo['Año'] == ultimo_anio, 'Mes'].max()
        
        # Predecir para los próximos meses
        for i in range(1, meses_a_prever + 1):
            mes_proyectado = ultimo_mes + i if (ultimo_mes + i) <= 12 else (ultimo_mes + i) - 12
            anio_proyectado = ultimo_anio + (ultimo_mes + i - 1) // 12
            
            fecha_proyectada = anio_proyectado * 100 + mes_proyectado
            cantidad_um2 = datos_articulo['Cantidad UM2'].mean()
            
            # Predecir cantidad UM1 usando el modelo entrenado o el promedio ponderado
            if codigo in modelos and isinstance(modelos[codigo], LinearRegression):
                modelo = modelos[codigo]
                cantidad_um1 = modelo.predict([[fecha_proyectada]])[0]
            else:
                cantidad_um1 = modelos[codigo]  # Utilizar promedio ponderado
            
            # Obtener descripción del artículo
            descripcion = datos_articulo['Descripción'].iloc[0]
            
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
    
    # Cargar datos y entrenar modelos con sklearn
    df = pd.read_sql(query, conn)
    modelos = entrenar_modelo_sklearn(df)
    
    # Estimar requerimientos futuros
    meses_a_prever = 6
    proyeccion_requerimientos = estimar_requerimientos_futuros(df, modelos, meses_a_prever)
    
    # Mostrar y guardar resultados
    print("\nProyección de requerimientos futuros (utilizando Regresión Lineal de sklearn):")
    print(proyeccion_requerimientos)
    
    ruta_archivo_excel = 'Proyeccion_Requerimientos_sklearn.xlsx'
    proyeccion_requerimientos.to_excel(ruta_archivo_excel, index=False)
    print(f"\nArchivo Excel guardado en: {ruta_archivo_excel}")

except pyodbc.Error as e:
    print('Ocurrió un error al conectar a la base de datos:', e)

finally:
    conn.close()
