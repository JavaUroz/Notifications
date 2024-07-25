#-*- coding: utf-8 -*-
import pyodbc
import pandas as pd
import os
from tokenize import Double
from xml.dom.minidom import TypeInfo
from xmlrpc.client import DateTime
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import dotenv
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = os.environ['CONNECTION_STRING']

# Definir la consulta SQL
query = '''
SELECT [icoart_CodGen] as [Código]
      ,[ico_Desc] as [Descripción]      
      ,SUM([ico_CantUM1]) as [Cantidad UM1]
      ,SUM([ico_CantUM2]) as [Cantidad UM2]
	  ,DATENAME(MONTH, [icocco_FEmision]) AS [Mes]
	  ,YEAR([icocco_FEmision]) as [Año]
  FROM [dbo].[ItemComp]
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
  ORDER BY Año desc, MONTH([icocco_FEmision]) desc, [icoart_CodGen]
'''

# Inicia la conexión
try:
    # Establecer la conexión con la base de datos
    conn = pyodbc.connect(connection_string)

    # Crear un cursor para ejecutar la consulta SQL
    cursor = conn.cursor()

    # Ejecutar la consulta SQL
    cursor.execute(query)

    # Obtener los resultados
    resultados = cursor.fetchall()
    
except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)


# Cerrar el cursor y la conexión
cursor.close()
conn.close()

# Ejecutar la consulta y cargar los resultados en un DataFrame de Pandas
df = pd.read_sql(query, conn)

# Cerrar la conexión
conn.close()


# Mostrar los primeros registros del DataFrame para verificar los datos
print(df.head())

# Aquí puedes realizar cualquier procesamiento adicional con los datos cargados en df
# Por ejemplo, calcular requerimientos futuros o preparar el reporte final

# Ejemplo de cálculo de requerimientos futuros basado en el promedio histórico
# Supongamos que quieres estimar el consumo para el próximo mes
# Puedes agregar lógica adicional aquí según tus necesidades específicas
# Este es solo un ejemplo ilustrativo

# Calcular el promedio de consumo mensual
promedio_consumo_mensual = df.groupby(['Año', 'Mes']).mean().reset_index()

# Estimar el consumo para el próximo mes (por ejemplo, usando el último mes registrado)
ultimo_mes = promedio_consumo_mensual.iloc[0]  # Suponiendo que el primer registro es el último mes
consumo_estimado_proximo_mes = {
    'Mes': ultimo_mes['Mes'],
    'Año': ultimo_mes['Año'],
    'Cantidad UM1': ultimo_mes['Cantidad UM1'] * 1.1,  # Ejemplo: Aumentar en un 10%
    'Cantidad UM2': ultimo_mes['Cantidad UM2'] * 1.1
}

# Imprimir el consumo estimado para el próximo mes
print("\nConsumo estimado para el próximo mes:")
print(consumo_estimado_proximo_mes)

# Aquí podrías preparar el reporte final, por ejemplo, guardar los resultados en un archivo CSV o Excel
# O mostrarlos en una tabla para visualización o exportación