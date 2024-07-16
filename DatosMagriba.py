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
from openpyxl import load_workbook

# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = os.environ['CONNECTION_STRING']

# Definir la consulta SQL
query = '''
SELECT	CONVERT(varchar,[CCO_FEMISION], 103) AS FEC_EMISION
		,[CCO_CODPRO] AS COD_PROV
		,[CCOPRO_CUIT] AS CUIT
		,[CCOPRO_RAZSOC] AS RAZON_SOCIAL
		,[SPCTCO_COD] AS COD
		,[CCO_LETRA] AS LETRA
	 	,[CCO_CODPVT] AS P_VENTA
		,[CCO_NRO] AS NRO
		--,[SegDetC].[sdccon_Cod] AS COD_CONC
		--,MIN([SegDetC].[sdc_Desc]) AS DESC_CONC
		,CASE
			-- Cooperativa Electrica Colon
			WHEN [CCO_NRO] = '00010125' THEN 29720
			WHEN [CCO_NRO] = '00010039' THEN 28815
			WHEN [CCO_NRO] = '00009949' THEN 28113
			WHEN [CCO_NRO] = '00009853' THEN 20888
			WHEN [CCO_NRO] = '00009763' THEN 24711
			--Litoral Gas SA
			WHEN [CCO_NRO] = '00568345' THEN 57.87
			WHEN [CCO_NRO] = '00545428' THEN 280.39
			ELSE MAX([SegDetC].[sdc_CantUM1]) 
		END AS CANTIDAD
		,CASE 
			WHEN [CCOPRO_CUIT] = '30545748831' THEN 'KWH'
			WHEN [CCOPRO_CUIT] = '30657866330' THEN 'M3'
			ELSE 'MIN/DAT'
		END AS UNIDAD
		,ABS([CCO_IMPMONCC]) AS IMPORTE_TOT
      
  FROM [SBDACEST].[dbo].[QRY_COMPRASPAGOS]
  INNER JOIN [SegDetC] ON [QRY_COMPRASPAGOS].[CCOSCC_ID] = [SegDetC].[sdcscc_ID]

  WHERE [SPCTCO_COD] IS NOT NULL AND
		[SegDetC].[sdc_CantUM1] > 0 AND
		[SegDetC].[sdccon_Cod] IS NOT NULL AND
		YEAR([CCO_FEMISION]) = YEAR(GETDATE()) AND
		([CCO_CODPRO] IN ('005022') AND [CCO_CODPVT] = 12 OR
		[CCO_CODPRO] IN ('008417','008047','005387','005193'))

  GROUP BY [CCO_FEMISION]
	  ,[CCO_CODPRO]
	  ,[CCOPRO_CUIT]
	  ,[CCOPRO_RAZSOC]
      ,[SPCTCO_COD]
	  ,[CCO_LETRA]
      ,[CCO_CODPVT]
	  ,[CCO_NRO]	  
	  ,[mon_simboloCC]
      ,[CCO_IMPMONCC]

  ORDER BY [CCO_CODPRO], [CCO_FEMISION] DESC, [CCO_IMPMONCC]
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

# Ejecutar la consulta y cargar los resultados en un DataFrame de Pandas
df = pd.read_sql(query, conn)

# Cerrar el cursor y la conexión
cursor.close()
conn.close()


# Mostrar los primeros registros del DataFrame para verificar los datos
print(df.head())

# Ruta de Archivo Excel para exportar
ruta_archivo_excel = 'C:/Users/javie/OneDrive/Documentos/COMPARTIDOS/DocExcel/datos Magriba.xlsx'
df.to_excel(ruta_archivo_excel, index=False)


# Cargar el archivo Excel con openpyxl
wb = load_workbook(ruta_archivo_excel)
sheet = wb.active

# Ajustar el ancho de las columnas basado en el contenido de las celdas
for col in sheet.columns:
    max_length = 0
    column = col[0].column_letter  # Obtener la letra de la columna
    for cell in col:
        try:
            if len(str(cell.value)) > max_length:
                max_length = len(cell.value)
        except:
            pass
    adjusted_width = (max_length + 2) * 1.2  # Multiplicador para ajuste de ancho
    sheet.column_dimensions[column].width = adjusted_width

# Guardar los cambios en el archivo Excel
wb.save(ruta_archivo_excel)