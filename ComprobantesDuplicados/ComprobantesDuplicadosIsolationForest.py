from re import I
from sklearn.ensemble import IsolationForest
import os
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import dotenv
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from datetime import datetime, timedelta
import csv

# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = os.environ['CONNECTION_STRING']

# Obtener la ruta absoluta del directorio del script actual
directorio_script = os.path.dirname(os.path.abspath(__file__))

# Cambiar al directorio del script
os.chdir(directorio_script)

print(os.getcwd())
print("Isolation-Forest")

# Establecer la consulta SQL
sql_query_resultados_completos = """                
               SELECT
	               [CCO_ID] AS Id,
	               [CCOPRO_CUIT] AS cuit,
                   [CCOPRO_CODIN] AS codPro,
	               [CCOPRO_RAZSOC] AS razSoc,
                   [SPCTCO_COD] AS codComp,
	               [CCO_LETRA] As letr,
                   [CCO_CODPVT] AS ptoVta,	  
                   [CCO_NRO] As nroComp,
                   [CCO_FEMISION] AS fechaEmis,
	               [CCO_FECMOD] AS fechaIngr,
                   [CCO_IMPMONCC] AS impMonCC,
                   [CCO_SALDOMONCC] AS monSald,      
	               [CCOPTR_COD] AS codPTrab,
                   [PTR_DESC] AS desPTrab,
	               [CCOUSU_CODIGO] AS UsuarioPago,
                   [USU_NOMBRE] AS descUsuPago
              FROM [SBDACEST].[dbo].[QRY_COMPRASPAGOS]

              WHERE
	            ([CCOPRO_RAZSOC] LIKE '%GAS PIC%'
	            OR [CCOPRO_RAZSOC] LIKE '%TRANSPORTE ESPINOSA S.R.L.%'
	            OR [CCOPRO_RAZSOC] LIKE '%IROS TOUR%'
	            OR [CCOPRO_RAZSOC] LIKE '%CR COMISIONES%'
	            OR [CCOPRO_RAZSOC] LIKE '%ROSATTO%'
	            OR [CCOPRO_RAZSOC] LIKE '%PILA%'
                OR [CCOPRO_RAZSOC] LIKE '%REPUEST%'
	            OR [CCOPRO_RAZSOC] LIKE '%SOLANO%')
	            AND CCOTCO_COD NOT IN ('OP','CG','CIB') 
	            AND SPCTCO_COD NOT LIKE 'NULL'
				AND [CCO_FEMISION] >= DATEADD(YEAR, -5, GETDATE())
				AND [CCO_SALDOMONCC] != 0

              ORDER BY [CCO_FEMISION] DESC
"""

# Establecer la consulta SQL
sql_query_resultados = """
    SELECT
           [CCO_ID] AS Id,	               
           CAST([CCOPRO_CODIN] AS INT) AS codPro,
	       CASE 
	    		WHEN [SPCTCO_COD] LIKE '%FC%' THEN 1
	    		WHEN [SPCTCO_COD] LIKE '%NC%' THEN 2
	    		WHEN [SPCTCO_COD] LIKE '%ND%' THEN 3	    		
	    		ELSE 0 
	       END AS codComp,	              
	       CASE 
	    		WHEN [CCO_LETRA] LIKE '%A%' THEN 1
	    		WHEN [CCO_LETRA] LIKE '%C%' THEN 2	    		
	    		ELSE 0
	       END AS letr,
           CAST([CCO_CODPVT] AS INT) AS ptoVta,	  
           CAST([CCO_NRO] AS INT) As nroComp,	      
	       CASE 
	    		WHEN [SCCMTCA_CODIGO] LIKE '%UNI%' THEN 1
	    		WHEN [SCCMTCA_CODIGO] LIKE '%VEN%' THEN 2	    		
	    		ELSE 0
	    	END AS tipCbio,
            ABS([CCO_IMPMONCC]) AS impMonCC,
            [CCO_FEMISION] AS fechaEmision  
    FROM [SBDACEST].[dbo].[QRY_COMPRASPAGOS]
    WHERE
        ([CCOPRO_RAZSOC] LIKE '%GAS PIC%'
        OR [CCOPRO_RAZSOC] LIKE '%TRANSPORTE ESPINOSA S.R.L.%'
        OR [CCOPRO_RAZSOC] LIKE '%IROS TOUR%'
        OR [CCOPRO_RAZSOC] LIKE '%CR COMISIONES%'
        OR [CCOPRO_RAZSOC] LIKE '%ROSATTO%'
        OR [CCOPRO_RAZSOC] LIKE '%PILA%'
        OR [CCOPRO_RAZSOC] LIKE '%REPUEST%'
        OR [CCOPRO_RAZSOC] LIKE '%SOLANO%')
        AND CCOTCO_COD NOT IN ('OP','CG','CIB') 
        AND SPCTCO_COD NOT LIKE 'NULL'
        AND [CCO_FEMISION] >= DATEADD(YEAR, -5, GETDATE())
        AND [CCO_SALDOMONCC] != 0
    ORDER BY [CCO_FEMISION] DESC
"""


# Inicializar el modelo Isolation Forest con parámetros ajustados
modelo = IsolationForest(n_estimators=100, contamination=0.5, random_state=42)

# Calcular la fecha actual y la fecha hace 5 años
fecha_actual = datetime.now()

# Función para agregar excepciones al archivo CSV
def agregar_a_csv(numero):
    with open('excepciones.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([numero])

# Inicia la conexión
try:
    # Establecer la conexión con la base de datos
    conexion = pyodbc.connect(connection_string)
    
    # Crear un cursor para ejecutar la consulta SQL
    cursor = conexion.cursor()
    
    # Ejecutar la consulta SQL resultados completos
    cursor.execute(sql_query_resultados_completos)
    resultados_completos = cursor.fetchall()

    # Ejecutar la consulta SQL resultados
    cursor.execute(sql_query_resultados)
    resultados = cursor.fetchall()   

    resultados_tuplas = [tuple(row) for row in resultados]

    # Crear un DataFrame basado en resultados
    df = pd.DataFrame(resultados_tuplas, columns=['Id', 'codPro', 'codComp', 'letr', 'ptoVta', 'nroComp', 'tipCbio', 'impMonCC', 'fechaEmision'])
    
    # Filtrar los datos para los últimos 3 meses
    fecha_limite = fecha_actual - timedelta(days=3*30)
    df_filt = df[df['fechaEmision'] >= fecha_limite]

    # Normalizar los datos
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df_filt[['codPro', 'codComp', 'letr', 'ptoVta', 'nroComp', 'tipCbio', 'impMonCC']])

    # Entrenar el modelo
    modelo.fit(df_scaled)

    # Obtener las etiquetas de anomalía para cada comprobante
    labels = modelo.predict(df_scaled)

    # Identificar los comprobantes potencialmente erróneos
    comprobantes_erroneos_indices = np.where(labels == -1)[0]
    comprobantes_erroneos = df.iloc[comprobantes_erroneos_indices]
    
    # Excepciones
    excepciones = []
    
    # Ruta del archivo CSV
    archivo_csv = 'excepciones.csv' 
    
    # Verificar si el archivo CSV existe
    if not os.path.exists(archivo_csv):
        with open(archivo_csv, 'w', newline='') as file:
            pass        
   
    # Leer los números de comprobante desde el archivo CSV y almacenarlos en la lista de excepciones
    excepciones = []
    with open(archivo_csv, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            for item in row:  # Iterar sobre los elementos de la fila
                excepciones.append(int(item.strip()))  # Eliminar espacios en blanco y convertir a entero    
    
    # Cabecera
    print("COMPROBANTES A REVISAR (ULTIMOS 3 MESES)\n")
    print("----------------------------------------------------------------------------------------------------------")
    print("Proveedor\t\t\t\tComprobante\t\tF. Emision Registracion\t\tImporte")
    print("----------------------------------------------------------------------------------------------------------")
    
    # Procesar los resultados
    for j, resultado_completo in enumerate(resultados_completos):        
        for i, resultado in enumerate(comprobantes_erroneos.values):
            if resultado_completo[0] == resultado[0]:
                idCompraPago = resultado[0]
                codProveedor = resultado[1]
                razonSocial = resultado_completo[3]
                codComprobante = resultado_completo[4]
                letrComprobante = resultado_completo[5]
                puntoVenta = resultado[4]
                nroComprobante = resultado[5]
                if nroComprobante in excepciones:            
                    continue
                tipoCambio = resultado[6]     
                importeCC = resultado[7]
                fechaEmision = resultado_completo[8]
                fechaIngreso = resultado_completo[9]
                
                # Formatear las fechas en el formato deseado 'dd/MM/aaaa'
                fechaEmision_formateada = fechaEmision.strftime('%d/%m/%Y')
                fechaIngreso_formateada = fechaIngreso.strftime('%d/%m/%Y')
                
                # limitar 25 caracteres razon social
                razonSocial_formateada = razonSocial[:25]

            else:
                continue
            print(f"\n{int(codProveedor)} {razonSocial_formateada}\t\t{codComprobante}\t{int(puntoVenta)}\t{int(nroComprobante)}\t{fechaEmision_formateada} {fechaIngreso_formateada}\t\t${importeCC}") 
            # Loop sobre los números de comprobante
            
            respuesta = input(f"\n¿Agregar excepcion? (s/n): ")
            if respuesta.lower() == 's':
                agregar_a_csv(int(nroComprobante))
                
    print("----------------------------------------------------------------------------------------------------------")

except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)
finally:
    # Cerrar el cursor y la conexión
    cursor.close()
    conexion.close()
    
input("\n\"Enter\" para salir...")