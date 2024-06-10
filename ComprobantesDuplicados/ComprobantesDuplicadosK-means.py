#-*- coding: utf-8 -*-
import csv
import decimal
from math import radians
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
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import pandas as pd
import numpy as np


# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = os.environ['CONNECTION_STRING']

# Obtener la ruta absoluta del directorio del script actual
directorio_script = os.path.dirname(os.path.abspath(__file__))

# Cambiar al directorio del script
os.chdir(directorio_script)

print(os.getcwd())
print("K-means")

# Establecer la consulta SQL
sql_query_resultados = """                
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
                OR [CCOPRO_RAZSOC] LIKE '%A. HARTRODT%'
	            OR [CCOPRO_RAZSOC] LIKE '%ROSATTO%'
	            OR [CCOPRO_RAZSOC] LIKE '%PILA%'
	            OR [CCOPRO_RAZSOC] LIKE '%SOLANO%')
	            AND CCOTCO_COD NOT IN ('OP','CG','CIB') 
	            AND SPCTCO_COD NOT LIKE 'NULL'
				AND [CCO_FEMISION] >= DATEADD(MONTH, -3, DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()), 0))
				AND [CCO_SALDOMONCC] != 0

              ORDER BY [CCO_FEMISION] DESC
"""

sql_query_matriz_erroneos = """                
               SELECT
	               --[CCO_ID] AS Id,
	               --[CCOPRO_CUIT] AS cuit,
                   --CAST([CCOPRO_CODIN] AS INT) AS codPro,
	               --[CCOPRO_RAZSOC] AS razSoc,
                   --[SPCTCO_COD] AS codComp,
				   CASE 
						WHEN [SPCTCO_COD] LIKE '%FC%' THEN 1
						WHEN [SPCTCO_COD] LIKE '%NC%' THEN 2
						WHEN [SPCTCO_COD] LIKE '%ND%' THEN 3
						-- Agrega más casos según sea necesario
						ELSE 0 -- Valor por defecto si no coincide con ninguno de los casos anteriores
					END AS codComp,
	               --[CCO_LETRA] As letr,
				   CASE 
						WHEN [CCO_LETRA] LIKE '%A%' THEN 1
						WHEN [CCO_LETRA] LIKE '%C%' THEN 2
						-- Agrega más casos según sea necesario
						ELSE 0 -- Valor por defecto si no coincide con ninguno de los casos anteriores
					END AS letr,
                   CAST([CCO_CODPVT] AS INT) AS ptoVta,	  
                   CAST([CCO_NRO] AS INT) As nroComp,
                   --[CCO_FEMISION] AS fechaEmis,
	               --[CCO_FECMOD] AS fechaIngr,
				   --[SCCMTCA_CODIGO],
				   CASE 
						WHEN [SCCMTCA_CODIGO] LIKE '%UNI%' THEN 1
						WHEN [SCCMTCA_CODIGO] LIKE '%VEN%' THEN 2
						-- Agrega más casos según sea necesario
						ELSE 0 -- Valor por defecto si no coincide con ninguno de los casos anteriores
					END AS tipCbio
                   --ABS([CCO_IMPMONCC]) AS impMonCC
                   --ABS([CCO_SALDOMONCC]) AS monSald     
	               --[CCOPTR_COD] AS codPTrab,
                   --[PTR_DESC] AS desPTrab,
	               --[CCOUSU_CODIGO] AS UsuarioPago,
                   --[USU_NOMBRE] AS descUsuPago,
              FROM [SBDACEST].[dbo].[QRY_COMPRASPAGOS]

              WHERE
	            CCOTCO_COD NOT IN ('OP','CG','CIB') 
	            AND SPCTCO_COD NOT LIKE 'NULL'
	            AND [CCO_FEMISION] >= DATEADD(YEAR, -10, GETDATE())
				AND [CCO_SALDOMONCC] != 0

				--AND [CCO_IMPMONCC]=-43952.42
				--AND [CCO_IMPMONCC]>=-43960 AND [CCO_IMPMONCC]<=-43940
				--AND [CCO_NRO]='00076725'
				--AND [CCO_CODPVT] = '00015'

              ORDER BY [CCO_FEMISION] DESC
"""

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

    # Ejecutar la consulta SQL matriz erroneos
    cursor.execute(sql_query_matriz_erroneos)

    # Obtener los resultados
    resultados_erroneos = cursor.fetchall()
    
    # Ejecutar la consulta SQL resultados
    cursor.execute(sql_query_resultados)
    resultados = cursor.fetchall()
    
    resultados_tuplas = [tuple(row) for row in resultados_erroneos]
    
    # Crear un DataFrame basado en resultados_tuplas
    df = pd.DataFrame(resultados_tuplas, columns=['codComp', 'letr', 'ptoVta', 'nroComp', 'tipCbio'])

    # Normalizar los datos
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df)

    # Definir el número de clusters
    num_clusters = 6

    # Inicializar el modelo K-means
    kmeans = KMeans(n_clusters=num_clusters)

    # Entrenar el modelo
    kmeans.fit(df_scaled)

    # Obtener las etiquetas de los clusters para cada comprobante
    labels = kmeans.labels_

    # Agregar las etiquetas de los clusters al DataFrame
    df['cluster'] = labels       
                
    # Calcular los centroides de cada cluster
    centroides = kmeans.cluster_centers_

    # Calcular la distancia de cada comprobante al centroide de su cluster
    distancias = np.sqrt(np.sum((df_scaled - centroides[labels])**2, axis=1))

    # Establecer un umbral de distancia (por ejemplo, desviación estándar)
    umbral = 1  # Ajusta este valor según tu criterio

    # Identificar los comprobantes que están más allá del umbral como potencialmente erróneos
    comprobantes_erroneos = df[distancias > umbral]
    
    # cantidad_comp_erroneos = len(comprobantes_erroneos)
    # print("Cantidad de comprobantes erróneos capturados:", cantidad_comp_erroneos)
    
    # print("Comprobantes potencialmente erróneos:")
    # print(comprobantes_erroneos)

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
    for i, resultado in enumerate(resultados):
        if distancias[i] > umbral:
            idCompraPago = resultado[0]
            cuitProveedor = resultado[1]
            codProveedor = resultado[2]        
            razonSocial = resultado[3]
            codComprobante = resultado[4]            
            letrComprobante = resultado[5]
            puntoVenta = resultado[6]
            nroComprobante = resultado[7]
            if int(nroComprobante) in excepciones:            
                    continue
            fechaEmision = resultado[8]
            fechaIngreso = resultado[9]        
            importeCC = resultado[10]        
            importeSaldo = resultado[11]        
            codPuesto = resultado[12]
            descPuesto = resultado[13]
            codUsuPago = resultado[14]
            descUsuPago = resultado[15]
            
          
            # Formatear las fechas en el formato deseado 'dd/MM/aaaa'
            fechaEmision_formateada = fechaEmision.strftime('%d/%m/%Y')
            fechaIngreso_formateada = fechaIngreso.strftime('%d/%m/%Y')
            
            # limitar 25 caracteres razon social
            razonSocial_formateada = razonSocial[:15]
                        
            pagado = False
            if codUsuPago == 'JAVIER':
                pagado =  True
            
            print(f"\n{int(codProveedor)} {razonSocial_formateada}\t\t{codComprobante}\t{int(puntoVenta)}\t{int(nroComprobante)}\t{fechaEmision_formateada} {fechaIngreso_formateada}\t\t${importeCC}")
            
            respuesta = input(f"\n¿Agregar excepcion? (s/n): ")
            if respuesta.lower() == 's':
                agregar_a_csv(int(nroComprobante))  

except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()

input("\n\"Enter\" para salir...")