#-*- coding: utf-8 -*-
import decimal
from math import radians
import os
from tokenize import Double
from xml.dom.minidom import TypeInfo
from xmlrpc.client import DateTime
from twilio.rest import Client, content
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
                   CAST([CCOPRO_CODIN] AS INT) AS codPro,
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
					END AS tipCbio,
                   ABS([CCO_IMPMONCC]) AS impMonCC
                   --ABS([CCO_SALDOMONCC]) AS monSald     
	               --[CCOPTR_COD] AS codPTrab,
                --   [PTR_DESC] AS desPTrab,
	               --[CCOUSU_CODIGO] AS UsuarioPago,
                --   [USU_NOMBRE] AS descUsuPago
              FROM [SBDACEST].[dbo].[QRY_COMPRASPAGOS]

              WHERE
	            ([CCOPRO_RAZSOC] LIKE '%GAS PIC%'
	            OR [CCOPRO_RAZSOC] LIKE '%TRANSPORTE ESPINOSA S.R.L.%'
	            OR [CCOPRO_RAZSOC] LIKE '%IROS TOUR%'
	            OR [CCOPRO_RAZSOC] LIKE '%CR COMISIONES%'
	            OR [CCOPRO_RAZSOC] LIKE '%ROSATTO%'
	            OR [CCOPRO_RAZSOC] LIKE '%PILA%'
				OR [CCOPRO_RAZSOC] LIKE '%HART%'
	            OR [CCOPRO_RAZSOC] LIKE '%SOLANO%')
	            AND CCOTCO_COD NOT IN ('OP','CG','CIB') 
	            AND SPCTCO_COD NOT LIKE 'NULL'
	            AND [CCO_FEMISION] >= DATEADD(YEAR, -5, GETDATE())
				AND [CCO_SALDOMONCC] != 0

				--AND [CCO_IMPMONCC]=-43952.42
				--AND [CCO_IMPMONCC]>=-43960 AND [CCO_IMPMONCC]<=-43940
				--AND [CCO_NRO]='00076725'
				--AND [CCO_CODPVT] = '00015'

              ORDER BY [CCO_FEMISION] DESC
"""

# Establecer cliente con credenciales de SID y Token de Twilio 
account_sid = os.environ['ACCOUNT_SID']
auth_token = os.environ['AUTH_TOKEN']
client = Client(account_sid, auth_token)
mensaje_plantilla = ''
i = 1
indices_acumulados = []
contenido_html = """
<html>
<head>
  <style>
    table {
      border-collapse: collapse;
      width: 100%;
    }
    th, td {
      border: 1px solid #E5E7E9;
      padding: 8px;
      text-align: left;
    }
    .cabecera{
       background-color: #566573       
    }
    .cabecera-text{
       color: #FDFEFE
    }
    .excedido-container{
       background-color: #CC0000;
    }
    .excedido-text{
       color: #FFFFFF;
    }
    .advertencia-container{
       background-color: #FF6600;
    }
    .advertencia-text{
       color: #FFFFF3;
    }
    .aviso-container{
       background-color: #FFFF99;
    }
    .aviso-text{
       color: #222222;
    }
    .favor-container{
       background-color: #CCFF66;
    }
    .favor-text{
       color: #222222;
    }
  </style>
</head>
<body>
  <h2>COMROBANTES MAL REGISTRADOS</h2>
  <table>
    <tr class="cabecera">
      <th class="cabecera-text">EMPRESA</th>
      <th class="cabecera-text">COMPROBANTE</th>
      <th class="cabecera-text">F. RECEP</th>            
      <th class="cabecera-text">IMPORTE</th>
      <th class="cabecera-text">SALDO</th>
      <th class="cabecera-text">REGISTRADO</th>
      <th class="cabecera-text">APLICADO</th>
      <th class="cabecera-text">ESTADO</th>
    </tr>
"""


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
    df = pd.DataFrame(resultados_tuplas, columns=['codPro', 'codComp', 'letr', 'ptoVta', 'nroComp', 'tipCbio', 'impMonCC'])

    # Normalizar los datos
    scaler = StandardScaler()
    df_scaled = scaler.fit_transform(df)

    # Definir el número de clusters
    num_clusters = 2

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
    
    cantidad_comp_erroneos = len(comprobantes_erroneos)
    print("Cantidad de comprobantes erróneos capturados:", cantidad_comp_erroneos)
    
    print("Comprobantes potencialmente erróneos:")
    print(comprobantes_erroneos)

    breakIt=True
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
                        
            pagado = False
            if codUsuPago == 'JAVIER':
                pagado =  True
        
            contenido_html += f"""
                             <tr>
                               <td>{int(codProveedor)} {cuitProveedor} {razonSocial}</td>
                               <td>{codComprobante} {letrComprobante} {puntoVenta} {nroComprobante}</td>
                               <td>{fechaEmision_formateada} / {fechaIngreso_formateada}</td>            
                               <td>$ {round(importeCC, 2)}</td>
                               <td>$ {round(importeSaldo, 2)}</td>
                               <td>{descPuesto}</td>
                               <td>{descUsuPago}</td>
                               <td>{"PAGADO" if pagado else "NO PAGADO"}</td>
                         """
            # mensaje_plantilla += f'{i}) *{int(codProveedor)} {razonSocial[:25]}* {codCompPri}{nroCompPri}/{codComp}{nroComp} * {int(diferenciaPorc)} %*\n'
            
            i += 1
            
    # mensaje_completo = f'DIFERENCIA PRECIOS OC/FC:\n  PROVEEDOR    -    COMPROBANTES    -   DIFERENCIA   -\n{mensaje_plantilla}'
    
    contenido_html += """
      </table>
    </body>
    </html>
    """
    
    # len_mensaje = len(mensaje_completo)
    
    # try:    
    #     # Llamar a la función para enviar el mensaje Javier Uroz
    #     enviar_mensaje_whatsapp('+5492473501336', mensaje_completo)
    # except Exception as e:
    #     print('Error al enviar mensaje: \n',e)
        
    # # Prueba en cmd
    # print(mensaje_completo)

except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)

remitente = 'no-reply@imcestari.com'
# destinatario  = ['javieruroz@imcestari.com', 'mcelli@imcestari.com']
destinatario  = ['javieruroz@imcestari.com']
asunto = 'Comprobantes con posibles errores'
msg = contenido_html

mensaje = MIMEMultipart()

mensaje['From'] = remitente
mensaje['To'] = ", ".join(destinatario)
mensaje['Subject'] = asunto

mensaje.attach(MIMEText(contenido_html, 'html'))

# Datos
username = os.environ['USERNAME']
password = os.environ['PASSWORD']

# Enviando el correo
server = smtplib.SMTP_SSL('px000056.ferozo.com:465')
# server.starttls()
username=remitente
server.login(username,password)
try:
    server.sendmail(remitente, destinatario, mensaje.as_string())
    server.quit()
    print('E- mail enviado exitosamente!')
except Exception as e:
    print('Ha ocurrido un error:\n', e)

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()
