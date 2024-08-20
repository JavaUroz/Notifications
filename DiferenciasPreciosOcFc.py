#-*- coding: utf-8 -*-
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

# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = os.environ['CONNECTION_STRING']

# Establecer la consulta SQL
sql_query = """                
                SELECT DISTINCT
    cabecera.[sccpro_Cod],
    cabecera.[sccpro_CUIT],
    cabecera.[sccpro_RazSoc],
    detalle.[sdc_FRecep],
    detalle.[sdcart_CodGen],
    detalle.[sdc_Desc],	
    tiposPri.[spctco_Cod] AS [Cod_Pri],
    tiposPri.[spc_Nro] AS [Nro_Pri],
    tiposOrig.[spctco_Cod] AS [Cod_Orig],
    tiposOrig.[spc_Nro] AS [Nro_Orig],
    tipos.[spctco_Cod] AS [Cod],
    tipos.[spc_Nro] AS [Nro],	    
    detallePri.[sdc_PrecioUn] AS [sdc_PrecioUnPri],
    detalleOrig.[sdc_PrecioUn] AS [sdc_PrecioUnOrig],
    detalle.[sdc_PrecioUn]
FROM 
    [SBDACEST].[dbo].[SegRelDetC] relacion
INNER JOIN 
    [SegTiposC] tiposPri ON relacion.[srcscc_IDPri] = tiposPri.[spcscc_ID]
INNER JOIN 
    [SegTiposC] tiposOrig ON relacion.[srcscc_IDOrig] = tiposOrig.[spcscc_ID]
INNER JOIN 
    [SegTiposC] tipos ON relacion.[srcscc_ID] = tipos.[spcscc_ID]
INNER JOIN 
    [SegCabC] cabecera ON relacion.[srcscc_ID] = cabecera.[scc_ID]
INNER JOIN 
    [SegDetC] detalle ON relacion.[srcscc_ID] = detalle.[sdcscc_ID]
INNER JOIN 
    [SegDetC] detallePri ON relacion.[srcscc_IDPri] = detallePri.[sdcscc_ID] AND detallePri.[sdcart_CodGen] = detalle.[sdcart_CodGen] AND detallePri.[sdc_NReng] = detalle.[sdc_NReng]
INNER JOIN 
    [SegDetC] detalleOrig ON relacion.[srcscc_IDOrig] = detalleOrig.[sdcscc_ID] AND detalleOrig.[sdcart_CodGen] = detalle.[sdcart_CodGen] AND detalleOrig.[sdc_NReng] = detalle.[sdc_NReng]
WHERE 
    tiposPri.[spctco_Cod] IN ('OC', 'OCR', 'OCP', 'RT') 
    AND tipos.[spctco_Cod] NOT LIKE '%RT%'
    AND ((tipos.[spctco_Cod] = 'FC' AND detalle.[sdc_PrecioUn] > detallePri.[sdc_PrecioUn] * 1.03)OR 
                (tipos.[spctco_Cod] != 'NC' AND NOT
                                ((detalleOrig.[sdc_PrecioUn] BETWEEN detalle.[sdc_PrecioUn] * 0.97 AND detalle.[sdc_PrecioUn] * 1.03)
                                OR
                                (detallePri.[sdc_PrecioUn] BETWEEN (detalleOrig.[sdc_PrecioUn] - detalle.[sdc_PrecioUn]) * 0.97
				                                AND (detalleOrig.[sdc_PrecioUn] - detalle.[sdc_PrecioUn]) * 1.02))))
    AND detalle.[sdc_FRecep] >= DATEADD(MONTH, -3, GETDATE()) 
    AND YEAR(detalle.[sdc_FechaOC]) = YEAR(GETDATE())
    AND detalle.[sdc_ImpTot] != 0
    AND detallePri.[sdc_ImpTot] != 0
    AND detalleOrig.[sdc_ImpTot] != 0	
    
ORDER BY 
    detalle.[sdc_FRecep] DESC;
"""

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
  <h2>PRECIOS EXCEDIDOS DE OC A FC</h2>  
  <table>
    <tr class="cabecera">
      <th class="cabecera-text">EMPRESA</th>
      <th class="cabecera-text">F. RECEP</th>
      <th class="cabecera-text">ARTICULO    </th>      
      <th class="cabecera-text">ORDEN COMPRA / REMITO</th>
      <th class="cabecera-text">PRECIO UNIT.</th>
      <th class="cabecera-text">FACTURA / N. CREDITO</th>
      <th class="cabecera-text">PRECIO UNIT.</th>
      <th class="cabecera-text">DIFERENCIA (%)</th>
    </tr>
"""

# Inicia la conexión
try:
    # Establecer la conexión con la base de datos
    conexion = pyodbc.connect(connection_string)

    # Crear un cursor para ejecutar la consulta SQL
    cursor = conexion.cursor()

    # Ejecutar la consulta SQL
    cursor.execute(sql_query)

    # Obtener los resultados
    resultados = cursor.fetchall()

    # Procesar los resultados
    for resultado in resultados:
        codProveedor = resultado[0]
        cuitProveedor = resultado[1]
        razonSocial = resultado[2]
        fechaRecepcion = resultado[3]
        codArticulo = resultado[4]
        descArticulo = resultado[5]
        codCompPri = resultado[6]
        nroCompPri = resultado[7]
        codCompOrig = resultado[8]
        nroCompOrig = resultado[9]
        codComp = resultado[10]
        nroComp = resultado[11]
        
        # region Exclusiones
        if (nroComp == '00000912' or 
            nroComp == '00000911' or 
            nroComp == '00033758' or
            nroComp == '00008737'):
            continue
        # endregion

        precioUnitPri = resultado[12]
        precioUnitOrig = resultado[13]
        precioUnit = resultado[14]
        
        codArtFormat = codArticulo[:2] + '.' + codArticulo[2:4] + '.' + codArticulo[4:7]
        
        fechaActual = datetime.now()
        
        diferenciaPorc =  ((precioUnitPri / precioUnit * 100) - 100)*(-1) 

        if diferenciaPorc > 50:
            contenido_html += f"""
                         <tr>
                           <td>({int(codProveedor)}) {razonSocial}</td>
                           <td>{fechaRecepcion.strftime('%d/%m/%Y')}</td>  
                           <td>({codArtFormat}) - {descArticulo}</td>
                           <td>{codCompPri} {int(nroCompPri)}</td>
                           <td>$ {int(precioUnitPri)}</td>
                           <td>{codComp} {int(nroComp)}</td>
                           <td>$ {int(precioUnit)}</td>                                                                   
                           <td class="excedido-container"><span class="excedido-text"><b>↑ {int(diferenciaPorc)}%</b></td>
                         </tr>
                     """
            
        elif diferenciaPorc > 30:
            contenido_html += f"""                                                
                        <tr>
                           <td>({int(codProveedor)}) {razonSocial}</td>
                           <td>{fechaRecepcion.strftime('%d/%m/%Y')}</td>  
                           <td>({codArtFormat}) - {descArticulo}</td>
                           <td>{codCompPri} {int(nroCompPri)}</td>
                           <td>$ {int(precioUnitPri)}</td>
                           <td>{codComp} {int(nroComp)}</td>
                           <td>$ {int(precioUnit)}</td>   
                           <td class="advertencia-container"><span class="advertencia-text"><b>↑ {int(diferenciaPorc)}%</b></td>                           
                         </tr>
                     """           
        elif diferenciaPorc > 3:
            contenido_html += f"""
                        <tr>
                           <td>({int(codProveedor)}) {razonSocial}</td>
                           <td>{fechaRecepcion.strftime('%d/%m/%Y')}</td>  
                           <td>({codArtFormat}) - {descArticulo}</td>
                           <td>{codCompPri} {int(nroCompPri)}</td>
                           <td>$ {int(precioUnitPri)}</td>
                           <td>{codComp} {int(nroComp)}</td>
                           <td>$ {int(precioUnit)}</td>   
                           <td class="aviso-container"><span class="aviso-text"><b>↑ {int(diferenciaPorc)}%</b></td>
                         </tr>
                     """        
        i += 1
    
    contenido_html += """
      </table>
    </body>
    </html>
    """
    
except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)

remitente = 'javieruroz@imcestari.com'
destinatario  = ['javieruroz@imcestari.com', 'mcelli@imcestari.com']
# destinatario  = ['javieruroz@imcestari.com']
asunto = 'Diferencias precios OC-FC'
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
