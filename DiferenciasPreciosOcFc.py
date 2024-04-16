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

# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = "Driver={SQL Server};Server=SERVIDOR;Database=SBDACEST;UID=sa;PWD=Sa2008R2;"

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
                    [SegDetC] detallePri ON relacion.[srcscc_IDPri] = detallePri.[sdcscc_ID] AND detallePri.[sdcart_CodGen] = detalle.[sdcart_CodGen]
                INNER JOIN 
                    [SegDetC] detalleOrig ON relacion.[srcscc_IDOrig] = detalleOrig.[sdcscc_ID] AND detalleOrig.[sdcart_CodGen] = detalle.[sdcart_CodGen]
                WHERE 
                    tiposPri.[spctco_Cod] = 'OC' 
                    AND tipos.[spctco_Cod] = 'FC'
                    AND detalle.[sdc_FRecep] <= DATEADD(YEAR, 1, GETDATE()) 
                    AND YEAR(detalle.[sdc_FechaOC]) = YEAR(GETDATE())
                    AND detalle.[sdc_ImpTot] != 0
                    AND detallePri.[sdc_ImpTot] != 0
                    AND detalleOrig.[sdc_ImpTot] != 0
                    AND detalle.[sdc_PrecioUn] > detallePri.[sdc_PrecioUn] * 1.2	
                ORDER BY 
                    detalle.[sdc_FRecep] DESC;
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
       background-color: #FA8C05;
    }
    .excedido-text{
       color: #FDFEFE;
       font-style:italic;
    }
    .advertencia-container{
       background-color: #FFDF80;
    }
    .advertencia-text{
       color: #17202A;
    }    
  </style>
</head>
<body>
  <h2>PRECIOS EXCEDIDOS DE OC A FC</h2>
  <table>
    <tr class="cabecera">
      <th class="cabecera-text">Empresa</th>
      <th class="cabecera-text">F. Recepcion</th>
      <th class="cabecera-text">Articulo</th>      
      <th class="cabecera-text">Orden Compra</th>
      <th class="cabecera-text">Precio unit.</th>
      <th class="cabecera-text">Factura</th>
      <th class="cabecera-text">Precio unit.</th>
      <th class="cabecera-text">Dif. Porc.</th>
    </tr>
"""
# Función para enviar mensaje de WhatsApp
def enviar_mensaje_whatsapp(destinatario, mensaje):
    # Split the message into chunks of 1600 characters or less
    message_chunks = [mensaje[i:i+1600] for i in range(0, len(mensaje), 1600)]
    for contador, chunk in enumerate(message_chunks):                
        try:
            message = client.messages.create(
                                      from_='whatsapp:+14155238886',
                                      body = chunk,                              
                                      to=f'whatsapp:{destinatario}'
                                  )
            print('Mensaje enviado correctamente:', message.sid)
            if contador >= 1:
                message = client.messages.create(
                                      from_='whatsapp:+14155238886',
                                      body = f'Se excedio la cantidad de mensajes permitidos({contador + 1})\nEl cuadro completo se enviara a las casillas de correo asignadas.',                              
                                      to=f'whatsapp:{destinatario}'
                                  )
                print(f'Se alcanzo el l\u00EDmite de mensajes permitidos de ({len(message_chunks) - 1}):\n')
                break                
        except Exception as e:
            print(f'Ha ocurrido un error:\n', e)

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
                           <td class="excedido-container"><span class="excedido-text"><b>↑{int(diferenciaPorc)}%</b></td>
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
                           <td class="advertencia-container"><span class="advertencia-text"><b>↑{int(diferenciaPorc)}%</b></td>                           
                         </tr>
                     """
        else:
            contenido_html += f"""
                         <tr>
                           <td>({int(codProveedor)}) {razonSocial}</td>
                           <td>{fechaRecepcion.strftime('%d/%m/%Y')}</td>  
                           <td>({codArtFormat}) - {descArticulo}</td>
                           <td>{codCompPri} {int(nroCompPri)}</td>
                           <td>$ {int(precioUnitPri)}</td>
                           <td>{codComp} {int(nroComp)}</td>
                           <td>$ {int(precioUnit)}</td>                       
                           <td><b>↑{int(diferenciaPorc)}%</b></td>
                         </tr>
                     """ 
        mensaje_plantilla += f'{i}) *{int(codProveedor)} {razonSocial[:25]}* {codCompPri}{nroCompPri}/{codComp}{nroComp} *↑{int(diferenciaPorc)}%*\n'
        i += 1
            
    mensaje_completo = f'DIFERENCIA PRECIOS OC/FC:\n  PROVEEDOR    -    COMPROBANTES    -   DIFERENCIA   -\n{mensaje_plantilla}'
    
    contenido_html += """
      </table>
    </body>
    </html>
    """
    
    len_mensaje = len(mensaje_completo)
    
    try:    
        # Llamar a la función para enviar el mensaje Javier Uroz
        enviar_mensaje_whatsapp('+5492473501336', mensaje_completo)
    except Exception as e:
        print('Error al enviar mensaje: \n',e)
        
    # Prueba en cmd
    print(mensaje_completo)

except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)

remitente = 'javieruroz@imcestari.com'
# destinatario  = ['javieruroz@imcestari.com', 'mcelli@imcestari.com']
destinatario  = ['javieruroz@imcestari.com']
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
