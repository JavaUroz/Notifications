#-*- coding: utf-8 -*-
from asyncio.windows_events import NULL
import decimal
from math import radians
import os
from pickle import EMPTY_LIST
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
connection_string = os.environ['CONNECTION_STRING']

# Establecer la consulta SQL
sql_query = """
                SELECT DISTINCT
                    cabecera.[sccpro_Cod],
                    cabecera.[sccpro_CUIT],
                    cabecera.[sccpro_RazSoc],
                    detalle.[sdc_FRecep],
	                tipos.[spctco_Cod] AS [Cod],
                    tipos.[spc_Nro] AS [Nro],
                    detalle.[sdcart_CodGen],
                    detalle.[sdccon_Cod],
                    detalle.[sdc_Desc],    
                    detalle.[sdc_PrecioUn],
	                detalle.[sdc_CantUM1],
	                detalle.[sdc_CantUM2],
	                detalle.[sdc_ImpTot]
                FROM 
                    [SBDACEST].[dbo].[SegRelDetC] relacion
                INNER JOIN 
                    [SegTiposC] tipos ON relacion.[srcscc_ID] = tipos.[spcscc_ID]
                INNER JOIN 
                    [SegCabC] cabecera ON relacion.[srcscc_ID] = cabecera.[scc_ID]
                INNER JOIN 
                    [SegDetC] detalle ON relacion.[srcscc_ID] = detalle.[sdcscc_ID]
                INNER JOIN
	                [Proveed] proveed ON cabecera.[sccpro_CUIT] = proveed.[pro_CUIT]
                WHERE
                    tipos.[spctco_Cod] = 'RT' AND
	                proveed.procpg_Cod IN ('017','018','019') AND 
	                CONVERT(date, detalle.[sdc_FRecep]) BETWEEN DATEADD(day, -7, CONVERT(date, GETDATE())) AND DATEADD(day, 7, CONVERT(date, GETDATE())) AND
                    (sdcart_CodGen IS NOT NULL OR sdcart_CodGen IS NOT NULL)    
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
    .atrasado-container{
       background-color: #CC0000;
    }
    .atrasado{
       color: #FFFFFF;
       font-style:italic;
    }
    .entrega-semana-container{
       background-color: #FF6600;
    }
    .entrega-semana{
       color: #FFFFF3;
    }
    .entrega-quincena-container{
       background-color: #FFFF99;
    }
    .entrega-quincena{
       color: #222222;
    }
  </style>
</head>
<body>
  <h2>Aviso de recepcion de mercaderia - Pagos contra entrega</h2>
  <table>
    <tr class="cabecera">
      <th class="cabecera-text">Cod. Prov</th>
      <th class="cabecera-text">Cuit</th>
      <th class="cabecera-text">R. Social</th>
      <th class="cabecera-text">F. Recepcion</th>
      <th class="cabecera-text">Comprobante</th>
      <th class="cabecera-text">Articulo</th>
      <th class="cabecera-text">Descripcion</th>
      <th class="cabecera-text">P. Unit.</th>
      <th class="cabecera-text">Cantidad</th>
      <th class="cabecera-text">Imp. Total</th>
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
        cuit = resultado[1]
        razonSocial = resultado[2]
        fechaRecepcion = resultado[3]
        codComprobante = resultado[4]
        nroComprobante = resultado[5]
        codArticulo = resultado[6]
        codArtFormat = codArticulo[:2] + '.' + codArticulo[2:4] + '.' + codArticulo[4:7]
        if codArticulo == None:
            codArtFormat = ''
        codConcepto = resultado[7]
        if codConcepto == None:
            codConcepto = ''
        descripcion = resultado[8]
        precioUnit = resultado[9]
        cantUM1 = resultado[10]
        if cantUM1 == NULL:
            cantUM1 = ''
        cantUM2 = resultado[11]
        if cantUM2 == NULL:
            cantUM2 = ''
        impTotal= resultado[12]
        
        contenido_html += f"""
            <tr>
              <td>{codProveedor}</td>
              <td>{cuit}</td>
              <td>{razonSocial}</td>
              <td>{fechaRecepcion.strftime('%d/%m/%Y')}</td>
              <td>{codComprobante} {nroComprobante}</td>
              <td>{codArtFormat}{codConcepto}</td>
              <td>{descripcion}</td>
              <td>${precioUnit}</td>
              <td>{cantUM1}{cantUM2}</td>
              <td>${impTotal}</td>
             </tr>
        """
        mensaje_plantilla += f'{i}) {int(codProveedor)} {razonSocial[:25]} {codComprobante} {nroComprobante} - {codArtFormat}{codConcepto} {descripcion} - Entregado hoy!\n'
        i += 1
            
    

except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)

if resultados != []:
    mensaje_completo = f'PENDIENTES A RECLAMAR:\n   -    RAZ\u00D3N SOCIAL    -   COMPROBANTE   -  ARTICULO  -   SITUACI\u00D3N   -\n{mensaje_plantilla}'
    
    contenido_html += """
      </table>
    </body>
    </html>
    """
    
    len_mensaje = len(mensaje_completo)
    
    try:
         # Llamar a la función para enviar el mensaje a Javier Gabarini
        enviar_mensaje_whatsapp('+5492473504073', mensaje_completo)
        
        # Llamar a la función para enviar el mensaje Javier Uroz
        enviar_mensaje_whatsapp('+5492473501336', mensaje_completo)
    except Exception as e:
        print('Error al enviar mensaje: \n',e)
        
    # Prueba en cmd
    print(mensaje_completo)    


    remitente = 'no-reply@imcestari.com'
    destinatario  = ['javieruroz@imcestari.com', 'jgabarini@imcestari.com']
    # destinatario  = ['javieruroz@imcestari.com']
    asunto = 'Pagos contra entrega'
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
