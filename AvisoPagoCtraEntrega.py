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
from email.mime.image import MIMEImage
import dotenv
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = os.environ['CONNECTION_STRING']
# Cargar la imagen desde el archivo
with open('images/footer.jpg', 'rb') as fp:
    img = MIMEImage(fp.read())
    img.add_header('Content-ID', '<image1>')
# Establecer la consulta SQL
sql_query = """
                SELECT [CCOPRO_CODIN]
	                  ,[CCOPRO_CUIT]
	                  ,[CCOPRO_RAZSOC]
	                  ,[CCO_FEMISION]
	                  ,[CCO_FECMOD]
	                  ,[CCOTCO_COD]
	                  ,[CCO_NRO]
                      ,[CCO_SALDOMONCC]
                      ,[CCOCPG_COD]
                      ,[CPG_DESC]
	                  ,[CPG_OBSERV]
                  FROM [SBDACEST].[dbo].[QRY_COMPRASPAGOS]

                  WHERE DescPasadoCG LIKE '%NO PASADO%' AND
		                CCOUSU_CODIGO LIKE 'JAVIERU' AND
		                CCOCPG_COD IN ('017','018','019') AND
		                CCOTCO_COD LIKE 'FC'

                  ORDER BY cco_FEmision desc
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
  <div class="titulo-aviso-container">
    <h2 class="titulo-aviso-text">Aviso de recepcion de mercaderia - Pagos contra entrega</h2>
  </div>
  <table>
    <tr class="cabecera">
      <th class="cabecera-text">Cod. Prov</th>
      <th class="cabecera-text">Cuit</th>
      <th class="cabecera-text">R. Social</th>
      <th class="cabecera-text">F. Emision</th>
      <th class="cabecera-text">F. Registracion</th>
      <th class="cabecera-text">Comprobante</th>
      <th class="cabecera-text">Importe</th>
      <th class="cabecera-text">Cond. Pago</th>
      <th class="cabecera-text">Observaciones</th>
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
        fechaEmision = resultado[3]
        fechaRegistracion = resultado[4]        
        codComprobante = resultado[5]
        nroComprobante = resultado[6]       
        impTotal = resultado[7]
        codCondPago = resultado[8]
        descCondPago = resultado[9]
        observaciones = resultado[10]
        
        contenido_html += f"""
            <tr>
              <td>{codProveedor}</td>
              <td>{cuit}</td>
              <td>{razonSocial}</td>
              <td>{fechaEmision.strftime('%d/%m/%Y')}</td>
              <td>{fechaRegistracion.strftime('%d/%m/%Y')}</td>
              <td>{codComprobante} {nroComprobante}</td>              
              <td>${impTotal}</td>
              <td>({codCondPago}) {descCondPago}</td>
              <td>{observaciones}</td>
             </tr>
        """
        mensaje_plantilla += f'{i}) {int(codProveedor)} {razonSocial[:25]} {codComprobante} {nroComprobante} - {observaciones} \n'
        i += 1
            
    

except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)

if resultados != []:
    mensaje_completo = f'PENDIENTES A RECLAMAR:\n   -    RAZ\u00D3N SOCIAL    -   COMPROBANTE   -  OBSERVACIONES   -\n{mensaje_plantilla}'
    
    contenido_html += """
      </table>
      </br>
      <hr>
      <div class="footer-container">
         <img src="cid:image1">
         <p class="footer-text-semibold">INDUSTRIAS METALÚRGICAS CESTARI S.R.L.</p>
         <p class="footer-text-condensed">Av. Eva Perón 1068. Colón, Buenos Aires.</p>
         <p class="footer-text-condensed">República Argentina.</p>
         <p class="footer-text-condensed">Tel: +54 2473 421001 / 430490</p>
         <p class="footer-text-condensed-italic">Este mensaje es confidencial. \n
             Puede contener información amparada por el secreto comercial. Si usted \n
             ha recibido este e-mail por error, deberá eliminarlo de su sistema. No \n
             deberá copiar el mensaje ni divulgar su contenido a ninguna persona. \n
             Muchas gracias.</p>
      </div>
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
    # destinatario  = ['javieruroz@imcestari.com', 'jgabarini@imcestari.com']
    destinatario  = ['javieruroz@imcestari.com']
    asunto = 'Pagos contra entrega'
    msg = contenido_html

    mensaje = MIMEMultipart()

    mensaje['From'] = remitente
    mensaje['To'] = ", ".join(destinatario)
    mensaje['Subject'] = asunto
    mensaje.attach(img)
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
