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
connection_string = os.environ['CONNECTION_STRING']

# Establecer la consulta SQL
sql_query = """
                SELECT 
                    [SegCabC].[sccpro_Cod],
                    [SegCabC].[sccpro_RazSoc],
                    [SegTiposC].[spctco_Cod],
                    [SegTiposC].[spc_Nro],
                    [SegDetC].[sdc_FRecep]
                FROM [SBDACEST].[dbo].[SegTiposC]
                INNER JOIN [SegDetC] ON [SegTiposC].[spcscc_ID] = [SegDetC].[sdcscc_ID]
                INNER JOIN [SegCabC] ON [SegTiposC].[spcscc_ID] = [SegCabC].[scc_ID]
                WHERE [sdc_TipoIt] != 'L' AND 
                    [spctco_Cod] != 'PC' AND	
                    [SegDetC].[sdc_FRecep] <= DATEADD(DAY, 15, GETDATE()) AND
                    ([sdc_CPendRtUM1] > 0 OR [sdc_CPendRtUM2] > 0) AND 
                    [spc_Nro] > 0 AND 
                    ([sdccon_Cod] IN ('015','015A','033','112','117','024','025','206','003','113','212','224','019','213','099','118','012','014') OR [sdcart_CodGen] > 0) AND 
                    ([spctco_Cod] IN ('OC','OCP','OCR')	
	                OR ([spctco_Cod] = 'FC' AND [sccpro_Cod] IN ('005056','005090','005110','005541','005551','005601','005706','005729','005814','005883','005884','005918','005932','006347','006630','006644','006936',
					                  '007267','007365','007941','008048','008192','008468','008491','008718','008781','008790','008841','008848','008863','008894','008981','009088','009140',
					                  '009248','009296','009378','009443','009458','009486','009922','009959','009978','010003','010014','010050','010069','010075','010107','010112',
					                  '010189','010190','010246','010269','010313','010397','010406','010436','010445','010473','010476','010485','010502','010519')
					                  AND [SegDetC].[sdc_FRecep] > '2022-01-10 00:00:00.000')
                    ) AND ([sdc_Desc] NOT IN ('Materiales para Fabricaci\u00f3n',
                                        'Materiales para Fabricaci\u00f3n (IVA 10,5%)',
                                        'Repuestos y Reparaciones (IVA 21%)',
                                        'Ferias y Exposiciones (IVA 21%)',
                                        'Materiales para la construcci\u00f3n',
                                        'Ferreter\u00EDa - Art\u00EDculos varios',
                                        'Muebles y Utiles',
                                        'Regalos Empresariales',
						                'Indumentaria',
						                'Reparaciones varias',
						                'Instalaciones (IVA 21%)',
						                'Gastos de Exposici\u00f3n',
						                'Mantenimiento Inmuebles (21%)',
						                'Fletes y Acarreos',
						                'Gastos Varios de Mantenimiento',
						                'Gastos de Seguridad e Higiene',
						                'Gastos de Fabricaci\u00f3n',
						                'Publicidad (IVA 21%)') AND   
                     ([sdc_Desc] NOT LIKE '%Materiales para Fabricaci\u00f3n%' AND
                      [sdc_Desc] NOT LIKE '%Repuestos y Reparaciones%' AND
                      [sdc_Desc] NOT LIKE '%Ferias y Exposiciones%' AND
                      [sdc_Desc] NOT LIKE '%Anticipo%' AND
                      [sdc_Desc] NOT LIKE '%anticipo%' AND
                      [sdc_Desc] NOT LIKE '%mal%' AND
                      [sdc_Desc] NOT LIKE '%Materiales para la construcci\u00f3n%' AND
                      [sdc_Desc] NOT LIKE '%Ferreter\u00EDa - Art\u00EDculos varios%' AND
                      [sdc_Desc] NOT LIKE '%Muebles y Utiles%')
                    ) 	
                GROUP BY [SegDetC].[sdc_FRecep],
                    [SegCabC].[sccpro_Cod],
                    [SegCabC].[sccpro_RazSoc],
                    [SegTiposC].[spctco_Cod],
                    [SegTiposC].[spc_Nro]
                ORDER BY [SegDetC].[sdc_FRecep];
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
  <h2>Entregas pendientes</h2>
  <table>
    <tr class="cabecera">
      <th class="cabecera-text">EMPRESA</th>
      <th class="cabecera-text">ORDEN COMPRAS</th>
      <th class="cabecera-text">F. RECEPCION</th>
      <th class="cabecera-text">ESTADO</th>
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
        razonSocial = resultado[1]
        tipoComprobante = resultado[2]
        numeroComprobante = resultado[3]        
        fechaEntrega = resultado[4]
        
        fechaActual = datetime.now()
        diferencia =  fechaEntrega - fechaActual
        diferenciaDias = diferencia.days
        tiempoLimite = 15
        contenido_html += f"""
            <tr>
              <td>({codProveedor}) {razonSocial}</td>
              <td>{tipoComprobante} {int(numeroComprobante)}</td>
              <td>{fechaEntrega.strftime('%d/%m/%Y')}</td>
        """
        if diferenciaDias < tiempoLimite:        
            if diferenciaDias < 0:            
                contenido_html += f"""                                    
                      <td class="atrasado-container"><span class="atrasado"><b>Atrasado por {diferenciaDias} d\u00EDas</b></td>
                    </tr>
                """
                mensaje_plantilla += f'{i}) *{int(codProveedor)} {razonSocial[:25]}* {fechaEntrega.strftime('%d/%m/%Y')} - Atrasado por *{diferenciaDias*-1}* d\u00EDas\n'
        
            elif diferenciaDias > 0 and diferenciaDias <= 7:            
                contenido_html += f"""                  
                      <td class="entrega-semana-container"><span class="entrega-semana"><b>Entrega en {diferenciaDias} d\u00EDas</b></span></td>
                    </tr>
                """
                mensaje_plantilla += f'{i}) *{int(codProveedor)} {razonSocial[:25]}* {fechaEntrega.strftime('%d/%m/%Y')} - Entrega en *{diferenciaDias}* d\u00EDas\n'
            
            elif diferenciaDias > 7 and diferenciaDias <= 15:            
                contenido_html += f"""                 
                      <td class="entrega-quincena-container"><span class="entrega-quincena"><b>Entrega en {diferenciaDias} d\u00EDas</b></span></td>
                    </tr>
                """
                mensaje_plantilla += f'{i}) *{int(codProveedor)} {razonSocial[:25]}* {fechaEntrega.strftime('%d/%m/%Y')} - Entrega en *{diferenciaDias}* d\u00EDas\n'
            
            else:
                contenido_html += f"""                  
                      <td class="entrega-semana-container"><span class="entrega-semana"><b>Entrega para hoy</b></span></td>
                    </tr>
                """
                mensaje_plantilla += f'{i}) *{int(codProveedor)} {razonSocial[:25]}* {fechaEntrega.strftime('%d/%m/%Y')} - *Entrega para hoy!*\n'
        i += 1
            
    mensaje_completo = f'PENDIENTES A RECLAMAR:\n   -    RAZ\u00D3N SOCIAL    -   F. ENTREGA   -   SITUACI\u00D3N   -\n{mensaje_plantilla}'
    
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

remitente = 'no-reply@imcestari.com'
destinatario  = ['javieruroz@imcestari.com', 'mcelli@imcestari.com']
# destinatario  = ['javieruroz@imcestari.com']
asunto = 'Pendientes de recibir - RECLAMAR'
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
