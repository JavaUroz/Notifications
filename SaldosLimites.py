# -*- coding: utf-8 -*-

import decimal
import os
from tokenize import Double
from twilio.rest import Client, content
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import dotenv

# Cargar librería para .env
dotenv.load_dotenv()

# Cargar la imagen desde el archivo
with open('images/footer.jpg', 'rb') as fp:
    img = MIMEImage(fp.read())
    img.add_header('Content-ID', '<image1>')

# Establecer el connection string
connection_string = os.environ['CONNECTION_STRING']

# Establecer la consulta SQL
sql_query = """
SELECT ccopro_CUIT, ccopro_RazSoc, pro_CodPos, pro_Loc, SUM(cco_ImpMonLoc) AS TotalImpMonLoc
FROM dbo.QRY_COMPRASPAGOS
WHERE DescTipCond != 'CONTADO'
GROUP BY ccopro_CUIT, ccopro_RazSoc, pro_CodPos, pro_Loc
ORDER BY pro_CodPos, TotalImpMonLoc
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
  <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Roboto&display=swap">
  <style>                
    table {
      font-family: 'Myriad Pro Condensed', 'Roboto', sans-serif;
      border-collapse: collapse;
      width: 100%;
    }
    h1, h2, h3 {
      color: #333333
    }
    th, td {
      border: 1px solid #E5E7E9;
      padding: 8px;
      text-align: left;
    }
    .cabecera {
      background-color: #FF0000;      
    }
    .titulo-aviso-container {
      background-color: #FFFFFF;                
    }
    .titulo-aviso-text {
      color: #333333;
      font-family: 'Myriad Pro', 'Roboto', sans-serif;
      font-weight: 600;
      padding-bottom: 2px;
    }
    .cabecera-text {
      color: #FFFFFF;
      font-family: 'Myriad Pro', 'Roboto', sans-serif;
      font-weight: 400;
    }
    .excedido-container {
       background-color: #CC0000;
    }
    .excedido-text {
       color: #FFFFFF;
       font-weight: 600;
    }
    .advertencia-container {
       background-color: #F2DF07;
    }
    .saludo-text {
      color: #000000;
      font-family: 'Myriad Pro', 'Roboto', sans-serif;
      font-weight: 400;
    }
    .footer-container {
       padding-top: 20px;
    }   
    .footer-text-semibold {
        color: #000000;
        font-family: 'Myriad Pro', 'Roboto', sans-serif;
        font-weight: 600; /* Semibold */
        font-size: 20px;
    }
    .footer-text-condensed {
        color: #333333;
        font-family: 'Myriad Pro Condensed', 'Roboto', sans-serif;
        font-weight: 400; /* Regular Condensed */
    }
    .footer-text-condensed-italic {
        color: #336699;
        font-family: 'Myriad Pro Condensed', 'Roboto', sans-serif;
        font-weight: 400; /* Regular Condensed */
        font-size: 11px;
        font-style: italic;
    }
    .saldo-text{
      color: #CC0000;
      font-style:italic;
    }
  </style>
</head>
<body>
  <div class="titulo-aviso-container">
      <h2 class="titulo-aviso-text">Saldos deudores</h2>
  </div>  
  <table>
    <tr class="cabecera">
      <th class="cabecera-text">EMPRESA</th>
      <th class="cabecera-text">CUIT</th>
      <th class="cabecera-text">SALDO</th>
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
            if contador >= 5:
                message = client.messages.create(
                                      from_='whatsapp:+14155238886',
                                      body = f'Se excedio la cantidad de mensajes permitidos({contador + 1})\nEl cuadro completo se enviara a las casillas de correo asignadas.',                              
                                      to=f'whatsapp:{destinatario}'
                                  )
                print(f'Se alcanzo el limite de mensajes permitidos de ({len(message_chunks) - 1}):\n')
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
        cuit = resultado[0]
        cuit_formateado = cuit[:2] + "-" + cuit[2:10] + "-" + cuit[10:]
        razon_social = resultado[1]
        codigo_postal = resultado[2]
        if codigo_postal == None:
            codigo_postal = 'None'
        localidad = resultado[3]
        if localidad == None:
            localidad = 'None'
        localidad_formateado = localidad.upper()
        saldo = resultado[4]        
        limite = -60000000
        diferencia = saldo - limite
        
        if 'banco' in razon_social.lower():
            limite = -100000000
        elif '2720' in codigo_postal.lower() and not \
            ('20221493185' in cuit.lower()) and not \
            ('30506730038' in cuit.lower()) and not \
            ('30717013162' in cuit.lower()) and not \
            ('20132864935' in cuit.lower()) and not \
            ('30707067434' in cuit.lower()) and not \
            (razon_social == 'GAS PIC S.A.') and not \
            ('pardo' in razon_social.lower()):
            limite = -5000;
        elif 'graneros' in razon_social.lower():
            limite = -150000
        elif (('transporte' in razon_social.lower()) and (cuit != '30710824106') and (cuit != '30516892788')) or \
            ('damario' in razon_social.lower()) or \
            ('comision' in razon_social.lower()) or \
            ('bayl' in razon_social.lower()) or \
            ('30714693243' in cuit.lower()) or \
            ('30515999686' in cuit.lower()) or \
            ('20319059394' in cuit.lower()) or \
            ('30707735577' in cuit.lower()) or \
            ('20046963335' in cuit.lower()) or \
            ('20303127411' in cuit.lower()) or \
            ('20362238650' in cuit.lower()) or \
            ('20103107572' in cuit.lower()) or \
            ('bossolani' in razon_social.lower()) or \
            (('cargo' in razon_social.lower()) and (cuit != '30710188501') and (cuit != '20221493185')): # Transportes exceptuando Transporte Patron y Transporte Italia
            limite = -10000
        elif ('rosatto' in razon_social.lower()) or ('cr comisiones' in razon_social.lower()):
            limite = -300000
        elif 'gas pic s.a. (combustibles)' in razon_social.lower():
            limite = -1000000
        elif '30711186154' in cuit: # Solser
            limite = -500000
        elif '33583193109' in cuit: # Plegamex
            limite = -50000
        elif '23067223284' in cuit: # Oxitécnica
            limite = -150000
        elif 'gas pic s.a. (lubricantes)' in razon_social.lower():
            limite = -100000        
    
        # Comprueba si el saldo supera el límite de la cuenta y ejecura la función verificar_saldos_y_enviar_notificaciones
        if saldo < limite:
            if '2720' in codigo_postal.lower():
                contenido_html += f"""
                    <tr>
                      <td><b>({localidad_formateado})</b> {razon_social}</td>
                      <td>{cuit_formateado}</td>
                      <td><b><span class="saldo-text">$  {round(saldo, 2)}</span></td>
                    </tr>
                """
                mensaje_plantilla += f'{i}) *({localidad_formateado})* {razon_social} - {cuit_formateado} - *$ {round(saldo, 2)}*\n'
            # Mensaje de la plantilla
            else:
                contenido_html += f"""
                    <tr>
                      <td>{razon_social}</td>
                      <td>{cuit_formateado}</td>
                      <td><b><span class="saldo-text">$  {round(saldo, 2)}</span></td>
                    </tr>
                """
                mensaje_plantilla += f'{i}) {razon_social} - {cuit_formateado} - *$ {round(saldo, 2)}*\n'
            i += 1
            
    mensaje_completo = f'ATENCION PAGOS EXCEDIDOS: \n   -    RAZON SOCIAL    -   CUIT   -   SALDO   -\n{mensaje_plantilla}'
    
    contenido_html += """
      </table>
      </br>
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

except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)

remitente = 'no-reply@imcestari.com'
destinatario  = ['javieruroz@imcestari.com', 'jgabarini@imcestari.com']
# destinatario  = ['javieruroz@imcestari.com']
asunto = 'Pagos excedidos'
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
