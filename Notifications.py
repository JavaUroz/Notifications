# -*- coding: utf-8 -*-

import decimal
import os
from tokenize import Double
from twilio.rest import Client, content
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import dotenv

# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = "Driver={SQL Server};Server=SERVIDOR;Database=SBDACEST;UID=sa;PWD=Sa2008R2;"

# Establecer la consulta SQL
sql_query = """
SELECT ccopro_CUIT, ccopro_RazSoc, SUM(cco_ImpMonLoc) AS TotalImpMonLoc
FROM dbo.QRY_COMPRASPAGOS
GROUP BY ccopro_CUIT, ccopro_RazSoc
"""

# Establecer cliente con credenciales de SID y Token de Twilio 
account_sid = os.environ['ACCOUNT_SID']
auth_token = os.environ['AUTH_TOKEN']
client = Client(account_sid, auth_token)

mensaje_plantilla = ''
i = 1
# Función para enviar mensaje de WhatsApp
def enviar_mensaje_whatsapp(destinatario, mensaje):
    message = client.messages.create(
                              from_='whatsapp:+14155238886',
                              body = mensaje,                              
                              to=f'whatsapp:{destinatario}'
                          )
    print('Mensaje enviado correctamente:', message.sid)

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
        saldo = resultado[2]
        limite = -60000000
        diferencia = saldo - limite
        
        if 'banco' in razon_social.lower():
            limite = -100000000
        elif 'graneros' in razon_social.lower():
            limite = -150000
        elif (('transporte' in razon_social.lower()) and (cuit != '30710824106') and (cuit != '30516892788')) or \
            ('damario' in razon_social.lower()) or \
            ('comision' in razon_social.lower()) or \
            ('bayl' in razon_social.lower()) or \
            ('bossolani' in razon_social.lower()) or \
            (('cargo' in razon_social.lower()) and (cuit != '30710188501')): # Transportes exceptuando Transporte Patron y Transporte Italia
            limite = -10000
        elif ('rosatto' in razon_social.lower()) or ('cr comisiones' in razon_social.lower()):
            limite = -300000
        elif 'gas pic' in razon_social.lower():
            limite = -1000000
        elif '30711186154' in cuit: # Solser
            limite = -500000
        elif '33583193109' in cuit: # Plegamex
            limite = -50000
        elif '23067223284' in cuit: # Oxitécnica
            limite = -150000            
    
        # Comprueba si el saldo supera el límite de la cuenta y ejecura la función verificar_saldos_y_enviar_notificaciones
        if saldo < limite:
            # Mensaje de la plantilla
            mensaje_plantilla += f'{i}. {razon_social}, CUIT: {cuit_formateado} \nSALDO: *${round(saldo, 2)}* LIMITE *${round(limite,2)}* DIFERENCIA: *$-{round((limite - saldo), 2)}*.\n'
            i += 1
            
    mensaje_completo = f'ATENCION JAVI, PAGOS EXCEDIDOS: \n{mensaje_plantilla}'
                
    # Llamar a la función para enviar el mensaje a Javier Gabarini
    enviar_mensaje_whatsapp('+5492473504073', mensaje_completo)
    
    # Llamar a la función para enviar el mensaje Javier Uroz
    enviar_mensaje_whatsapp('+5492473501336', mensaje_completo)
    
    # Prueba en cmd
    print(mensaje_completo)

except pyodbc.Error as e:
    print('Ocurrio un error al conectar a la base de datos:', e)

remitente = 'javieruroz@imcestari.com'
destinatario  = ['javieruroz@imcestari.com', 'jgabarini@imcestari.com']
# destinatario  = ['javieruroz@imcestari.com']
asunto = 'Pagos excedidos'
msg = mensaje_completo

mensaje = MIMEMultipart()

mensaje['From'] = remitente
mensaje['To'] = ", ".join(destinatario)
mensaje['Subject'] = asunto

mensaje.attach(MIMEText(msg, 'plain'))

# Datos
username = os.environ['USERNAME']
password = os.environ['PASSWORD']

# Enviando el correo
server = smtplib.SMTP_SSL('px000056.ferozo.com:465')
# server.starttls()
username=remitente
server.login(username,password)
server.sendmail(remitente, destinatario, mensaje.as_string())
server.quit()

# Cerrar el cursor y la conexión
cursor.close()
conexion.close()
