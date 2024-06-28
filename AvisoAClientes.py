# -*- coding: utf-8 -*-
import decimal
from math import radians
import re
import os
from unittest import result
import pyodbc
import smtplib
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
import dotenv
from datetime import datetime

def obtenerMailsValidos(texto):
    # Expresión regular para encontrar direcciones de correo electrónico
    patron = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    
    # Buscar coincidencias en el texto
    emails = re.findall(patron, texto)
    
    # Retornar la lista de direcciones de correo electrónico válidas
    return emails

# Cargar librería para .env
dotenv.load_dotenv()

# Establecer el connection string
connection_string = os.environ['CONNECTION_STRING']

# Establecer la consulta SQL
sql_query = """                
    SELECT MIN([cve_FVto] ) as Vencimiento
      ,SUM([cve_SaldoMonLoc]) as Saldo	  
	  ,[cve_CodCli] as Cod_Pro
      ,MAX([cvecli_RazSoc]) as Razon_Social
      ,[Clientes].[cli_EMail] as Email

    FROM [SBDACEST].[dbo].[CabVenta]

    FULL OUTER JOIN [Clientes] ON [CabVenta].[cve_CodCli] = [Clientes].[cli_Cod]

    WHERE cve_FVto <= DATEADD(DAY, -20, GETDATE())
 
    GROUP BY   [cve_CodCli]
		      ,[Clientes].[cli_EMail]

    HAVING SUM([cve_SaldoMonLoc]) >= 1000

    ORDER BY [cve_CodCli] DESC
"""

# Contador de errores
errorCount = 0
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

    # Diccionario para almacenar información de los clientes
    clientes = {}
    
    

    # Procesar los resultados
    for resultado in resultados:
        # Comprobar si hay mas de 3 errores seguidos o si existen esos codClientes y finaliza el for      
        codCliente = resultado[2]
        if codCliente in ('005693','005696','003800','007076','004086','006567','006916') or errorCount >= 3:
            continue
        # Si el cliente no está en el diccionario, crear una nueva entrada
        if codCliente not in clientes:
            clientes[codCliente] = {
                'razon_social': resultado[3],
                'email': obtenerMailsValidos(resultado[4]),
                'saldo': resultado[1],
                'vencimiento': resultado[0]
            }

        # # Agregar información del comprobante al cliente actual
        # comprobante = {
        #     'codigo': resultado[0],
        #     'letra': resultado[1],
        #     'pto_venta': resultado[2],
        #     'numero': resultado[3],
        #     'fecha_emision': resultado[4].strftime('%d/%m/%Y'),
        #     'fecha_vencimiento': resultado[5].strftime('%d/%m/%Y'),
        #     'tipo_moneda': resultado[6],            
        #     'importe': resultado[8]            
        # }
        # clientes[codCliente]['comprobantes'].append(comprobante)
        
    # Cerrar el cursor y la conexión
    cursor.close()
    conexion.close()

    # Procesar la información de los clientes y enviar correos
    for codCliente, cliente_info in clientes.items():
        fecha_actual = datetime.now()
        fecha_vencimiento = cliente_info['vencimiento']
        diferencia = fecha_actual - fecha_vencimiento
        diferencia_dias = diferencia.days
        # Generar contenido HTML para el cliente actual
        # Cargar la imagen desde el archivo
        with open('images/footer.jpg', 'rb') as fp:
            img = MIMEImage(fp.read())
            img.add_header('Content-ID', '<image1>')
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
                   background-color: #FFFFFF;
                }
                .excedido-text {
                   color: #CC0000;
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
              </style>
            </head>
        """
        contenido_html += """
            <body> 
              <div>
                <h3 class="titulo-aviso-text">Estimado cliente,</h3>
                <p>nos ponemos en contacto con usted para informarle que de acuerdo a nuestros registros, consta un saldo deudor de <strong>$ {saldoTotal}</strong> de <strong>{diasAtraso}</strong> días de atraso.</p>
              </div>                           
              <div>
                 <p>Desde ya muchas gracias.</p>
              </div>
              <p>Saludos cordiales.</p>
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
        """.format(saldoTotal = float(cliente_info['saldo']), 
                   diasAtraso = diferencia_dias)
        
        # Configurar y enviar el correo
        remitente = 'javieruroz@imcestari.com'
        
        # # Definitivos
        # destinatario = cliente_info['email']
        # if destinatario != []:
        #     destinatario.append('arodriguez@imcestari.com')
        #     destinatario.append('jgabarini@imcestari.com')
        #     destinatario.append('javieruroz@imcestari.com')

        # De prueba
        destinatario=[]
        destinatario.append('javieruroz@imcestari.com')
        # destinatario.append('jgabarini@imcestari.com')
        
        asunto = 'Estado de cuentas - IND. MET. CESTARI S.R.L.'        
        mensaje = MIMEMultipart()
        mensaje['From'] = remitente
        mensaje['To'] = ", ".join(destinatario)
        mensaje['Subject'] = asunto
        mensaje.attach(img)
        mensaje.attach(MIMEText(contenido_html, 'html'))

        # Datos de autenticación
        username = os.environ['USUARIO']
        password = os.environ['PASSWORD']

        # Enviar el correo
        server = smtplib.SMTP_SSL('px000056.ferozo.com:465')
        server.login(username, password)
        try:
            server.sendmail(remitente, destinatario, mensaje.as_string())
            server.quit()
            print('E- mail enviado exitosamente!')
            
            # tiempo de retardo para evitar sancion de SPAM
            # time.sleep(280)
        except Exception as e:
            print('Ha ocurrido un error:\n{0}\nCuyo destinatario es: {1}'.format(e, destinatario))


except pyodbc.Error as e:
    print('Ocurrió un error al conectar a la base de datos:', e)
    errorCount += 1
