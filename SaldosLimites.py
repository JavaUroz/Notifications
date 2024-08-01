# -*- coding: utf-8 -*-

import os
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from dotenv import load_dotenv
import traceback

# Cargar variables de entorno
load_dotenv()

# Constantes
CONNECTION_STRING = os.environ['CONNECTION_STRING']
EMAIL_USERNAME = os.environ['USUARIO']
EMAIL_PASSWORD = os.environ['PASSWORD']
SMTP_SERVER = 'px000056.ferozo.com'
SMTP_PORT = 465
SENDER_EMAIL = 'javieruroz@imcestari.com'
# RECIPIENTS = ['javieruroz@imcestari.com', 'jgabarini@imcestari.com']
RECIPIENTS = ['javieruroz@imcestari.com']

# Consulta SQL
SQL_QUERY = """
SELECT  CCOPRO_CODIN,
		CCOPRO_CUIT, 
		MIN(CCOPRO_RAZSOC) AS CCOPRO_RAZSOC, 
		MIN(PRO_CODPOS) AS PRO_CODPOS, 
		MIN(PRO_LOC) AS PRO_LOC, 
		ABS(ROUND(SUM(ROUND(CCO_IMPMONLOC, 2)),0)) AS SALDO

FROM dbo.QRY_COMPRASPAGOS

WHERE CCO_MARCACDOCTACTEBCOTARJ = 2

GROUP BY CCOPRO_CODIN,
		 CCOPRO_CUIT

HAVING SUM(ROUND(cco_ImpMonLoc, 2)) < -1000

ORDER BY SALDO DESC
"""

class SaldoDeudor:
    def __init__(self, cod_prov,cuit, razon_social, codigo_postal, localidad, saldo):
        self.cod_prov = cod_prov
        self.cuit = cuit
        self.razon_social = razon_social
        # self.codigo_postal = codigo_postal
        # self.localidad = localidad
        self.codigo_postal = codigo_postal if codigo_postal is not None else ''
        self.localidad = localidad if localidad is not None else ''
        self.saldo = saldo

    def cuit_formateado(self):
        return f"{self.cuit[:2]}-{self.cuit[2:10]}-{self.cuit[10:]}"

    def localidad_formateada(self):
        return self.localidad.upper() if self.localidad else 'None'

    def limite(self):
        if 'banco' in self.razon_social.lower():
            return 100000000
        elif ('2720' in self.codigo_postal.lower() or
              ('30707733264' in self.cuit.lower())) and not any(excl in self.cuit.lower() for excl in
              ('20221493185', '30506730038', '30717013162', '20132864935', '30707067434')) and \
              self.razon_social not in ('GAS PIC S.A.', 'pardo'):
            return 5000
        elif 'graneros' in self.razon_social.lower():
            return 150000
        elif ('transporte' in self.razon_social.lower() and self.cuit not in ('30710824106', '30516892788')) or \
             'damario' in self.razon_social.lower() or \
             'comision' in self.razon_social.lower() or \
             'bayl' in self.razon_social.lower() or \
             any(excl in self.cuit.lower() for excl in ('30714693243', '30515999686', '20319059394',
                                                       '30707735577', '20046963335', '20303127411',
                                                       '20362238650', '20103107572', 'bossolani')) or \
             ('cargo' in self.razon_social.lower() and self.cuit not in ('30710188501', '20221493185')):
            return 10000
        elif 'rosatto' in self.razon_social.lower() or 'cr comisiones' in self.razon_social.lower():
            return 300000
        elif 'gas pic s.a. (combustibles)' in self.razon_social.lower():
            return 1000000
        elif '30711186154' in self.cuit:  # Solser
            return 500000
        elif '33583193109' in self.cuit:  # Plegamex
            return 50000
        elif '23067223284' in self.cuit:  # Oxitécnica
            return 150000
        elif 'gas pic s.a. (lubricantes)' in self.razon_social.lower():
            return 100000
        return 60000000

    def supera_limite(self):
        return self.saldo > self.limite()

def connect_to_db():
    try:
        return pyodbc.connect(CONNECTION_STRING)
    except pyodbc.Error as e:
        print(f'Error al conectar a la base de datos: {e}')
        raise

def fetch_data(cursor):
    cursor.execute(SQL_QUERY)
    return cursor.fetchall()

def process_results(results):
    saldo_deudores = [SaldoDeudor(*resultado) for resultado in results]
    saldo_deudores.sort(key=lambda x: x.saldo, reverse=True)

    mensaje_plantilla = ''
    contenido_html = """
    <html>
    <head>
      <link rel="stylesheet" href="https://fonts.googleapis.com/css?family=Roboto&display=swap">
      <style>
        table { font-family: 'Roboto', sans-serif; border-collapse: collapse; width: 100%; }
        h1, h2, h3 { color: #333333 }
        th, td { border: 1px solid #E5E7E9; padding: 8px; text-align: left; }
        .cabecera { background-color: #FF0000; }
        .titulo-aviso-container { background-color: #FFFFFF; }
        .titulo-aviso-text { color: #333333; font-weight: 600; padding-bottom: 2px; }
        .cabecera-text { color: #FFFFFF; font-weight: 400; }
        .excedido-container { background-color: #CC0000; }
        .excedido-text { color: #FFFFFF; font-weight: 600; }
        .advertencia-container { background-color: #F2DF07; }
        .saludo-text { color: #000000; font-weight: 400; }
        .footer-container { padding-top: 20px; }
        .footer-text-semibold { color: #000000; font-weight: 600; font-size: 20px; }
        .footer-text-condensed { color: #333333; font-weight: 400; }
        .footer-text-condensed-italic { color: #336699; font-weight: 400; font-size: 11px; font-style: italic; }
        .saldo-text { color: #CC0000; font-style: italic; }
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

    for i, saldo_deudor in enumerate(saldo_deudores, start=1):
        if saldo_deudor.supera_limite():
            if '2720' in saldo_deudor.codigo_postal.lower():
                contenido_html += f"""
                    <tr>
                      <td><b>({saldo_deudor.localidad_formateada()})</b> {saldo_deudor.razon_social} ({int(saldo_deudor.cod_prov)})</td>
                      <td>{saldo_deudor.cuit_formateado()}</td>
                      <td><b><span class="saldo-text">$ {int(saldo_deudor.saldo)}</span></td>
                    </tr>
                """
                mensaje_plantilla += f'{i}) *({saldo_deudor.localidad_formateada()})* {saldo_deudor.razon_social} - {saldo_deudor.cuit_formateado()} - *$ {int(saldo_deudor.saldo)}*\n'
            else:
                contenido_html += f"""
                    <tr>
                      <td>{saldo_deudor.razon_social} ({int(saldo_deudor.cod_prov)})</td>
                      <td>{saldo_deudor.cuit_formateado()}</td>
                      <td><b><span class="saldo-text">$ {int(saldo_deudor.saldo)}</span></td>
                    </tr>
                """
                mensaje_plantilla += f'{i}) {saldo_deudor.razon_social} - {saldo_deudor.cuit_formateado()} - *$ {int(saldo_deudor.saldo)}*\n'

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

    return contenido_html, mensaje_completo

def send_email(contenido_html, mensaje_completo):
    try:
        # Cargar la imagen del pie de página
        with open(r'C:\Users\javie\source\Notifications\images\footer.jpg', 'rb') as fp:
            img = MIMEImage(fp.read())
            img.add_header('Content-ID', '<image1>')

        # Configurar el mensaje
        mensaje = MIMEMultipart()
        mensaje['From'] = SENDER_EMAIL
        mensaje['To'] = ", ".join(RECIPIENTS)
        mensaje['Subject'] = 'Pagos excedidos'
        mensaje.attach(img)
        mensaje.attach(MIMEText(contenido_html, 'html'))

        # Enviar el correo
        with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT) as server:
            server.login(EMAIL_USERNAME, EMAIL_PASSWORD)
            server.sendmail(SENDER_EMAIL, RECIPIENTS, mensaje.as_string())

        print('E-mail enviado exitosamente!')
    except Exception as e:
        print(f'Error al enviar el correo electronico: {e}')

def main():
    try:
        # Establecer conexión con la base de datos
        conexion = connect_to_db()
        cursor = conexion.cursor()

        # Obtener datos de la base de datos
        resultados = fetch_data(cursor)

        # Procesar los resultados y construir el contenido
        contenido_html, mensaje_completo = process_results(resultados)

        # Enviar el correo electrónico
        send_email(contenido_html, mensaje_completo)

    except Exception as e:
        # Capturar y mostrar información detallada del error
        print(f'Error en la ejecución principal: {e}')
        print('Detalles del error:')
        print(traceback.format_exc())
    finally:
        # Cerrar el cursor y la conexión
        try:
            cursor.close()
            conexion.close()
        except Exception as close_error:
            print(f'Error al cerrar la conexión o el cursor: {close_error}')
            print('Detalles del error de cierre:')
            print(traceback.format_exc())

# Ejecutar la función principal
if __name__ == "__main__":
    main()