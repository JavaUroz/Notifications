# -*- coding: utf-8 -*-
import re
import os
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
SELECT
    [SegCabC].[sccpro_Cod],
    [SegCabC].[sccpro_RazSoc],
    [SegTiposC].[spctco_Cod],
    [SegTiposC].[spc_Nro],
    [Proveed].[pro_EMail],
    [SegDetC].[sdc_FRecep],
    [SegDetC].[sdcart_CodGen],
    [SegDetC].[sdccon_Cod],
    [SegDetC].[sdc_Desc],
    [SegDetC].[sdcume_Desc1],
	[SegDetC].[sdc_CPendRtUM1],
    [SegDetC].[sdcume_Desc2],
	[SegDetC].[sdc_CPendRtUM2]
FROM [SBDACEST].[dbo].[SegTiposC]
INNER JOIN [SegDetC] ON [SegTiposC].[spcscc_ID] = [SegDetC].[sdcscc_ID]
INNER JOIN [SegCabC] ON [SegTiposC].[spcscc_ID] = [SegCabC].[scc_ID]
INNER JOIN [Proveed] ON [SegCabC].[sccpro_Cod] = [Proveed].[pro_Cod]
WHERE [sdc_TipoIt] != 'L' AND 
    [spctco_Cod] != 'PC' AND 
    [SegDetC].[sdc_FRecep] <= DATEADD(DAY, 15, GETDATE()) AND
    ([sdc_CPendRtUM1] > 0 OR [sdc_CPendRtUM2] > 0) AND 
    [spc_Nro] > 0 AND 
    ([sdccon_Cod] IN ('015','015A','033','112','117','024','025','206','003','113','212','003','224','019','213','099','118','012','014') OR [sdcart_CodGen] > 0) AND 
    ([spctco_Cod] IN ('OC','OCP','OCR') OR 
        ([spctco_Cod] = 'FC' AND [sccpro_Cod] IN ('008790', '010406'))
    ) AND 
    ([sdc_Desc] NOT IN ('Materiales para Fabricación',
                        'Materiales para Fabricación (IVA 10,5%)',
                        'Repuestos y Reparaciones (IVA 21%)',
                        'Ferias y Exposiciones (IVA 21%)',
                        'Materiales para la construcción',
                        'Ferretería - Artículos varios',
                        'Muebles y Utiles',
					                'Regalos Empresariales',
					                'Indumentaria',
					                'Reparaciones varias',
					                'Instalaciones (IVA 21%)',
					                'Gastos de Exposición',
					                'Mantenimiento Inmuebles (21%)',
					                'Fletes y Acarreos',
					                'Gastos Varios de Mantenimiento',
					                'Gastos de Seguridad e Higiene',
					                'Gastos de Fabricación',
					                'Publicidad (IVA 21%)') AND    
     ([sdc_Desc] NOT LIKE '%Materiales para Fabricación%' AND
      [sdc_Desc] NOT LIKE '%Repuestos y Reparaciones%' AND
      [sdc_Desc] NOT LIKE '%Ferias y Exposiciones%' AND
      [sdc_Desc] NOT LIKE '%Materiales para la construcción%' AND
      [sdc_Desc] NOT LIKE '%Ferretería - Artículos varios%' AND
      [sdc_Desc] NOT LIKE '%Muebles y Utiles%' AND
      [sdc_Desc] NOT LIKE '%mal facturada%')
    )
ORDER BY [SegCabC].[sccpro_Cod], [SegDetC].[sdc_FRecep];
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

    # Diccionario para almacenar información de los proveedores
    proveedores = {}

    # Procesar los resultados
    for resultado in resultados:
        # Comprobar si hay mas de 3 errores seguidos o si existen esos codProveedor y finaliza el for      
        codProveedor = resultado[0]
        if codProveedor in ('006644', '006905') or errorCount >= 3:
            continue
        # Si el proveedor no está en el diccionario, crear una nueva entrada
        if codProveedor not in proveedores:
            proveedores[codProveedor] = {
                'razon_social': resultado[1],
                'email': obtenerMailsValidos(resultado[4]),
                'comprobantes': []
            }

        # Agregar información del comprobante al proveedor actual
        comprobante = {
            'cod_comprobante': resultado[2],
            'nro_comprobante': resultado[3],
            'fecha_entrega': resultado[5].strftime('%d/%m/%Y'),
            'cod_articulo': resultado[6],
            'desc_articulo': resultado[8],
            'unidad_medida1': resultado[9],
            'cantidad_um1': resultado[10],
            'unidad_medida2': resultado[11],
            'cantidad_um2': resultado[12]
        }
        proveedores[codProveedor]['comprobantes'].append(comprobante)

    # Cerrar el cursor y la conexión
    cursor.close()
    conexion.close()

    # Procesar la información de los proveedores y enviar correos
    for codProveedor, proveedor_info in proveedores.items():
        # Generar contenido HTML para el proveedor actual
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
              </style>
            </head>
            <body>
              <div class="titulo-aviso-container">
                <h2 class="titulo-aviso-text">Estimados buenos días,</h2>              
                <h3 class="titulo-aviso-text">enviamos una actualización sobre los artículos  que,  según nuestros registros, necesitarán de pronta atención. Por favor, verificar los siguientes pedidos pendientes de recibir:</h3>
              </div>
              <table>
                <tr class="cabecera">
                  <th><p class="cabecera-text">COMPROBANTES</p></th>
                  <th><p class="cabecera-text">CÓDIGO</p></th>
                  <th><p class="cabecera-text">DESCRIPCIÓN</p></th>
                  <th><p class="cabecera-text">U.M.1</p></th>
                  <th><p class="cabecera-text">UNIDADES</p></th>
                  <th><p class="cabecera-text">U.M.2</p></th>
                  <th><p class="cabecera-text">UNIDADES</p></th>
                  <th><p class="cabecera-text">F. ENTREGA</p></th>
                </tr>
        """

        for comprobante in proveedor_info['comprobantes']:
            if comprobante['cantidad_um1'] != 0.0 or comprobante['cantidad_um2'] != 0.0:
                contenido_html += """
                    <tr>
                      <td>{cod_comprobante} {nro_comprobante}</td>
                      <td>{cod_articulo}</td>
                      <td>{desc_articulo}</td>
                      <td>{unidad_medida1}</td>
                      <td>{cantidad_um1}</td>
                      <td>{unidad_medida2}</td>
                      <td>{cantidad_um2}</td>                      
                """.format(cod_comprobante=comprobante['cod_comprobante'],
                       nro_comprobante=comprobante['nro_comprobante'],
                       cod_articulo=comprobante['cod_articulo'],
                       desc_articulo=comprobante['desc_articulo'],
                       unidad_medida1=comprobante['unidad_medida1'],
                       unidad_medida2=comprobante['unidad_medida2'],
                       cantidad_um1=comprobante['cantidad_um1'] if comprobante['cantidad_um1'] != 0 else '',
                       cantidad_um2=comprobante['cantidad_um2'] if comprobante['cantidad_um2'] != 0 else '',
                       fecha_entrega=comprobante['fecha_entrega'])
                
                fecha_actual = datetime.now()
                fecha_entrega_string = comprobante['fecha_entrega']
                fecha_entrega = datetime.strptime(fecha_entrega_string, '%d/%m/%Y')
                diferencia = fecha_entrega - fecha_actual
                diferencia_dias = diferencia.days
                
                if diferencia_dias < 0:
                    contenido_html += """
                        <td class="excedido-container"><p class="excedido-text">{fecha_entrega}</p></td>
                    </tr>
                    """.format(fecha_entrega=comprobante['fecha_entrega'])
                elif diferencia_dias < 7 and diferencia_dias >= 0:
                    contenido_html += """
                        <td class="advertencia-container"><p>{fecha_entrega}</p></td>
                    </tr>
                    """.format(fecha_entrega=comprobante['fecha_entrega'])
                else:
                    contenido_html += """
                        <td><p>{fecha_entrega}</p></td>
                    </tr>
                    """.format(fecha_entrega=comprobante['fecha_entrega'])

        contenido_html += """
              </table>
              </br>
              </br>              
              <p class="saludo-text">Agradecemos si informaran a <a href="mailto:mcelli@imcestari.com">Mariana Celli - Compras</a> sobre el estado de este pedido.</p>
              </br>
              <p class="saludo-text">Saludos,</p>
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

        # Configurar y enviar el correo
        remitente = 'javieruroz@imcestari.com'
        
        # Definitivos
        destinatario = proveedor_info['email']
        if destinatario != []:
            destinatario.append('javieruroz@imcestari.com')
            destinatario.append('compras@imcestari.com')

        # # De prueba
        # destinatario = ['javieruroz@imcestari.com']
        
        asunto = 'Estado ordenes de compra próximas - IND. MET. CESTARI S.R.L.'        
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
            time.sleep(280)
        except Exception as e:
            print('Ha ocurrido un error:\n{0}\nCuyo destinatario es: {1}'.format(e, destinatario))


except pyodbc.Error as e:
    print('Ocurrió un error al conectar a la base de datos:', e)
    errorCount += 1
