# -*- coding: utf-8 -*-
import re
import os
import pyodbc
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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
                    [SegDetC].[sdc_CantUM1],
                    [SegDetC].[sdcume_Desc2],
                    [SegDetC].[sdc_CantUM2]
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
        codProveedor = resultado[0]
        if codProveedor in ('006644', '006905'):
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
                   background-color: #F2DF07;
                }
              </style>
            </head>
            <body>
              <h2>Atención, existen artículos próximos a vencer/vencidos.</h2>              
              <table>
                <tr class="cabecera">
                  <th class="cabecera-text">COMPROBANTES</th>
                  <th class="cabecera-text">CODIGO</th>
                  <th class="cabecera-text">DESCRIPCION</th>
                  <th class="cabecera-text">U.M.1</th>
                  <th class="cabecera-text">UNIDADES</th>
                  <th class="cabecera-text">U.M.2</th>
                  <th class="cabecera-text">UNIDADES</th>
                  <th class="cabecera-text">F. ENTREGA</th>
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
              <p>Por favor informar a <a href="mailto:mcelli@imcestari.com">Mariana Celli - Compras</a> sobre el estado del pedido</p>
              </br>
              <p>Saludos.</p>
              </br>
              <p>Industrias Metalúrgicas Cestari S.R.L.</p>
            </body>
            </html>
        """               

        # Configurar y enviar el correo
        remitente = 'no-reply@imcestari.com'
        
        # Definitivos
        destinatario = proveedor_info['email']
        if destinatario != []:
            destinatario.append('javieruroz@imcestari.com')

        # De prueba
        # destinatario = ['javieruroz@imcestari.com']
       
        asunto = 'Estado ordenes de compra próximas - IND. MET. CESTARI S.R.L.'

        mensaje = MIMEMultipart()
        mensaje['From'] = remitente
        mensaje['To'] = ", ".join(destinatario)
        mensaje['Subject'] = asunto
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
        except Exception as e:
            print('Ha ocurrido un error:\n', e)

except pyodbc.Error as e:
    print('Ocurrió un error al conectar a la base de datos:', e)
