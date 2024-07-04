# -*- coding: utf-8 -*-
import re
import os
import pyodbc
import smtplib
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
SELECT DISTINCT
        SegCabC.sccpro_Cod, 
        SegCabC.sccpro_RazSoc,
        SegCabC.scc_FEmision,
        TiposDestino.spctco_Cod,
        TiposDestino.spc_CodPvt,
        TiposDestino.spc_Nro,
        Proveed.pro_EMail,
        TiposDestino.spctco_Cod + ' ' + TiposDestino.spc_Letra + ' ' + TiposDestino.spc_CodPvt + '-' + TiposDestino.spc_Nro

FROM ((((((((((((((SegCabC   
     INNER JOIN ParamGen ON SegCabC.sccemp_Codigo = ParamGen.pgeemp_Codigo
     INNER JOIN SegRelDetC ON SegCabC.scc_ID = SegRelDetC.srcscc_ID
     INNER JOIN SegTiposC TiposOrigen ON SegRelDetC.srcscc_IDPri = TiposOrigen.spcscc_ID     
        INNER JOIN CondPago ON (SegCabC.scccpg_Cod = CondPago.cpg_Cod)) 
        INNER JOIN SegTiposC TiposDestino ON (SegCabC.sccemp_Codigo = TiposDestino.spcemp_Codigo) AND 
         (SegCabC.sccsuc_Cod = TiposDestino.spcsuc_Cod) AND  
         (SegCabC.scc_ID = TiposDestino.spcscc_ID)) 
         INNER JOIN TipComp ON (TipComp.tco_Cod = TiposDestino.spctco_Cod) AND 
            (TipComp.tco_Circuito = TiposDestino.spctco_Circuito)) 
            INNER JOIN SegDetC ON (SegCabC.sccemp_Codigo = SegDetC.sdcemp_Codigo) AND
                (SegCabC.sccsuc_Cod = SegDetC.sdcsuc_Cod) AND 
                (SegCabC.scc_ID = SegDetC.sdcscc_ID)) 
                INNER JOIN mon ON (SegCabC.sccmon_codigo = mon.mon_codigo)) 
				                LEFT JOIN Proveed ON (SegCabC.sccpro_Cod = Proveed.pro_Cod)) 
					                LEFT JOIN Conceptos ON (Conceptos.con_Cod = SegDetC.sdccon_Cod)) 
						                LEFT JOIN Articulos ON (Articulos.art_CodGen = SegDetC.sdcart_CodGen AND 
							                Articulos.art_CodEle1 = SegDetC.sdcart_CodEle1 AND 
							                Articulos.art_CodEle2 = SegDetC.sdcart_CodEle2 AND 
							                Articulos.art_CodEle3 = SegDetC.sdcart_CodEle3)) 
							                LEFT JOIN ClasArt ON (ClasArt.cla_Cod = Articulos.artcla_Cod)) 
							                 LEFT JOIN UniMed ON (SegDetC.sdcume_Cod1 = UniMed.ume_Cod)) 
								                LEFT JOIN UniMed AS UniMed_2 ON (SegDetC.sdcume_Cod2 = UniMed_2.ume_Cod)) 
								                 LEFT JOIN TIva ON (SegDetC.sdctiv_Cod = TIva.tiv_Cod)) 
								                  LEFT JOIN NoGrav ON (SegDetC.sdcngr_Cod = NoGrav.ngr_Cod)) 
								                   LEFT JOIN ArtProv ON (Articulos.art_CodGen = ArtProv.aprart_CodGen AND 
									                Articulos.art_CodEle1 = ArtProv.aprart_CodEle1 AND 
									                Articulos.art_CodEle2 = ArtProv.aprart_CodEle2 AND 
									                Articulos.art_CodEle3 = ArtProv.aprart_CodEle3 AND 
									                Articulos.artpro_Cod = ArtProv.aprpro_Cod)) 

WHERE (sdc_TipoIt = 'A' OR sdc_TipoIt = 'C') AND 
      ((sdc_CPendFcUM1 >  0 OR  sdc_CPendFcUM2 >  0) OR  
      (tco_TipoFijo='RD' AND scc_GenPendFc=1 And scc_GenPendRt=0 And (sdc_CPendFcUM1 <>  0 OR  sdc_CPendFcUM2 <>  0))) AND
      (sdc_ActStock <> '5') AND Not 
      (scc_OrigenComp = 'P' And TiposDestino.spctco_TipoFijo = 'OC' And TiposDestino.spc_EstadoAutoriz Is Null) AND 
      (scc_CumpXPgm = 0) AND (scc_Estado <> 'X') AND 
      (TiposDestino.spc_Orig <> 'R' AND TiposDestino.spc_Orig <> 'C') AND 	  
      TiposDestino.spctco_Cod in ('RT', 'FC') AND (TiposDestino.spc_EstadoAutoriz != 'R' OR TiposDestino.spc_EstadoAutoriz IS NULL) AND
      TipComp.tco_Circuito = 'C' AND NOT
      TiposOrigen.spctco_Cod LIKE 'OCR' AND
      SegCabC.scc_FEmision >= DATEADD(YEAR, -1, GETDATE()) AND NOT
      SegCabC.sccpro_Cod IN (010406) AND NOT
      TiposDestino.spc_Nro IN ('00095975','00003493','00172178','00171574','00010365',
							   '00152502','00014727','00014679','00013555','00012181',
							   '00015909','00559307','00063537','00002590','00002470',
							   '00002642','00000333','04122023','00000300','00000256',
							   '00014180','00004713','00291606','00056841','00065391',
							   '00061422','00060907','00060741','00000278','00000276',
							   '00000200','00084589','00000303','00009294','00009265',
							   '00006383','29122023','00172260','00170097','00165832',
							   '00165834','00165838','00164795','00162525','02052024',
							   '08042024','00085854','00010065','00001227','00001222',
							   '00001189','00001186','00002033','00254774')
	  
ORDER BY SegCabC.sccpro_RazSoc, SegCabC.scc_FEmision DESC
"""
with open('images/footer.jpg', 'rb') as fp:
            img = MIMEImage(fp.read())
            img.add_header('Content-ID', '<image1>')
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

    comprobanteAnterior = ""

    # Procesar los resultados
    for resultado in resultados:

        # Comprobar si hay mas de 3 errores seguidos o si existen esos codProveedor y finaliza el for      
        codProveedor = resultado[0]
        if codProveedor in ('006644'):
            continue
        codProveedor = resultado[0]
        if codProveedor == '009248':
            proveedores = {
            '009248': {
            'razon_social': 'EVER WEAR S.A.',
            'email': ['atencionalcliente@everwear.com.ar','facturacion@everwear.com.ar'],
            'comprobantes': []
        },
    }
        # Si el proveedor no está en el diccionario, crear una nueva entrada
        if codProveedor not in proveedores:
            proveedores[codProveedor] = {
                'razon_social': resultado[1],
                'email': obtenerMailsValidos(resultado[6]),
                'comprobantes': []
            }

        # Agregar información del comprobante al proveedor actual
        comprobante = {
            'numero_comp': resultado[5],  
            'comprobante': resultado[7],
            'fecha_emision': resultado[2].strftime('%d/%m/%Y')           
        }
        
        # Si se repite el comprobante se omite la carga
        if comprobante['numero_comp'] == comprobanteAnterior:
            continue 

        proveedores[codProveedor]['comprobantes'].append(comprobante)

        comprobanteAnterior = resultado[5]

    # Cerrar el cursor y la conexión
    cursor.close()
    conexion.close()
    # Procesar la información de los proveedores y enviar correos
    for codProveedor, proveedor_info in proveedores.items():
        # Generar contenido HTML para el proveedor actual
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
                <h2 class="titulo-aviso-text">Aviso:</h2>              
                <h3 class="titulo-aviso-text">Existen FACTURAS pendientes de recepción correspondiente a los siguientes REMITOS:</h3>
              </div>              
              <table>
                <tr class="cabecera">
                  <th class="cabecera-text">COMPROBANTES</th>
                  <th class="cabecera-text">F. EMISION</th>
                </tr>
        """

        for comprobante in proveedor_info['comprobantes']:            
            contenido_html += """
                <tr>
                  <td>{comprobante}</td>
                  <td>{fecha_emision}</td>
                </tr>
            """.format(comprobante=comprobante['comprobante'],                       
                       fecha_emision=comprobante['fecha_emision']) 

        contenido_html += """
            </table>
            </br>
            </br>              
            <p class="saludo-text">Por favor enviarlas a <a href="mailto:javieruroz@imcestari.com">Javier Uroz - Administración</a></p>
            </br>
            <p class="saludo-text">Saludos.</p>
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

        # # De prueba
        # destinatario = ['javieruroz@imcestari.com']
       
        asunto = 'Remitos pendientes de Facturar - IND. MET. CESTARI S.R.L.'

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
        except Exception as e:
            print('Ha ocurrido un error:\n{0}\nCuyo destinatario es: {1}'.format(e, destinatario))

except pyodbc.Error as e:
    print('Ocurrió un error al conectar a la base de datos:', e)
    

