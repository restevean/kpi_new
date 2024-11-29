# src/est_ane_gru.py

import os
from datetime import timezone
from dotenv import load_dotenv
from utils.bmaster_api import BmasterApi as BmApi
from utils.compose_q10_line import compose_arc_header
from utils.misc import n_ref
import pandas as pd
import logging
from datetime import datetime
import paramiko

# Activamos logging
logging.basicConfig(
    level=logging.INFO,     # Nivel mínimo de mensajes a mostrar
    format='%(message)s',   # Formato del mensaje
    # format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje
)

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path="../conf/.env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=f"../conf/.env.{INTEGRATION_CUST}")

class EstadoAneArc:

    def __init__(self):
        self.host = os.getenv("SFTP_SERVER_ARC")
        self.username = os.getenv("SFTP_USER_ARC")
        self.password = os.getenv("SFTP_PW_ARC")
        self.port = int(os.getenv("SFTP_PORT_ARC"))
        self.local_work_directory = "../fixtures"
        self.remote_work_out_directory = os.getenv("SFTP_OUT_PATH_ARC")
        self.remote_work_in_directory = os.getenv("SFTP_IN_PATH_ARC")
        self.partidas = None
        self.max_itrk = None
        self.bm = BmApi()
        self.query_partidas = """
        SELECT TOP 10
            itrk, aebtrk.ihit, aebhit.chit, aebhit.dhit, maxitrk, aebtrk.fmod, aebtrk.hmod, 
            aebtrk.fhit, aebtrk.hhit, trapda.cpda, trapda.ipda, nrefcor
        FROM 
            aebtrk
        INNER JOIN 
            trapda ON trapda.ipda = aebtrk.creg AND aebtrk.dtab = 'trapda'
        INNER JOIN 
            aebhit ON aebhit.ihit = aebtrk.ihit
        LEFT JOIN (
            SELECT 
                MAX(itrk) AS maxitrk, creg
            FROM 
                aebtrk
            INNER JOIN 
                trapda ON trapda.ipda = aebtrk.creg AND aebtrk.dtab = 'trapda'
            WHERE 
                trapda.ientcor IN (244692,252997,244799,77904,246489,251061,185744,244788,255503,252995,253463,253462,77957,253464,244152,89322,96202,94544) 
                AND trapda.cpda LIKE 'TIP%'
                --AND aebtrk.ihit IN (647, 511, 523, 524) 
                AND aebtrk.ihit IN (647)
            GROUP BY 
                creg
        ) max_itrk ON max_itrk.creg = aebtrk.creg
        WHERE 
            aebtrk.ihit IN (541, 542, 543, 0, 544, 562, 630, 631, 632, 633, 635, 
                            513, 526, 527, 530, 568, 632, 633, 635, 636, 546, 547, 
                            511, 469, 302, 507, 512, 493, 508, 523, 524)
            AND trapda.ientcor IN (244692,252997,244799,77904,246489,251061,185744,244788,255503,252995,253463,253462,77957,253464,244152,89322,96202,94544)
            AND trapda.cpda LIKE 'TIP%'
            AND (maxitrk < itrk OR maxitrk IS NULL)
            AND YEAR(fhit) * 100 + MONTH(fhit) > 202409
        ORDER BY 
            ipda, itrk ASC;
        """
        # self.query_repesca = None
        self.conversion_dict = {
            "ANXE01": ("TBD", "Pick-up in progress + Estimated pick-up time"),
            "ANXE02": ("TBD", "Pick-up in progress + Estimated pick-up time"),
            "ANXE05": ("COR", "Shipment delivered"),
            "ANXE06": ("COR", "Shipment delivered"),
            "ANXE07": ("CRI", "Shipment delivered, with remarks"),
            "ANXE09": ("COR", "Shipment delivered"),
            "ANXE11": ("302", "Not delivered - refusal"),
            "ANXE12": ("CRI", "Shipment delivered, with remarks"),
            "DESCARFAL": ("SMA", "Missing inbound"),
            "DESCARFTOT": ("SMA", "Missing inbound"),
            "DESCARKO": ("SMA", "Missing inbound"),
            "DESCAROK": ("SMA", "Shipment scanned - no difference"),
            "ENT001": ("SMA", "Shipment scanned - no difference"),
            "ENT004": ("SMA", "Shipment scanned - no difference"),
            "ENT011": ("SMA", "Missing inbound"),
            "LINTRK02": ("CRI", "Shipment delivered, with remarks"),
            "SAL001": ("402", "In delivery"),
            "TRA0081": ("202", "Not delivered - wrong address"),
            "TRA0102": ("402", "In delivery"),
            "TRA0106": ("COR", "Shipment delivered"),
        }

    @property
    def query_repesca(self):
        return f"""
                SELECT TOP 2000
                    trapda.cpda,
                    trapda.ipda,
                    nrefcor,
                    itrk,
                    aebtrk.ihit,
                    aebhit.chit,
                    aebhit.dhit,
                    aebtrk.fmod,
                    aebtrk.hmod,
                    aebtrk.fhit,
                    aebtrk.hhit
                FROM
                    aebtrk
                INNER JOIN
                    trapda
                    ON trapda.ipda = aebtrk.creg
                    AND aebtrk.dtab = 'trapda'
                INNER JOIN
                    aebhit
                    ON aebhit.ihit = aebtrk.ihit
                WHERE
                    aebtrk.ihit IN (
                        0, 302, 469, 493, 507, 508, 511, 512, 513, 523, 524, 526, 527, 530, 541,
                        542, 543, 544, 546, 547, 562, 568, 630, 631, 632, 633, 635, 636
                    )
                    AND trapda.ientcor IN (244692,252997,244799,77904,246489,251061,185744,244788,255503,252995,253463,253462,77957,253464,244152,89322,96202,94544)
                    AND trapda.cpda LIKE 'TIP%'

                    --AND ipda = 5215  /* Hay que poner itrk e ipda según las variables acumuladas en el proceso */
                    --AND ipda = 5215  /* Hay que poner itrk e ipda según las variables acumuladas en el proceso */
                    AND itrk > {self.max_itrk}
                    AND YEAR(fhit) * 100 + MONTH(fhit) > 202409;
                    """


    def procesa_partida(self, cpda, lines):

        number_of_records = len(lines)
        txt_file_content = ""
        for line in lines:
            event_code = \
            self.conversion_dict.get(line.get("status_code"), ("Código no encontrado", "Descripción no encontrada"))[0]
            event_description = \
            self.conversion_dict.get(line.get("status_code"), ("Código no encontrado", "Descripción no encontrada"))[1]
            txt_file_content += compose_arc_header(
                order_number=line["order_number"],
                document_type=line["document_type"],
                event_code=event_code,
                event_description=event_description,
                event_date=line["date_of_event"],
                event_time=line["time_of_event"]
            )
        local_file_path = self.write_txt_file(cpda, txt_file_content)
        success = self.upload_file(local_file_path)
        if isinstance(self.partidas[cpda], list):
            self.partidas[cpda] = {
                'events': self.partidas[cpda],
                'success': success
            }
        else:
            self.partidas[cpda]['success'] = success

        if success:
            # os.remove(local_file_path)
            logging.info("8P")
        else:
            print(f"Fallo al subir el archivo para cpda {cpda}")

        # print(txt_file_content)
        return txt_file_content


    def write_txt_file(self, cpda, content):
        filename = f"ESITI_ANEXA_{datetime.now().strftime('%Y%m%d%H%M')}_{cpda}.txt"
        local_file_path = os.path.join(self.local_work_directory, filename)
        with open(local_file_path, 'w') as f:
            f.write(content)
        return local_file_path  # Devolver la ruta completa del archivo creado



    def process_query_response(self, query_reply):
        self.partidas = {}
        for entry in query_reply.get("contenido", []):
            nrefcor = n_ref(entry.get("nrefcor"), "r")
            cpda = entry.get("cpda")
            ipda = entry.get("ipda")  # Obtener ipda
            chit = entry.get("chit")
            fhit = entry.get("fhit")
            hhit = entry.get("hhit")
            event_data = {
                "order_number": nrefcor,
                "document_type": "S",
                "status_code": chit,
                "date_of_event": fhit,
                "time_of_event": hhit
            }
            if cpda not in self.partidas:
                self.partidas[cpda] = {
                    'ipda': ipda,
                    'events': []
                }
            self.partidas[cpda]['events'].append(event_data)

        for cpda, data in self.partidas.items():
            events = data['events']
            self.procesa_partida(cpda, events)





    def upload_file(self, local_file_path):
        remote_directory = self.remote_work_in_directory
        remote_file_path = f"{remote_directory}/{os.path.basename(local_file_path)}"
        try:
            transport = paramiko.Transport((self.host, self.port))
            transport.connect(username=self.username, password=self.password)
            sftp = paramiko.SFTPClient.from_transport(transport)
            logging.info(f"Successfuly connected to {self.host}:{self.port}")
            sftp.put(local_file_path, remote_file_path)
            sftp.close()
            transport.close()
            logging.info("Connection closed")
            return True
        except Exception as e:
            print(f"Error al subir el archivo {local_file_path} mediante SFTP: {e}")
            return False




    def actualizar_comunicado(self):
        for cpda, data in self.partidas.items():
            if data.get('success') is True:
                ipda = data.get('ipda')
                if not ipda:
                    print(f"No se encontró 'ipda' para cpda {cpda}")
                    continue

                fechatracking = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                tracking_data = {
                    "codigohito": "ESTADOCOM",
                    "descripciontracking": "ESTADO COMUNICADO",
                    "fechatracking": fechatracking
                }
                response = self.bm.post_partida_tracking(ipda, tracking_data)
                if not isinstance(response, dict) or 'status_code' not in response:
                    print(f"Error al actualizar el comunicado para ipda {ipda}: Respuesta inválida")
                    continue
                if response['status_code'] not in [200, 201]:
                    print(
                        f"Error al actualizar el comunicado para ipda {ipda}: Código de estado {response['status_code']}")
                else:
                    print(f"Comunicado actualizado exitosamente para ipda {ipda}, {cpda}")


    def run(self):
        query_reply = self.bm.n_consulta(self.query_partidas)
        df = pd.DataFrame(query_reply['contenido'])
        self.max_itrk = df['itrk'].max()
        df = df.fillna("")
        pd.options.display.float_format = '{:.0f}'.format
        print(df.to_markdown(index=False))
        print(self.max_itrk)
        self.process_query_response(query_reply)
        # self.actualizar_comunicado()
        second_query_reply = self.bm.n_consulta(self.query_repesca)
        df = pd.DataFrame(second_query_reply['contenido'])
        print(df.to_markdown(index=False))
        if second_query_reply.get("contenido"):
            self.partidas = None
            self.process_query_response(second_query_reply)
            # self.actualizar_comunicado()


if __name__ == "__main__":
    estado_ane_arc = EstadoAneArc()
    estado_ane_arc.run()
