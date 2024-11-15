import os
from dotenv import load_dotenv
from utils.fortras_stat import MensajeEstado as Fort
from utils.bmaster_api import BmasterApi as BmApi
from utils.compose_q10_line import compose_q10_line as composeQ10
import pandas as pd
from datetime import datetime
import paramiko


load_dotenv(dotenv_path="../conf/.env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=f"../conf/.env.'+{INTEGRATION_CUST}")
EMAIL_TO = os.getenv("EMAIL_TO")
SFTP_SERVER = os.getenv("SFTP_SERVER")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PW = os.getenv("SFTP_PW")
SFTP_PORT = os.getenv("SFTP_PORT")
SFTP_STAT_IN_DIR = os.getenv("SFTP_STAT_IN_DIR")
SFTP_DEV_STAT_IN_DIR = os.getenv("SFTP_DEV_STAT_IN_DIR")
SFTP_STAT_OUT_DIR = os.getenv("SFTP_STAT_OUT_DIR")
SFTP_DEV_STAT_OUT_DIR = os.getenv("SFTP_DEV_STAT_OUT_DIR")


class EstadoAneGru:

    def __init__(self):
        self.entorno = ENTORNO
        self.host = SFTP_SERVER
        self.username = SFTP_USER
        self.password = SFTP_PW
        self.port = int(SFTP_PORT)
        self.local_work_directory = "../fixtures"
        self.remote_work_out_directory = SFTP_STAT_OUT_DIR if self.entorno == "prod" else SFTP_DEV_STAT_OUT_DIR
        self.remote_work_in_directory = SFTP_STAT_IN_DIR if self.entorno == "prod" else SFTP_DEV_STAT_IN_DIR
        self.partidas = None
        self.query_partidas = """
        SELECT TOP 15
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
                trapda.ientcor IN (82861, 232829, 232830, 232831, 232833) 
                AND trapda.cpda LIKE 'TIP%'
                AND aebtrk.ihit IN (647, 511, 523, 524) 
            GROUP BY 
                creg
        ) max_itrk ON max_itrk.maxitrk < aebtrk.itrk AND max_itrk.creg = aebtrk.creg
        WHERE 
            aebtrk.ihit IN (541, 542, 543, 0, 544, 562, 630, 631, 632, 633, 635, 
                            513, 526, 527, 530, 568, 632, 633, 635, 636, 546, 547, 
                            511, 469, 302, 507, 512, 493, 508, 523, 524)
            AND trapda.ientcor IN (82861, 232829, 232830, 232831, 232833)
            AND trapda.cpda LIKE 'TIP%'
            AND maxitrk IS NOT NULL
            AND YEAR(fhit) * 100 + MONTH(fhit) > 202409
        ORDER BY 
            ipda, itrk ASC;
        """
        self.conversion_dict = {
            "ANXE01": "TBD",
            "ANXE02": "TBD",
            "ANXE05": "COR",
            "ANXE06": "COR",
            "ANXE07": "CRI",
            "ANXE09": "COR",
            "ANXE11": "302",
            "ANXE12": "CRI",
            "DESCARFAL": "SMA",
            "DESCARFTOT": "SMA",
            "DESCARKO": "SMA",
            "DESCAROK": "SMA",
            "ENT001": "SMA",
            "ENT004": "SMA",
            "ENT011": "SMA",
            "LINTRK02": "CRI",
            "SAL001": "402",
            "TRA0081": "202",
            "TRA0102": "402",
            "TRA0106": "COR"
            }


    def procesa_patida(self, cpda, q10_lines):
        number_of_records = len(q10_lines)
        fortras = Fort()
        txt_file_content = fortras.header("w")
        txt_file_content += fortras.header_q00("w")
        for q10_line in q10_lines:
            txt_file_content += composeQ10(
                status_code=self.conversion_dict.get(q10_line["status_code"], q10_line["status_code"]),
                date_of_event=q10_line["date_of_event"],
                time_of_event=q10_line["time_of_event"]
            )
        txt_file_content += fortras.z_control_record(number_of_records)
        txt_file_content += fortras.cierre("w")

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
            os.remove(local_file_path)
        else:
            print(f"Fallo al subir el archivo para cpda {cpda}")

        # print(txt_file_content)
        return txt_file_content


    def write_txt_file(self, cpda, content):
        # Generar el nombre del archivo con el formato especificado
        filename = f"STATE-{cpda}-{datetime.now().strftime('%Y%m%d%H%M')}.txt"
        local_file_path = os.path.join(self.local_work_directory, filename)
        with open(local_file_path, 'w') as f:
            f.write(content)
        return local_file_path  # Devolver la ruta completa del archivo creado

    def process_query_response(self, query_reply):
        self.partidas = {}

        for entry in query_reply.get("contenido", []):
            cpda = entry.get("cpda")
            chit = entry.get("chit")
            fhit = entry.get("fhit")
            hhit = entry.get("hhit")

            event_data = {
                "status_code": chit,
                "date_of_event": fhit,
                "time_of_event": hhit
            }

            if cpda not in self.partidas:
                self.partidas[cpda] = []
            self.partidas[cpda].append(event_data)

        for cpda, events in self.partidas.items():
            self.procesa_patida(cpda, events)


    @staticmethod
    def upload_file(local_file_path):
        remote_directory = os.environ.get('SFTP_STAT_IN_DIR') if ENTORNO == 'prod' else os.environ.get(
            'SFTP_DEV_STAT_IN_DIR')
        sftp_server = os.environ.get('SFTP_SERVER')
        sftp_user = os.environ.get('SFTP_USER')
        sftp_pw = os.environ.get('SFTP_PW')
        sftp_port = int(os.environ.get('SFTP_PORT', 22))
        remote_file_path = f"{remote_directory}/{os.path.basename(local_file_path)}"

        try:
            transport = paramiko.Transport((sftp_server, sftp_port))
            transport.connect(username=sftp_user, password=sftp_pw)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put(local_file_path, remote_file_path)
            sftp.close()
            transport.close()
            return True

        except Exception as e:
            # Manejar excepciones según sea necesario
            print(f"Error al subir el archivo {local_file_path} mediante SFTP: {e}")
            return False


    def run(self):
        bm = BmApi()
        query_reply = bm.consulta_(self.query_partidas)
        df = pd.DataFrame(query_reply['contenido'])
        print(df.to_markdown(index=False))
        self.process_query_response(query_reply)


if __name__ == "__main__":
    estado_ane_gru = EstadoAneGru()
    estado_ane_gru.run()

"""
x 1. Lanzamos la consulta consulta_(query)
x 2. Cambiamos el chit y los convertimos a su edi
x 3. Procesamos los resultados y escribimos un archivo formato txt por partida con tantas Q10 como itrk de esa partida
x 4. Subimos los archivos al SFTP de gru
5. Por cada partida usamos post_partida_tracking para asignarle el ihit a 647
6. Repesca? si guardamos el último itrk max. de la última consulta, la siguiente consulta la hacemos a partir de ese 
    itrk+1 cuyos hitos no sean 647, ¿no?
"""