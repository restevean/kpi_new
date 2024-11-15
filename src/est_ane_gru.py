import os
from dotenv import load_dotenv
from utils.fortras_stat import MensajeEstado as Fort
from utils.bmaster_api import BmasterApi as BmApi
from utils.compose_q10_line import compose_q10_line as composeQ10
import pandas as pd

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
        self.communication_pending = None
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
        # TODO Update conversion dictionary. There are missing items
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


    def genera_archivo(self, cpda, q10_lines):
        number_of_records = len(q10_lines)
        fortras = Fort()
        txt_file_dict = fortras.header("w")
        txt_file_dict += fortras.header_q00("w")
        for q10_line in q10_lines:
            txt_file_dict += composeQ10(status_code=self.conversion_dict[q10_line["status_code"]],
            date_of_event=q10_line["date_of_event"],
                       time_of_event=q10_line["time_of_event"])
        txt_file_dict += fortras.z_control_record(number_of_records)
        txt_file_dict += fortras.cierre("w")
        self.write_txt_file(cpda, txt_file_dict)
        print(txt_file_dict)
        return txt_file_dict


    def write_txt_file(self, cpda, txt_file):
        """Write the txt file"""
        # TODO Whicha name must have the file?
        with open(f"{self.local_work_directory}/State-{cpda}", "w") as f:
            f.write(txt_file)
        return True


    def process_query_response(self, query_reply):
        cpda_groups = {}

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

            if cpda not in cpda_groups:
                cpda_groups[cpda] = []
            cpda_groups[cpda].append(event_data)

        for cpda, events in cpda_groups.items():
            self.genera_archivo(cpda, events)

    def run(self):
        bm = BmApi()
        query_reply = bm.consulta_(self.query_partidas)
        # Convertir los datos a un DataFrame de pandas
        df = pd.DataFrame(query_reply['contenido'])
        # Imprimir el DataFrame como tabla
        print(df.to_markdown(index=False))
        self.process_query_response(query_reply)


if __name__ == "__main__":
    estado_ane_gru = EstadoAneGru()
    estado_ane_gru.run()

"""
x 1. Lanzamos la consulta consulta_(query)
x 2. Cambiamos el chit y los convertimos a su edi
x 3. Procesamos los resultados y escribimos un archivo formato txt por partida con tantas Q10 como itrk de esa partida
4. Subimos los archivos al SFTP de gru
5. Por cada partida usamos post_partida_tracking para asignarle el ihit a 647
"""