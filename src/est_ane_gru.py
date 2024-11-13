import os
from dotenv import load_dotenv
from utils.fortras_stat import MensajeEstado as Fort
from utils.bmaster_api import BmasterApi as BmApi
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
SFTP_STAT_DIR = os.getenv("SFTP_STAT_DIR")
SFTP_DEV_STAT_DIR = os.getenv("SFTP_DEV_STAT_DIR")


class EstadoAneGru:

    def __init__(self):
        self.entorno = ENTORNO
        self.host = SFTP_SERVER
        self.username = SFTP_USER
        self.password = SFTP_PW
        self.port = int(SFTP_PORT)
        self.local_work_directory = "../fixtures"
        self.remote_work_directory = SFTP_STAT_DIR if self.entorno == "prod" else SFTP_DEV_STAT_DIR
        self.communication_pending = None
        self.query_partidas = """
        SELECT TOP 4 
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
            "ENT001": "001",
            "DESCARFAL": "002",
            "DESCARKO": "003",
            "SAL001": "SPC",
            "ANXE05": "COR",
            "TRA0102": "T01",   # Para test
            "ANXE02": "T02"     # Para test
        }

    @staticmethod
    def genera_archivo(q10_lines):
        number_of_records = len(q10_lines) + 2
        fortras = Fort()
        txt_file_dict = fortras.header()
        txt_file_dict = fortras.header_q00()
        for q10_line in q10_lines:
            pass
        txt_file_dict = fortras.z_control_record()
        txt_file_dict = fortras.cierre()
        print(txt_file_dict)
        return txt_file_dict


    def write_txt_file(self, txt_file):
        """Write the txt file"""
        # TODO Whicha name must have the file?
        with open(f"{self.local_work_directory}/estado_ane_gru", "w") as f:
            f.write(txt_file)
        return True

    def run(self):
        bm = BmApi()
        query_reply = bm.consulta_(self.query_partidas)
        # Convertir los datos a un DataFrame de pandas
        df = pd.DataFrame(query_reply['contenido'])
        # Imprimir el DataFrame como tabla
        print(df.to_markdown(index=False))


if __name__ == "__main__":
    estado_ane_gru = EstadoAneGru()
    estado_ane_gru.run()
    file_contents = estado_ane_gru.genera_archivo(
        {
            1: {
                "record_type": "Q10",
                "Consignment number sending depot": " " * 35,
                "Consignment number receiving depot": " " * 35,
                "Pick-up order number": " " * 35,
                "Status code": "001",                               # Mandatory
                "Date of event": "20241110",                           # Mandatory
                "Time of event (HHMM)": "1921 ",                    # Mandatory
                "Consignment number delivering party": " " * 35,
                "Wait/Downtime in minutes": " " * 4,
                "Name of acknowledg-ing party": " " * 35,
                "Additional text": " " * 70,
                "Reference number": " " * 12,
                "Latitude (Lat)": " " * 11,
                "Longitude (Lon)": " " * 11
                },
            2: {
                "record_type": "Q10",
                "Consignment number sending depot": " " * 35,
                "Consignment number receiving depot": " " * 35,
                "Pick-up order number": " " * 35,
                "Status code": "003",                               # Mandatory
                "Date of event": "20241110",                           # Mandatory
                "Time of event (HHMM)": "2021",                    # Mandatory
                "Consignment number delivering party": " " * 35,
                "Wait/Downtime in minutes": " " * 4,
                "Name of acknowledg-ing party": " " * 35,
                "Additional text": " " * 70,
                "Reference number": " " * 12,
                "Latitude (Lat)": " " * 11,
                "Longitude (Lon)": " " * 11
                }
        }
    )
    estado_ane_gru.write_txt_file(file_contents)






"""
consulta_ query
Les comunicamos lo que ellos nos han enviado IMPORTACIÓN
tomamos su chit y devolvemos su edi

Recorremos la BBDD cada 5 minutos
Si encontramos algún hito porterior a nuestro último hito de comunicación

post_partida_tracking

Si obtengo datos, es decir partidas pendioentes de comunicar:
lanzo consulta por partida y comunico:

Comunicación:
=============
@HP00
Q00
Compopnemos la línea q10


"""