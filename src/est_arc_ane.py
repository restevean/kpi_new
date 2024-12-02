# src/est_arc.ane.py

import os
import logging
from dotenv import load_dotenv
from utils.fortras_stat import MensajeEstado as Fot
from utils.bmaster_api import BmasterApi as BmApi
from utils.sftp_connect import SftpConnection as py_Sftp
from utils.send_email import EmailSender
from utils.send_email import EmailSender as email
from datetime import datetime


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
EMAIL_TO = os.getenv("EMAIL_TO")

class EstadoCorresponsalAnexa:
    def __init__(self):
        self.entorno = ENTORNO
        self.email_from = "Estado Arcese Anexa"
        self.email_to = [EMAIL_TO, "restevean@gmail.com"] if (self.entorno == "prod"
                                                              and EMAIL_TO) else ["restevean@gmail.com"]
        self.email_subject = "Anexa estado"
        self.email_body = None
        self.host = os.getenv("SFTP_SERVER_ARC")
        self.username = os.getenv("SFTP_USER_ARC")
        self.password = os.getenv("SFTP_PW_ARC")
        self.port = int(os.getenv("SFTP_PORT_ARC"))
        # TODO enclose this path in .env files
        self.local_work_directory = "../fixtures"
        self.remote_work_out_directory = os.getenv("SFTP_OUT_PATH_ARC")
        self.bm = BmApi()

    def run (self):
        email_sender = EmailSender()
        n_sftp= py_Sftp()
        n_sftp.connect(host=self.host, port=self.port, username=self.username,password=self.password)
        n_sftp.sftp.chdir(self.remote_work_out_directory)
        # TODO Fix double instance to BmApi
        # bm = BmApi()

        filtered_list_dir = [file for file in n_sftp.sftp.listdir() if "ESITI" in file]
        enviar_email = False

        # Procesamos cada archivo
        for fichero in filtered_list_dir:
            mensaje = ""
            enviar_email=True

            ruta_descarga = self.local_work_directory+fichero
            stat = Fot()
            # n_sftp.sftp.download(remote_path=self.sftp_remote_dir+"/"+fichero, target_local_path=ruta_descarga)
            n_sftp.sftp.get(fichero, self.local_work_directory+fichero)
            estados = stat.leer_stat_arcese(fichero=ruta_descarga)

            for est in estados:
                est_anexa = stat.conversion_stat_arcese_anexa(est.get("EventCode").strip())
                cpda = est.get('Customer Reference').strip()
                fecha = datetime.now()
                subir_hito = False #lo uso para controlar si lo subimso o enviamos un mensaje de error según vamos
                # haciendo validaciones

                # Ponemos fecha al evento en caso de que no la tenga
                if est.get("Event Date").strip() =="":
                    mensaje+=f"No se ha incluido fecha de evento para la partida {cpda}\n"
                else:
                    fecha = datetime.strptime(est.get("Event Date").strip()+est.get("Event Time").strip(),
                                              "%Y%m%d%H%M")
                    subir_hito=True

                # Asignamos el hito en caso de que no haya error
                if est_anexa[:5]=="Error":
                    mensaje+=f"\nEstado de Arcese no conocido {est.get("EventCode").strip()} para la partida {cpda}"
                    subir_hito=False
                else:
                    if subir_hito:
                        resp_partida = self.bm.n_consulta(query=f"select ipda, ientcor from trapda where cpda='{cpda}'")
                        if len(resp_partida.get("contenido"))>0:
                            if resp_partida["contenido"][0]["ientcor"] in [89322,96202,77904,84019,84054,88931,
                                                                           185744,244692,244788,244799,246489,251061,
                                                                           251265,252995,252997]:
                                # Comprobamos que el corresponsal es Arcese ... si no, por error, podría otro
                                # corresponsal por la TEP o referir a otro corresposnal. No todas las TEP son correctas
                                fecha = datetime.strptime(est.get("Event Date").strp() + est.get("Event Time").strp(),
                                                          "%Y%m%d%H%M")
                                hito_json ={
                                    "codigohito": est_anexa,
                                    "descripciontracking": est.get("Event Description").strip(),
                                    "fechatracking": fecha.strftime("%Y-%m-%dT%H:%M:00.000Z")  #"2024-11-30T12:43:55.101Z"
                                }

                                resp_hito = BmApi.post_partida_tracking(ipda=resp_partida["contenido"][0]["ipda"],
                                                                        data_json=hito_json)
                                if resp_hito.get("cod_error")==201:
                                    mensaje+=(f"Hito {est_anexa} -- {est.get("Event Description").strip()} "
                                              f"para la partida {cpda} subido.\n")
                            else:
                                mensaje+= (f"La partida {cpda} no corresponde a Arcese con el ientcor "
                                           f"{str(resp_partida["contenido"][0]["ientcor"])}")
                        else:
                            mensaje += f"\n Estado Arcese para la partida {cpda} que no está en BM."
            if enviar_email:
                # email_sender.send_email(self.email_from, self.email_to, self.email_subject, mensaje)
                logging.info("enviamos email")
                # print("enviamos email")


if __name__ == '__main__':
    est_ane_arc = EstadoCorresponsalAnexa()
    est_ane_arc.run()
