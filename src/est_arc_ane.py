# src/est_arc.ane.py

import os
import logging
from dotenv import load_dotenv
from utils.fortras_stat import MensajeEstado as Fot
from utils.bmaster_api import BmasterApi as BmApi
from utils.sftp_connect import SftpConnection as py_Sftp
from utils.send_email import EmailSender
from datetime import datetime
from pathlib import Path


# Activamos logging
logging.basicConfig(
    level=logging.INFO,     # Nivel mínimo de mensajes a mostrar
    # format='%(message)s',   # Formato del mensaje
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje
)
logger = logging.getLogger(__name__)
base_dir = Path(__file__).resolve().parent

load_dotenv(dotenv_path=base_dir.parent / "conf" / ".env.base")

ENTORNO = os.getenv("ENTORNO")
EMAIL_OURS = os.getenv("EMAIL_OURS")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=base_dir.parent / "conf" / f".env.{INTEGRATION_CUST}")
EMAIL_TO = os.getenv("EMAIL_TO_ARC")

class EstadoCorresponsalAnexa:
    def __init__(self):
        self.entorno = ENTORNO
        self.email_from = "Estado Arcese Anexa"
        # TODO gestiuonar varios correos en .env
        self.email_to = [EMAIL_TO, EMAIL_OURS] if (self.entorno == "pro"
                                                              and EMAIL_TO) else [EMAIL_OURS]
        self.email_subject = "Anexa estado"
        self.email_body = None
        self.host = os.getenv("SFTP_SERVER_ARC")
        self.username = os.getenv("SFTP_USER_ARC")
        self.password = os.getenv("SFTP_PW_ARC")
        self.port = int(os.getenv("SFTP_PORT_ARC"))
        # TODO enclose this path in .env files
        # self.local_work_directory = "../fixtures/"
        self.base_path = Path(__file__).resolve().parent
        self.local_work_directory = self.base_path.parent / "fixtures"
        self.remote_work_out_directory = os.getenv("SFTP_OUT_PATH_ARC")
        self.bm = BmApi()

    def run (self):
        email_sender = EmailSender()
        n_sftp= py_Sftp(self.host, username=self.username,password=self.password, port=self.port)
        n_sftp.connect()
        n_sftp.sftp.chdir(self.remote_work_out_directory)

        filtered_list_dir = [file for file in n_sftp.sftp.listdir() if "ESITI" in file]
        enviar_email = False

        # Procesamos cada archivo
        for fichero in filtered_list_dir:
            mensaje = ""
            enviar_email=True

            ruta_descarga = self.local_work_directory / fichero
            stat = Fot()
            n_sftp.sftp.get(fichero, self.local_work_directory / fichero)
            estados = stat.leer_stat_arcese(fichero=ruta_descarga)

            for est in estados:
                est_anexa = stat.conversion_stat_arcese_anexa(est.get("EventCode").strip())
                cpda = est.get('Customer Reference').strip()
                logging.info(cpda)
                fecha = datetime.now()
                subir_hito = False  # lo uso para controlar si lo subimos o enviamos un mensaje de error según vamos
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

                            # Si encuentra esos ientcor
                            if resp_partida["contenido"][0]["ientcor"] in [89322,96202,77904,84019,84054,88931,
                                                                           185744,244692,244788,244799,246489,251061,
                                                                           251265,252995,252997,253462]:
                                # Comprobamos que el corresponsal es Arcese ... si no, por error, podría otro
                                # corresponsal por la TEP o referir a otro corresposnal. No todas las TEP son correctas
                                fecha = datetime.strptime(est.get("Event Date").strip() + est.get("Event Time").strip(),
                                                          "%Y%m%d%H%M")
                                hito_json ={
                                    "codigohito": est_anexa,
                                    "descripciontracking": est.get("Event Description").strip(),
                                    "fechatracking": fecha.strftime("%Y-%m-%dT%H:%M:00.000Z")  #"2024-11-30T12:43:55.101Z"
                                }

                                # Marcamos el hito
                                resp_hito = self.bm.post_partida_tracking(ipda=resp_partida["contenido"][0]["ipda"],
                                                                        data_json=hito_json)
                                if resp_hito.get("status_code")==201:
                                    mensaje+=f"Hito {est_anexa} -- {est.get("Event Description").strip()} "
                                    mensaje +=f"para la partida {cpda} subido.\n"
                                    # Mover ficher
                                    os.rename(self.local_work_directory / fichero,
                                              self.local_work_directory / "arc" / "edi" / "processed" / fichero)
                                # Si el status code no es 201
                                else:
                                    mensaje += (f"Error al marcacar el hito {est_anexa} -- "
                                                f"{est.get("Event Description").strip()} para la partida {cpda}")

                                    # Mover fichero
                                    os.rename(self.local_work_directory / fichero,
                                              self.local_work_directory / "arc" / "edi" /"error" / fichero)

                            # Si no ha encontrado el ientcor
                            else:
                                mensaje+= (f"La partida {cpda} no corresponde a Arcese con el ientcor "
                                           f"{str(resp_partida["contenido"][0]["ientcor"])}")
                                os.rename(self.local_work_directory / fichero,
                                          self.local_work_directory / "arc" / "edi" /"error" / fichero)
                        else:
                            mensaje += f"\n Estado Arcese para la partida {cpda} que no está en BM."
                            os.rename(self.local_work_directory / fichero,
                                      self.local_work_directory / "arc" / "edi" /"error" / fichero)
            if enviar_email:
                email_sender.send_email(self.email_from, self.email_to, self.email_subject, mensaje)
                logging.info("enviamos email")


if __name__ == '__main__':
    est_ane_arc = EstadoCorresponsalAnexa()
    est_ane_arc.run()
