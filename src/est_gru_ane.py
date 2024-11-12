import os
from dotenv import load_dotenv
from utils import fortras_stat as state
from utils.bmaster_api import BmasterApi as BmApi
from datetime import datetime, timezone
from utils.send_email import EmailSender
from utils.sftp_connect import SftpConnection


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


class EstadoGruAne:

    def __init__(self):
        self.entorno = ENTORNO
        self.host = SFTP_SERVER
        self.username = SFTP_USER
        self.password = SFTP_PW
        self.port = int(SFTP_PORT)
        self.local_work_directory = "../fixtures"
        self.remote_work_directory = SFTP_STAT_DIR if self.entorno == "prod" else SFTP_DEV_STAT_DIR
        self.download_files()
        self.files = self.load_dir_files()
        self.email_from = "Estado Gruber Anexa"
        self.email_subject = "Gruber Estado"
        self.email_body = None
        self.email_to = [EMAIL_TO, "restevean@gmail.com"] if (self.entorno == "prod"
                                                              and EMAIL_TO) else ["restevean@gmail.com"]

    def load_dir_files(self):
        return {
            file: {
                "success": False,
                "message": "",
            }
            for file in os.listdir(self.local_work_directory)
            if os.path.isfile(os.path.join(self.local_work_directory, file))
        }


    def download_files(self):
        n_sftp = SftpConnection()
        n_sftp.connect()

        if not os.path.exists(self.local_work_directory):
            os.makedirs(self.local_work_directory)

        n_sftp.sftp.chdir(self.remote_work_directory)
        for file in n_sftp.sftp.listdir():
            if n_sftp.sftp.stat(file).st_mode & 0o170000 == 0o100000:  # Verifica si es un archivo regular
                local_path = os.path.join(self.local_work_directory, file)
                # print(f"Descargando {file} a {local_path}")
                n_sftp.sftp.get(file, local_path)
        n_sftp.disconnect()


    @staticmethod
    def get_cod_hito(status):
        status_map = {
            "001": "ENT001",
            "002": "DESCARFAL",
            "003": "DESCARKO",
            "SPC": "SAL001",
            "COR": "ANXE05"
        }
        return status_map.get(status)


    def file_process(self, file_name):
        results = state.MensajeEstado().leer_stat_gruber(file_name)
        bm = BmApi()
        file_key = file_name.rsplit("/", 1)[-1]
        self.files[file_key] = {"success": False, "message": ""}

        for lineas in results.get("Lineas", {}):
            if lineas.get("Record type ‘Q10’") == "Q10":
                n_ref_cor = lineas.get("Consignment number sending depot")
                n_status = lineas.get("Status code")
                m_query = f"select ipda, * from trapda where nrefcor ='{n_ref_cor}'"
                # print(m_query)  # Para depuración; elimina en producción
                query_reply = bm.consulta_(m_query)
                # print(query_reply)  # Para depuración; elimina en producción

                # Verifica si el contenido está vacío y actualiza el mensaje si es así
                if "contenido" in query_reply and not query_reply["contenido"]:
                    # Caso de `contenido == {}`
                    self.files[file_key]["success"] = False
                elif "contenido" in query_reply:
                    # Procesa el contenido si no está vacío
                    record = query_reply["contenido"][0]
                    n_ipda = record.get("ipda")
                    n_hito = self.get_cod_hito(n_status)
                    n_cpda = record.get("cpda")
                    n_json = self.build_tracking_json(file_key, n_hito)

                    tracking_reply = bm.post_partida_tracking(n_ipda, n_json)
                    if tracking_reply["status_code"] == 201:
                        self.files[file_key]["success"] = True
                        self.files[file_key]["message"] += f"\nCreada partida, hito {n_hito}-{n_cpda}"
                        print("Success")
                    else:
                        self.files[file_key]["success"] = False
                        self.files[file_key]["message"] += f"\nNO Creada partida, hito {n_hito}-{n_cpda}"
                        print("Fail")

                    # self.update_file_status(file_key, tracking_reply["status_code"], n_hito, n_cpda)
                else:
                    # Si no hay contenido o error, actualiza el mensaje adecuadamente
                    self.files[file_key]["success"] = False
                # print(f"El ipda es: {n_ipda if 'n_ipda' in locals() else 'None'}")


    @staticmethod
    def build_tracking_json(file_key, n_hito):
        """Construye el JSON de tracking."""
        return {
            "codigohito": n_hito,
            "descripciontracking": file_key,
            "fechatracking": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        }

    def update_file_status(self, file_key, status_code, n_hito, n_cpda):
        if status_code == 201:
            self.files[file_key]["success"] = True
            self.files[file_key]["message"] += f"\nCreada partida, hito {n_hito}-{n_cpda}"
            print("Success")
        else:
            self.files[file_key]["success"] = False
            self.files[file_key]["message"] += f"\nNO Creada partida, hito {n_hito}-{n_cpda}"
            print("Fail")


    def run(self):
        email_sender = EmailSender()
        n_sftp = SftpConnection()
        n_sftp.connect()

        for file_name, file_attrs in self.files.items():
            local_path = os.path.join(self.local_work_directory, file_name)
            self.file_process(local_path)

            # Obtiene los valores actualizados de file_attrs después de file_process()
            file_attrs = self.files[file_name]

            remote_dir = f"{self.remote_work_directory}/OK" if file_attrs["success"] else f"{self.remote_work_directory}/ERROR"
            remote_path = f"{remote_dir}/{file_name}"
            n_sftp.sftp.put(local_path, remote_path)
            n_sftp.sftp.remove(f"{self.remote_work_directory}/{file_name}")
            target_dir = os.path.join(self.local_work_directory, "success" if file_attrs["success"] else "fail")
            os.makedirs(target_dir, exist_ok=True)  # Crea el directorio si no existe
            os.rename(local_path, os.path.join(target_dir, file_name))
            self.email_body = (
                f'{file_attrs["message"]}\nArchivo: {file_name}'
                if file_attrs.get("message")
                else f"No se crearon partidas para el archivo: {file_name}"
            )
            # print(f"Email body: {self.email_body}")
            email_sender.send_email(self.email_from, self.email_to, self.email_subject, self.email_body)
        n_sftp.disconnect()


if __name__ == "__main__":
    estado_grub_anex = EstadoGruAne()
    estado_grub_anex.run()
