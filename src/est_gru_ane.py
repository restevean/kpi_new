import os
from dotenv import load_dotenv
from utils import fortras_stat as state
from utils.bmaster_api import BmasterApi as BmApi
from datetime import datetime, timezone
from utils.send_email import EmailSender


load_dotenv(dotenv_path="../conf/.env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=f"../conf/.env.'+{INTEGRATION_CUST}")
EMAIL_TO = os.getenv("EMAIL_TO")


class EstadoGruAne:

    def __init__(self):
        self.work_directory = "../fixtures"
        self.files = self.load_dir_files()
        self.email_from = "Gruber Estado"
        self.email_subject = "Gruber Estado"
        self.email_body = None
        self.entorno = ENTORNO  # Define ENTORNO como un atributo
        self.email_to = [EMAIL_TO, "restevean@gmail.com"] if (self.entorno == "prod"
                                                              and EMAIL_TO) else ["restevean@gmail.com"]

    # TODO Need to get SFTP data in order to download files in /fixtures
    def load_dir_files(self):
        files_in_dir = {
            file: {
                "success": False,
                "file_name": "",
                "message": "",
                "misc": None
            }
            for file in os.listdir(self.work_directory)
            if os.path.isfile(os.path.join(self.work_directory, file))
        }
        print(files_in_dir)
        return files_in_dir


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

        for lineas in results.get("Lineas", []):
            if lineas.get("Record type ‘Q10’") == "Q10":
                n_ref_cor = lineas.get("Consignment number sending depot")
                n_status = lineas.get("Status code")
                m_query = f"select ipda, * from trapda where nrefcor ='{n_ref_cor}'"
                print(m_query)  # Eliminar en producción
                query_reply = bm.consulta_(m_query)
                print(query_reply)  # Eliminar en producción
                if "contenido" in query_reply and query_reply["contenido"]:
                    n_ipda = query_reply["contenido"][0].get("ipda")
                    n_json = {
                        "codigohito": self.get_cod_hito(n_status),
                        "descripciontracking": file_name.rsplit("/", 1)[-1],
                        "fechatracking": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                    }
                    n_cpda = query_reply["contenido"][0].get("cpda")

                    # Excribimos el fichero json
                    # if n_cpda:
                    #     json_dir = os.path.join(self.work_directory, "json_files")
                    #     json_file = os.path.join(json_dir, f"par_{file_name.rsplit('/', 1)[-1]}_{n_cpda.strip()}.json")
                    #     os.makedirs(json_dir, exist_ok=True)
                    #     with open(json_file, "w") as f:
                    #         f.write(str(n_json))

                    tracking_reply = bm.post_partida_tracking(n_ipda, n_json)
                    self.files[file_name.rsplit("/", 1)[-1]]["file_name"] = file_name.rsplit("/", 1)[-1]
                    if tracking_reply["status_code"] == 201:
                        self.files[file_name.rsplit("/", 1)[-1]]["success"] = True
                        self.files[file_name.rsplit("/", 1)[-1]]["message"] = f"\nCreada partida {n_ipda}"
                        print("Success")
                    else:
                        self.files[file_name.rsplit("/", 1)[-1]]["success"] = False
                        self.files[file_name.rsplit("/", 1)[-1]]["message"] = f"\nNO Creada partida {n_ipda}"
                        print("Fail")
                else:
                    n_ipda = None  # O asigna otro valor predeterminado si lo prefieres
                print(f"El ipda es: {n_ipda}")


    def run(self):
        email_sender = EmailSender()  # Instanciamos `EmailSender` una vez

        for file_name, file_info in self.files.items():
            self.file_process(os.path.join(self.work_directory, file_name))
            target_dir = os.path.join(self.work_directory, "success" if file_info["success"] else "fail")
            os.makedirs(target_dir, exist_ok=True)  # Crea el directorio si no existe

            os.rename(
                os.path.join(self.work_directory, file_name),
                os.path.join(target_dir, file_name)
            )

            self.email_body = f"{file_info['message']}\nArchivo: {file_name}"
            print(self.email_body)  # Para depuración; usa logging en producción
            email_sender.send_email(self.email_from, self.email_to, self.email_subject, self.email_body)


if __name__ == "__main__":
    estado_grub_anex = EstadoGruAne()
    estado_grub_anex.run()
