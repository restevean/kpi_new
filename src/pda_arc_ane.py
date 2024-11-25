import os
from dotenv import load_dotenv
from utils.sftp_connect import SftpConnection


load_dotenv(dotenv_path="../conf/.env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
if ENTORNO == "pro":
    load_dotenv(dotenv_path=f"../conf/.env.{INTEGRATION_CUST}{ENTORNO}")
    print(f"../conf/.env.{INTEGRATION_CUST}")
else:
    load_dotenv(dotenv_path=f"../conf/.env.{INTEGRATION_CUST}{ENTORNO}")
    print(f"../conf/.env.{INTEGRATION_CUST}dev")


class PartidaArcAne:

    def __init__(self  ):
        self.entorno = ENTORNO
        self.sftp_url = os.getenv("SFTP_SERVER_ARC")
        self.sftp_user = os.getenv("SFTP_USER_ARC")
        self.sftp_pw = os.getenv("SFTP_PW_ARC")
        self.sftp_port = os.getenv("SFTP_PORT_ARC")
        self.remote_work_out_directory = os.getenv("PDA_PATH_ARC")
        self.local_work_directory = "../fixtures/pda"

    def download_files(self):
        n_sftp = SftpConnection()
        n_sftp.connect()

        if not os.path.exists(self.local_work_directory):
            os.makedirs(self.local_work_directory)

        n_sftp.sftp.chdir(self.remote_work_out_directory)
        for file in n_sftp.sftp.listdir():
            if n_sftp.sftp.stat(file).st_mode & 0o170000 == 0o100000:  # Verifica si es un archivo regular
                local_path = os.path.join(self.local_work_directory, file)
                # print(f"Descargando {file} a {local_path}")
                n_sftp.sftp.get(file, local_path)
        n_sftp.disconnect()


if __name__ == "__main__":
    PartidaArcAne().download_files()






"""
1.- Revisamos la carpeta temporal "pending_of_process"
2.- Procesamos los archivos que contiene:
    Si hay archivos
        Comprobamos si tienen nrefcor asignado:
            En caso de que si: procesamos archivo, enchufamos las partidas
            En caso de que no: no hacemos nada
3.- Descargamos los archivos del FTP
4.- Procesamos los archivos descargados
        Comprobamos si tienen nrefcor asignado:
            En caso de que si: enchufamos partidas
            En caso de que no: los movemos a la carpeta "pending_of_process"
"""