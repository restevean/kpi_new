# src/pda_arc_ane

import os
import logging
from dotenv import load_dotenv
from utils.sftp_connect import SftpConnection

logging.basicConfig(
    level=logging.INFO,  # Nivel mínimo de mensajes a mostrar
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje
)

logger = logging.getLogger(__name__)

load_dotenv(dotenv_path="../conf/.env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")

if ENTORNO == "pro":
    load_dotenv(dotenv_path=f"../conf/.env.{INTEGRATION_CUST}{ENTORNO}")
    logger.info(f"Cargando configuración: ../conf/.env.{INTEGRATION_CUST}{ENTORNO}")
else:
    load_dotenv(dotenv_path=f"../conf/.env.{INTEGRATION_CUST}dev")
    logger.info(f"Cargando configuración: ../conf/.env.{INTEGRATION_CUST}dev")

class PartidaArcAne:

    def __init__(self):
        self.entorno = ENTORNO
        self.sftp_url = os.getenv("SFTP_SERVER_ARC")
        self.sftp_user = os.getenv("SFTP_USER_ARC")
        self.sftp_pw = os.getenv("SFTP_PW_ARC")
        self.sftp_port = os.getenv("SFTP_PORT_ARC")
        self.remote_work_out_directory = os.getenv("PDA_PATH_ARC")
        self.local_work_directory = "../fixtures/arc/edi"
        self.local_work_pof_process = "/pending_of_process"
        self.files = None

    def download_files(self):
        n_sftp = SftpConnection()
        try:
            logger.info("Estableciendo conexión SFTP...")
            n_sftp.connect()
            logger.info("Conexión SFTP establecida correctamente.")

            if not os.path.exists(self.local_work_directory):
                os.makedirs(self.local_work_directory)
                logger.info(f"Creado directorio local: {self.local_work_directory}")

            n_sftp.sftp.chdir(self.remote_work_out_directory)
            logger.info(f"Accediendo al directorio remoto: {self.remote_work_out_directory}")

            archivos_remotos = n_sftp.sftp.listdir()
            archivos_filtrados = [
                file for file in archivos_remotos
                if 'BOLLE' in file.upper() and (n_sftp.sftp.stat(file).st_mode & 0o170000 == 0o100000)
            ]

            if not archivos_filtrados:
                logger.info("No se encontraron para descargar.")
            else:
                for file in archivos_filtrados:
                    local_path = os.path.join(self.local_work_directory, file)
                    logger.info(f"Descargando {file} a {local_path}")
                    n_sftp.sftp.get(file, local_path)
                logger.info("Descarga de archivos completada.")

        except Exception as e:
            logger.error(f"Error durante la descarga de archivos: {e}")
        finally:
            n_sftp.disconnect()
            logger.info("Desconectado del servidor SFTP.")

    def load_dir_files(self, subdir=""):
        directory = os.path.join(f"{self.local_work_directory}{subdir}")
        return {
            file: {
                "success": False,
                "message": "",
            }
            for file in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, file)) and 'BOLLE' in file
        }

    def file_process(self, n_path):
        logger.info("Iniciando el procesamiento de archivos...")
        for file, info in self.files.items():
            file_path = os.path.join(n_path, file)
            try:
                logger.info(f"Procesando archivo: {file_path}")
                # Proceso del archivo
                # 1.- Comprobamos si tiene expediente
                # Si lo tiene: enchufamos las partidas
                # Si no lo tiene: movemos el archivo a "pending_to_process" si no está ya allí y
                #    enviamos un correo
                ...
                info["success"] = True
                info["message"] = "Procesado correctamente."
                logger.info(f"Archivo {file} procesado exitosamente.")
            except Exception as e:
                info["success"] = False
                info["message"] = str(e)
                logger.error(f"Error al procesar {file}: {e}")
        logger.info("Procesamiento de archivos completado.")

    def run(self):

        # Nos ocupamos primero de los descargados antes y sin iexp/cexp
        logger.info("Iniciando proceso de archivos pendientes de nrefcor")
        pda_arc = PartidaArcAne()
        pda_arc.files = self.load_dir_files(self.local_work_pof_process)
        pda_arc.file_process(f"{self.load_dir_files}{self.local_work_pof_process}")

        # Ahora procesamos los recién descargados
        logger.info("Descargando nuevos archivos")
        pda_arc.download_files()
        pda_arc.files = self.load_dir_files()
        if pda_arc.files:
            pda_arc.file_process(f"{self.load_dir_files}")
        logger.info("Proceso completado")


if __name__ == "__main__":
    partida = PartidaArcAne()
    partida.run()









"""
x 1.- Revisamos la carpeta temporal "pending_of_process"
2.- Procesamos los archivos que contiene:
    Si hay archivos
        Comprobamos si tienen nrefcor asignado:
            En caso de que si: procesamos archivo, enchufamos las partidas
            En caso de que no: no hacemos nada
x 3.- Descargamos los archivos del FTP
4.- Procesamos los archivos descargados
        Comprobamos si tienen nrefcor asignado:
            En caso de que si: enchufamos partidas
            En caso de que no: los movemos a la carpeta "pending_of_process"
"""