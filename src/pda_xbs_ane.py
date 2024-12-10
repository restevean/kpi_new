# src/pda_xbs_ane.py

import os
import logging
from dotenv import load_dotenv
from jsonmerge import merge
from utils.ftp_connect import FtpConnection
from utils.bordero_xbs import BorderoXBS
from utils.bmaster_api import BmasterApi as BmApi
from utils.misc import n_ref
from utils.buscar_empresa import busca_destinatario
from utils.send_email import EmailSender
from pathlib import Path
# import psycopg2

# Activamos logging
logging.basicConfig(
    level=logging.DEBUG,     # Nivel mínimo de mensajes a mostrar
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje
)

logger = logging.getLogger(__name__)

# Empezamos con pathlib y obtenemos el path
base_dir = Path(__file__).resolve().parent

load_dotenv(dotenv_path="../conf/..env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")

if ENTORNO == "pro":
    load_dotenv(dotenv_path=base_dir.parent / "conf" / f".env.{INTEGRATION_CUST}{ENTORNO}")
    logger.info(f"Cargando configuración: {INTEGRATION_CUST}{ENTORNO}")
else:
    load_dotenv(dotenv_path=base_dir.parent / "conf" / f".env.{INTEGRATION_CUST}{ENTORNO}")
    logger.info(f"Cargando configuración: {INTEGRATION_CUST}{ENTORNO}")


class PartidaXbsAne:

    def __init__(self):
        self.entorno = ENTORNO
        self.base_path = base_dir
        self.ftp_url = os.getenv("FTP_SERVER_XBS")
        self.ftp_user = os.getenv("FTP_USER_XBS")
        self.ftp_pw = os.getenv("FTP_PW_XBS")
        self.ftp_port = int(os.getenv("FTP_PORT_XBS"))
        self.email_from = "javier@kpianalisis.com"
        self.email_to = "resteve24@gmail.com"
        self.remote_work_out_directory = os.getenv("FTP_OUT_PATH_XBS")
        self.local_work_directory = self.base_path.parent / "fixtures" / "xbs" / "edi"
        self.local_work_pof_process = self.local_work_directory / "process_pending"
        self.local_work_processed = self.local_work_directory / "processed"
        self.files = None


    def load_dir_files(self, subdir=""):
        # subdir = os.path.join(f"{self.local_work_directory}{subdir}")
        subdir = self.local_work_directory if subdir == "" else subdir
        return {
            file: {
                "success": False,
                "n_message": "",
                "process_again": False,
                "path": ""
            }
            for file in os.listdir(subdir)
            if os.path.isfile(os.path.join(subdir, file))  # and 'BOLLE' in file
        }


    def download_files(self):
        n_ftp = FtpConnection(self.ftp_url, self.ftp_user, self.ftp_pw)

        try:
            n_ftp.connect()
            if not os.path.exists(self.local_work_directory):
                os.makedirs(self.local_work_directory)
                logger.debug(f" --- Creado directorio local: {self.local_work_directory}")

            logger.debug(f" --- Accediendo al directorio remoto: {self.remote_work_out_directory}")
            n_ftp.change_directory(self.remote_work_out_directory)
            archivos_remotos = n_ftp.ftp.nlst()
            # TODO ojo, los archivos deben contener 'BOLLE' en el nombre. O nombramos de alguna manera los archivos o
            #  los los descargamos de una carpeta específica para estado-partida.
            # archivos_filtrados = [
            #     file for file in archivos_remotos
            #     if 'BOLLE' in file.upper() and (n_ftp.sftp.stat(file).st_mode & 0o170000 == 0o100000)
            # ]
            archivos_filtrados = archivos_remotos

            if not archivos_filtrados:
                logger.info(" --- No se encontraron para descargar.")
            else:
                for file in archivos_filtrados:
                    local_path = os.path.join(self.local_work_directory, file)
                    logger.info(" --- Descargando nuevos archivos")

                    try:
                        n_ftp.download_file(file, local_path)
                        logger.info(f" --- Descargando {file} a {local_path}")
                        # logger.info(f"Eliminando {file} del servidor SFTP")
                        # n_ftp.sftp.remove(file)
                    except Exception as e:
                        logger.error(f" --- Error al descargar {file}: {e}")
                logger.info(" --- Descarga de archivos completada")

        except Exception as e:
            logger.error(f" --- Error durante la descarga de archivos: {e}")
        finally:
            n_ftp.disconnect()
            logger.info(" --- Desconectado del servidor FTP")


    def files_process(self, n_path):
        logger.info("\nIniciando el procesamiento de archivos...")
        bm = BmApi()
        email_sender = EmailSender()
        encontrado_expediente = False
        mensaje = ""

        for file, info in self.files.items():
            file_path = os.path.join(n_path, file)
            info['path'] = file_path

            try:
                logger.info(f"\nProcesando archivo: {file_path}")

                # TODO BorderoXBS()
                bx = BorderoXBS()
                archivo = bx.read_xbs_file(file_path)
                # Proceso del archivo individual
                ipda = 0
                cpda = ""

                # with open(file_path, 'rt') as archivo:


                # Procesamos la fila
                for fila in archivo:
                    # Si es cabecera
                    if fila[:3] == 'A00':
                        # cab_partida = bx.registro_a00(fila=fila)
                        trip = bx.expediente_ref_cor()  # 2024MO12103

                        # Buscamos el expediente
                        query = f"select * from traexp where nrefcor in ('{trip}', '{n_ref(trip)}')"
                        query_reply = bm.n_consulta(query=query)

                        # Si la consulta tiene contenido (si hay expediente)
                        if query_reply["contenido"]:
                            encontrado_expediente = True
                            if f"\nEncontrado el expediente: {query_reply['contenido'][0]['cexp']}" not in mensaje:
                                mensaje += f"\nEncontrado el expediente: {query_reply['contenido'][0]['cexp']}"
                                logger.info(f"\nEncontrado expediente {query_reply["contenido"][0]["cexp"]}")
                            expediente_bm = query_reply["contenido"][0]
                            pda_json_xbs = self.partida_json(cabecera=cab_partida, expediente=expediente_bm)

                            nref_value = n_ref(cab_partida["Order Full Number"].strip())
                            query_existe = f"SELECT ipda, cpda FROM trapda WHERE nrefcor='{nref_value}'"

                            # Buscamos la partida
                            query_existe_reply = bm.n_consulta(query=query_existe)

                            # Si no existe la partidda, enchufamos partida
                            if not query_existe_reply["contenido"]:
                                resp_partida = bm.post_partida(data_json=pda_json_xbs)

                                # Si exito al enchufar la partida
                                if resp_partida["status_code"] == 201:
                                    ipda = resp_partida["contenido"]["id"]
                                    cpda = resp_partida["contenido"]["codigo"]
                                    mensaje += f"\nCreada partida {resp_partida['contenido']['codigo']}"

                                # Si falla comunicar la partida
                                else:
                                    errores = "\n".join(
                                        e["Descripcion"] for e in resp_partida["contenido"]["Errores"])
                                    mensaje += f"\nNo se ha creado la partida {n_ref(cab_partida['Order Full Number'
                                                                                     ].strip())} \n{errores}\n"

                            # Si existe la partida
                            else:
                                ipda = query_existe_reply["contenido"][0]["ipda"]
                                mensaje += (f"\nExiste la partida {query_existe_reply['contenido'][0]['cpda']}, "
                                            f"ref corresponsal: {n_ref(cab_partida['Order Full Number'].strip())}\n")

                        # Si no tiene expediente por primera vez
                        else:  # self.local_work_pof_process not in n_path:
                            # Marcamos el fichero para moverlo a process_pending
                            self.files[file]["process_again"] = True
                            # Asignamos el mensaje del correo
                            self.files[file]["n_message"] = mensaje

                        # Si no tiene expediente y es de repesca
                        # else:
                        # El fichero ya está en el path adecuado
                        # Ya se envió correo
                        # Asignamos el mensaje del correo
                        # self.files[file]["n_message"] = mensaje
                        # ...

                    # No es cabecera y se ha encontrado expediente (LINEAS)
                    elif encontrado_expediente:
                        n_linea = bx.linea_arcese(fila)

                        # Si tiene nº ipda (partida) y barcode (algún bulto)
                        if ipda > 0 and n_linea["Barcode"].strip():

                            # Comprobamos que no existe ese bulto
                            query_barcode = query = (f"SELECT COUNT(1) AS cuenta FROM ttemereti WHERE ipda={ipda} "
                                                     f"AND dcodbar='{n_linea['Barcode'].strip()}'")
                            query_barcode_reply = bm.n_consulta(query=query_barcode)

                            # Si no existe el bulto
                            if not query_barcode_reply:
                                json_etiqueta = {
                                    "codigobarras": n_linea["Barcode"].strip(),
                                    "altura": float(n_linea["Hight"].strip()) / 100,
                                    "ancho": float(n_linea["Width"].strip()) / 100,
                                    "largo": float(n_linea["Lenght"].strip()) / 100,
                                }
                                #  Enchufamos el bulto
                                resp_etiqueta = bm.post_partida_etiqueta(id=ipda, data_json=json_etiqueta)

                                # Actualizamos el emnsaje en función del resultado
                                if resp_etiqueta["cod_error"] == 201:
                                    mensaje += f"Subida Etiqueta. {n_linea['Barcode'].strip()}\n\n"

                                else:
                                    mensaje += (f"\nYa existe la etiqueta {n_linea['Barcode'].strip()} "
                                                f"de la "
                                                f"partida {cpda}")
                            # Si existe el bulto
                            else:
                                logging.info("Bulto encontrado")

                        else:
                            logging.info(f"No es cabecera, cpda: {cpda}, ipda: {ipda}, barcode: {n_linea['Barcode'].strip()}")
                            ...

                if not info["process_again"]:
                    # ba.genera_json_bordero(path=f"{self.local_work_processed}/Bordero"
                    #                         f"_{trip}{file}")
                    bx.genera_json_bordero(path= self.local_work_processed / f"Bordero_{trip}{file}")

                info["success"] = True
                info["n_message"] = mensaje
                logger.info(f"Archivo procesado exitosamente {file}.")
            except Exception as e:
                info["success"] = False
                mensaje += str(e)
                info["n_message"] = mensaje
                logger.error(f"\nError al procesar {file}: {e}\n")
            # finally:
                # logger.info("Procesamiento de archivo completado.")


        # Movemos los archivos a processed o process_pending según corresponda
        # Enviamos los correos
        for file, info in self.files.items():
            # email_sender.send_email("javier@kpianalisis.com", self.email_to, f"Partida: {cpda}", info["message"])
            # Movemos a las carpetas según el resultado del proceso
            if info["process_again"]:
                os.rename(info["path"], f"{self.local_work_pof_process / file}")
            else:
                os.rename(info["path"], f"{self.local_work_processed / file}")

    def run(self):

        # Nos ocupamos primero de los descargados antes y sin iexp/cexp
        logger.info("\nIniciando proceso de archivos que estaban pendientes de procesar")
        pda_xbs = PartidaXbsAne()
        pda_xbs.files = self.load_dir_files(self.local_work_pof_process)
        if pda_xbs.files:
            # pda_xbs.files_process(self.local_work_pof_process)
            logging.info("Procesados los archivos que estaban pendientes de procesar")

        # Procesamos los recién descargados del FTP
        logger.info("\nIniciando proceso de archivos descargados")
        pda_xbs.download_files()
        # pda_xbs.files = None
        # pda_xbs.files = self.load_dir_files(self.local_work_directory)
        pda_xbs.files = self.load_dir_files()
        if pda_xbs.files:
            pda_xbs.files_process(self.local_work_directory)
            logging.info("Procesados los archivos descargados de FTP")


if __name__ == "__main__":
    partida = PartidaXbsAne()
    partida.run()