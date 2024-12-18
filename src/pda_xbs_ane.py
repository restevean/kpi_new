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
import json
# import psycopg2

# Activamos logging
logging.basicConfig(
    level=logging.DEBUG,     # Nivel mínimo de mensajes a mostrar
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje
)
logger = logging.getLogger(__name__)

# Empezamos con pathlib y obtenemos el path
base_dir = Path(__file__).resolve().parent

load_dotenv(dotenv_path=base_dir.parent / "conf" / "env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
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
        self.bx = BorderoXBS()
        self.bm = BmApi()
        self.enc_exp = False


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
            # TODO ¡Ojo! en arc los archivos contenían 'BOLLE', ¿aquí como los diferenciamos de los otros?
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
            # TODO Ojo!, los archivos deben contener 'BORD512' en el nombre.
            archivos_filtrados = [
                file for file in archivos_remotos
                if 'BORD512' in file.upper() and (n_ftp.sftp.stat(file).st_mode & 0o170000 == 0o100000)
            ]
            # archivos_filtrados = archivos_remotos

            if not archivos_filtrados:
                logger.info(" --- No se encontraron para descargar.")
            else:
                for file in archivos_filtrados:
                    local_path = os.path.join(self.local_work_directory, file)
                    logger.info(" --- Descargando nuevos archivos")

                    try:
                        n_ftp.download_file(file, local_path)
                        logger.info(f" --- Descargando {file} a {local_path}")
                    except Exception as e:
                        logger.error(f" --- Error al descargar {file}: {e}")
                logger.info(" --- Descarga de archivos completada")

        except Exception as e:
            logger.error(f" --- Error durante la descarga de archivos: {e}")
        finally:
            n_ftp.disconnect()
            logger.info(" --- Desconectado del servidor FTP")


    def files_process(self, n_path):
        logger.info(" --- Iniciando el procesamiento de archivos...")
        bm = BmApi()
        email_sender = EmailSender()
        encontrado_expediente = False
        mensaje = ""

        for file, info in self.files.items():
            file_path = os.path.join(n_path, file)
            info['path'] = file_path

            try:
                logger.info(f" --- Procesando archivo: {file_path}")

                with open(file_path, 'rt') as archivo:
                    ipda = 0
                    cpda = ""
                    # bx = BorderoXBS()
                    trip = ""

                    # Nos leemos el archivo entero antes de procesarlo y generamos el json
                    for fila in archivo:
                        if fila[:4] == '@@PH':
                            continue
                        elif fila[:3] == 'A00':
                            cab_partida = self.bx.cabecera_xbs(fila=fila)
                        else:
                            n_linea = self.bx.linea_xbs(fila)

                    trip = self.bx.expediente_ref_cor()
                    # self.bx.genera_json_bordero(path= self.local_work_processed / f"Bordero_{trip}:{file}")
                    self.bx.genera_json_bordero(path= self.local_work_processed / file)
                    archivo.seek(0)

                    # Procesamos la fila
                    for fila in archivo:

                        if fila[:11] == '@@PHBORD100':
                            continue
                        # Si es cabecera
                        if fila[:3] == 'A00':

                            # Por claridad procesamos la cabecera en un method
                            process_header_data = self.header_process(file, fila, info)
                            ipda = process_header_data[0]
                            cpda = process_header_data[1]
                            info = process_header_data[2]
                            trip = process_header_data[3]

                        if not encontrado_expediente:
                            break

                        # No es cabecera y se ha encontrado expediente (LINEAS)
                        elif encontrado_expediente:

                            # Por claridad procesamos la línea en un method
                            self.line_process(fila, ipda, cpda, info)

                    if not info["process_again"]:
                        self.bx.genera_json_bordero(path= self.local_work_processed / f"Bordero_{trip}{file}")

                info["success"] = True
                info["n_message"] = mensaje
                logger.info(f"Archivo procesado exitosamente {file}.")
            except Exception as e:
                info["success"] = False
                mensaje += str(e)
                info["n_message"] = mensaje
                logger.error(f"\nError al procesar {file}: {e}\n")
            finally:
                logger.info("Procesamiento de archivo completado.")


        # Movemos los archivos a processed o process_pending según corresponda
        # Enviamos los correos
        for file, info in self.files.items():
            # email_sender.send_email("javier@kpianalisis.com", self.email_to, f"Partida: {cpda}", info["message"])
            # Movemos a las carpetas según el resultado del proceso
            if info["process_again"]:
                os.rename(info["path"], f"{self.local_work_pof_process / file}")
                os.rename(self.local_work_processed / f"{file}_Bordero{trip}.json", self.local_work_pof_process /
                          f"{file}_Bordero{trip}.json")
            else:
                os.rename(info["path"], f"{self.local_work_processed / file}")


    def header_process(self, file, row, info):
        logging.info(f" --- Cabecera: \n --- {row}")
        cab_partida = self.bx.cabecera_xbs(fila=row)
        trip = self.bx.expediente_ref_cor()  # 2024MO12103
        self.enc_exp = False
        mensaje = ""

        # Eliminar al descomentar comunicar la partida
        ipda = 0
        cpda = ""

        # Buscamos el expediente
        query = f"select * from traexp where nrefcor in ('{trip}', '{n_ref(trip)}')"
        query_reply = self.bm.n_consulta(query=query)

        #  Si existe el expediente (si la consulta tiene contenido)
        if query_reply["contenido"]:
            self.enc_exp = True

            # if f"\nEncontrado el expediente: {query_reply['contenido'][0]['cexp']}" not in mensaje:
            if f"\nEncontrado el expediente: {query_reply['contenido'][0]['cexp']}" not in info["n_message"]:
                info["expediente"] = query_reply['contenido'][0]['cexp']
                mensaje += f"\nEncontrado el expediente: {query_reply['contenido'][0]['cexp']}"
                logger.info(f" --- Encontrado expediente {query_reply["contenido"][0]["cexp"]}")
            expediente_bm = query_reply["contenido"][0]
            pda_json_arc = self.partida_json(cabecera=cab_partida, expediente=expediente_bm)

            # Buscamos la partida
            n_partida = n_ref(cab_partida["Order Full Number"].strip())
            query_existe = f"SELECT ipda, cpda FROM trapda WHERE nrefcor='{n_partida}'"
            query_existe_reply = self.bm.n_consulta(query=query_existe)

            # Si no existe la partida, comunicamos partida
            if not query_existe_reply["contenido"]:
                logging.debug(f" --- Hemos de comunicar la partida {n_ref(cab_partida['Order Full Number'
                                                                          ].strip())}")
                resp_partida = self.bm.post_partida(data_json=pda_json_arc)

                # Si exito al comunicar la partida
                if resp_partida["status_code"] == 201:
                    ipda = resp_partida["contenido"]["id"]
                    cpda = resp_partida["contenido"]["codigo"]
                    mensaje += f"\nCreada partida {resp_partida['contenido']['codigo']}"
                    logging.debug(f" --- Creada partida {resp_partida['contenido']['codigo']}")

                # Si falla comunicar la partida
                else:
                    errores = "\n".join(
                        e["Descripcion"] for e in resp_partida["contenido"]["Errores"])
                    mensaje += f"\nNo se ha creado la partida {n_ref(cab_partida['Order Full Number'
                                                                     ].strip())} \n{errores}\n"
                    logging.debug(logging.debug(f" --- No se ha creado la partida {n_ref(cab_partida['Order Full Number'
                                                                                         ].strip())} \n{errores}\n"))

            # Si existe la partida
            else:
                ipda = query_existe_reply["contenido"][0]["ipda"]
                cpda = query_existe_reply["contenido"][0]["cpda"]
                mensaje += (f"\nExiste la partida {query_existe_reply['contenido'][0]['cpda']}, "
                            f"ref corresponsal: {n_ref(cab_partida['Order Full Number'].strip())}")
                logging.debug(f" --- Existe la partida {query_existe_reply['contenido'][0]['cpda']}, "
                              f"ref corresponsal: {n_ref(cab_partida['Order Full Number'].strip())}")

        # No existe el expediente (la consulta no tiene contenido)
        else:
            info["process_again"] = True
            # self.bx.genera_json_bordero(path= self.local_work_processed / f"Bordero_{trip}{file}")
            mensaje += f"\n No existe el expediente {trip} {n_ref(trip)}"

        # Almacenamos el mensaje y retornamos
        info["n_message"] += mensaje
        logging.debug(f" --- No existe el expediente {trip} {n_ref(trip)}")
        return [ipda, cpda, info, trip]


    def line_process(self, row, ipda, cpda, info):
        logging.info(f" --- Línea: \n --- {row}")
        n_row = self.bx.linea_xbs(row)
        mensaje = ""

        # Si tiene nº ipda (partida) y barcode (algún bulto)
        if ipda > 0 and n_row["Barcode"].strip():

            # Comprobamos que no existe ese bulto
            query_barcode = query = (f"SELECT COUNT(1) AS cuenta FROM ttemereti WHERE ipda={ipda} "
                                     f"AND dcodbar='{n_row['Barcode'].strip()}'")
            query_barcode_reply = self.bm.n_consulta(query=query_barcode)

            # Si no existe el bulto
            if not query_barcode_reply:
                json_etiqueta = {
                    "codigobarras": n_row["Barcode"].strip(),
                    "altura": float(n_row["Hight"].strip()) / 100,
                    "ancho": float(n_row["Width"].strip()) / 100,
                    "largo": float(n_row["Lenght"].strip()) / 100,
                }
                #  Enchufamos el bulto
                resp_etiqueta = self.bm.post_partida_etiqueta(id=ipda, data_json=json_etiqueta)
                # Actualizamos el emnsaje en función del resultado
                if resp_etiqueta["cod_error"] == 201:
                    mensaje += f"\nSubida Etiqueta. {n_row['Barcode'].strip()}"
                else:
                    mensaje += (f"\nYa existe la etiqueta {n_row['Barcode'].strip()} "
                                f"de la "
                                f"partida {cpda}")
            # Si existe el bulto
            else:
                mensaje += f"\nSe encontró el bulto {n_row['Barcode'].strip()}"
                logging.info("Bulto encontrado")

        else:
            logging.info(f"No es cabecera, cpda: {cpda}, ipda: {ipda}, barcode: {n_row['Barcode'].strip()}")


    def run(self):

        # Nos ocupamos primero de los descargados antes y sin iexp/cexp
        logger.info("\nIniciando proceso de archivos que estaban pendientes de procesar")
        # pda_xbs = PartidaXbsAne()
        self.load_dir_files(self.local_work_pof_process)
        if self.files:
            # self.files_process(self.local_work_pof_process)
            logging.info("Procesados los archivos que estaban pendientes de procesar")

        # Procesamos los recién descargados del FTP
        logger.info("\nIniciando proceso de archivos descargados")
        self.download_files()
        # self.files = None
        # self.files = self.load_dir_files(self.local_work_directory)
        self.files = self.load_dir_files()
        if self.files:
            self.files_process(self.local_work_directory)
            logging.info("Procesados los archivos descargados de FTP")


if __name__ == "__main__":
    partida = PartidaXbsAne()
    partida.run()
