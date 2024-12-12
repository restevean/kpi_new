# src/pda_arc_ane_new.py

import os
import logging
from dotenv import load_dotenv
from jsonmerge import merge
from utils.sftp_connect import SftpConnection
from utils.bordero import BorderoArcese
from utils.bmaster_api import BmasterApi as BmApi
from utils.misc import n_ref
from utils.buscar_empresa import busca_destinatario
from utils.send_email import EmailSender
from utils.logger_config import setup_logger
from pathlib import Path


# Activamos logging
setup_logger()
logger = logging.getLogger(__name__)

# Gestionamos adecuadamente los path
base_dir = Path(__file__).resolve().parent

load_dotenv(dotenv_path=base_dir.parent / "conf" / ".env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=base_dir.parent / "conf" / f".env.{INTEGRATION_CUST}{ENTORNO}")
logger.info(f"Cargando configuración: {INTEGRATION_CUST}{ENTORNO}")


class PartidaArcAne:

    def __init__(self):
        self.entorno = ENTORNO
        self.base_path = base_dir
        self.sftp_url = os.getenv("SFTP_SERVER_ARC")
        self.sftp_user = os.getenv("SFTP_USER_ARC")
        self.sftp_pw = os.getenv("SFTP_PW_ARC")
        self.sftp_port = os.getenv("SFTP_PORT_ARC")
        self.email_from = "Arcese Partida"
        self.email_to = ["javier@kpianalisis.com, dgorriz@anexalogistica.com, trafico2@anexalogistica.com"]
        self.remote_work_out_directory = os.getenv("SFTP_PDA_PATH_ARC")
        self.local_work_directory = self.base_path.parent / "fixtures" / "arc" / "edi"
        self.local_work_pof_process = self.local_work_directory / "process_pending"
        self.local_work_processed = self.local_work_directory / "processed"
        self.files = None
        self.ba = BorderoArcese()
        self.bm = BmApi()
        self.enc_exp = False


    # Buscamos el ordenante
    @staticmethod
    def expediente_ordenante(cabecera_=None):
        if cabecera_ is None:
            cabecera_ = {}
        empresa = busca_destinatario(rsocial=cabecera_["Sender Company Name"].strip(),
                                     codpostal=cabecera_["Sender ZipCode"].strip(),
                                     cpais=cabecera_["Sender Country Code"].strip())
        logging.info(empresa)
        if empresa["ient"] > 0:
            return {"expedidor": {"id": empresa["ient"],}}
        else:
            return {
                "expedidor": {
                    "division": "VLC",
                    "descripcion": cabecera_["Sender Company Name"].strip() + " +++(BORRAR)+++",
                    "empresa": {
                        "division": "VLC",
                        "codigopaisfiscal": cabecera_["Sender Country Code"].strip(),
                        "descripcion": cabecera_["Sender Company Name"].strip(),
                        "tipopersona": "N",
                        "nombreempresafisica": cabecera_["Sender Company Name"].strip()[:19],
                        "direccionfiscal": {
                            "direccion": cabecera_["Sender Address"].strip(),
                            "poblacion": cabecera_["Sender Place"].strip(),
                            "codigopostal": cabecera_["Sender ZipCode"].strip(),
                            "codigopais": cabecera_["Sender Country Code"].strip()
                        }
                    },
                }
            }


    @staticmethod
    def expediente_destinatario(cabecera_= None):

        if cabecera_ is None:
            cabecera_ = {}

        empresa = busca_destinatario(
            rsocial=cabecera_["Destination Company Name"].strip(),
            codpostal=cabecera_["Destination ZipCode"].strip(),
            cpais=cabecera_["Destination Country Code"].strip()
        )

        if (empresa["ient"] > 0):
            return {"destinatario": {"id": empresa["ient"]}}
        else:
            # json_destinatario = {
            return {
                "destinatario": {
                    "division": "VLC",
                    "descripcion": cabecera_["Destination Company Name"].strip() + " +++(BORRAR)+++",
                    "codigorelacioncliente": cabecera_["Destination Company Name"].strip()[:19],
                    "empresa": {
                        "codigopaisfiscal": cabecera_["Destination Country Code"].strip(),
                        "direccionfiscal": {
                            "direccion": cabecera_["Destination Address"].strip(),
                            "poblacion": cabecera_["Destination Place"].strip(),
                            "codigopostal": cabecera_["Destination ZipCode"].strip(),
                            "codigopais": cabecera_["Destination Country Code"].strip()
                        }
                    },
                    "direcciones": [
                        {
                            "direccion": cabecera_["Destination Address"].strip(),
                            "poblacion": cabecera_["Destination Place"].strip(),
                            "codigopostal": cabecera_["Destination ZipCode"].strip(),
                            "codigopais": cabecera_["Destination Country Code"].strip(),
                        }
                    ],
                }
            }


    # Convertimos la partida a json
    def partida_json(self, expediente=None, cabecera=None):
        if expediente is None:
            expediente = {}
        if cabecera is None:
            cabecera = []

        destinatario_json_data = self.expediente_destinatario(cabecera_=cabecera)
        expedidor_json_data = self.expediente_ordenante(cabecera_=cabecera)

        partida_json = {
            "division": "VLC",
            "estado": "FBLE",
            "expediente": expediente["cexp"],
            "trafico": "TI",
            "tipotrafico": "TER",
            "flujo": "I",
            "refcliente": expediente["ientrefcli"],  # ya viene en el expediente
            "refcorresponsal": n_ref(cabecera['Order Full Number'].strip()),
            "tipocarga": "C",
            "puertoorigen": "TIRARCCAM",  # MODENA --> TIRARCCAM, ROMA YA LO CREARÁ --> TIRARCRIA
            "puertodestino": "TIRANERIB",  # ANEXA TIRANERIB
            "paisorigen": cabecera["Origin Country Code"].strip(),
            "paisdestino": cabecera["Destination Country Code"].strip(),
            "portes": "s",
            "incoterm": cabecera["Additional Requirements"].strip(),
            "servicio": "NOR",
            "bultos": int(cabecera["Quantity"].strip()),
            "tipobultos": "PX",
            "pesobruto": float(cabecera["Total GrossWeight"].strip()) / 100,
            "pesoneto": float(cabecera["Total GrossWeight"].strip()) / 100,
            "pesotasable": float(cabecera["Total GrossWeight"].strip()) / 100,
            "volumen": float(cabecera["Total Volume"].strip()) / 100,
            "seguro": "S",
            "valorasegurable": float(cabecera["Insured Amount"].strip()) / 100,
            "divisa": "EUR",
            "divisavinculacion": "EUR",
            "tipovinculacion": "SV",
            "aduanas": "N",
            "gdp": "N",
            "almacenlogico": "CROS",
            "generarentradaalmacen": False,
            "generaralbarandistribucion": False,
            "tipotransportenacional": "S",
            "tipotransporteextranjero": "S",
            "fechaprevistasalida": expediente["fpresal"],  # "2024-01-17T22:57:24.737Z",
            "fechasalida": expediente["fsal"],  # "2024-01-17T22:57:24.737Z",
            "fechaprevistallegada": expediente["fprelle"],  # "2024-01-17T22:57:24.737Z",
            "fechallegada": expediente["flle"],  # ,
        }
        if expedidor_json_data is not None:
            partida_json = merge(partida_json, expedidor_json_data)

        if partida_json["incoterm"] == "DAP":
            json_fact = {
                "clientefacturacion": {
                    "id": expediente["ientcor"]}
            }
            partida_json = merge(partida_json, json_fact)

        if destinatario_json_data is not None:
            partida_json = merge(partida_json, destinatario_json_data)
        return partida_json


    # Descarga los archivos del directorio remoto
    def download_files(self):
        n_sftp = SftpConnection(self.sftp_url, self.sftp_user, self.sftp_pw, self.sftp_port)

        try:
            logger.debug(" --- Estableciendo conexión SFTP...")
            n_sftp.connect()
            logger.debug(" --- Conexión SFTP establecida correctamente.")

            if not os.path.exists(self.local_work_directory):
                os.makedirs(self.local_work_directory)
                logger.debug(f" --- Creado directorio local: {self.local_work_directory}")

            n_sftp.sftp.chdir(self.remote_work_out_directory)
            logger.debug(f" --- Accediendo al directorio remoto: {self.remote_work_out_directory}")

            archivos_remotos = n_sftp.sftp.listdir()
            archivos_filtrados = [
                file for file in archivos_remotos
                if 'BOLLE' in file.upper() and (n_sftp.sftp.stat(file).st_mode & 0o170000 == 0o100000)
            ]

            if not archivos_filtrados:
                logger.info(" --- No se encontraron para descargar.")
            else:
                for file in archivos_filtrados:
                    local_path = os.path.join(self.local_work_directory, file)
                    try:
                        logger.info(f" --- Descargando {file} a {local_path}")
                        n_sftp.sftp.get(file, local_path)
                    except Exception as e:
                        logger.error(f" --- Error al descargar {file}: {e}")
                logger.info(" --- Descarga de archivos completada.")

        except Exception as e:
            logger.error(f" --- Error durante la descarga de archivos: {e}")
        finally:
            n_sftp.disconnect()
            logger.debug(" --- Desconectado del servidor SFTP.")


    # Carga los archivos del directorio
    def load_dir_files(self, subdir=""):
        subdir = self.local_work_directory if subdir == "" else subdir

        return {
            file: {
                "success": False,
                "n_message": "",
                "process_again": False,
                "path": "",
                "expediente": "",
            }
            for file in os.listdir(subdir)
            if os.path.isfile(os.path.join(subdir, file)) and 'BOLLE' in file
        }

    def files_process(self, n_path):
        logger.info(" --- Iniciando el procesamiento de archivos...")
        email_sender = EmailSender()
        # enc_exp = False

        for file, info in self.files.items():
            file_path = n_path / file
            info['path'] = file_path
            mensaje = ""

            try:
                logger.info(f" --- Procesando archivo: {file_path}")

                # Proceso del archivo individual
                with open(file_path, 'rt') as archivo:
                    ipda = 0
                    cpda = ""
                    n_expediente = ""

                    # Procesamos la fila
                    for fila in archivo:
                        # Si es cabecera
                        if fila[:2] == '01':
                            # Procesamos la cabecera
                            process_header_data = self.header_process(fila, info)
                            ipda = process_header_data[0]
                            cpda = process_header_data[1]
                            info = process_header_data[2]
                            n_expediente = process_header_data[3]

                        # No es cabecera y se ha encontrado expediente (LINEAS)
                        elif self.enc_exp:
                            self.line_process(fila, ipda, cpda, info)

                    # Si no hay que procesarlo de nuevo generamos el json
                    if not info["process_again"]:
                        self.ba.genera_json_bordero(path=self.local_work_processed / f"Bordero_{n_expediente}{file}")

                info["success"] = True
                logger.info(f" --- Archivo procesado con éxito {file}.")
            except Exception as e:
                info["success"] = False
                mensaje += str(e)
                logger.error(f" --- Error al procesar {file}: {e}\n")

        # Movemos los archivos a processed o process_pending según corresponda
        # Enviamos los correos
        for file, info in self.files.items():
        #     # Enviamos los correos
        #     email_sender.send_email(from_addrs=self.email_from, to_addrs=self.email_to,
        #                             subject=f"Arcese Subir partida {info["expediente"]}", body=info["n_message"])
            # Movemos a las carpetas según el resultado del proceso
            if info["process_again"]:
                os.rename(info["path"], f"{self.local_work_pof_process / file}")
            else:
                os.rename(info["path"], f"{self.local_work_processed / file}")


    def header_process(self, row, info):
        cab_partida = self.ba.cabecera_arcese(fila=row)
        trip = self.ba.expediente_ref_cor()  # 2024MO12103
        self.enc_exp = False
        mensaje = ""

        #eliminar al descomentar comunicar la partida
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
                """
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
                """

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
            mensaje += f"\n No existe el expediente {trip} {n_ref(trip)}"

        # Almacenamos el mensaje y retornamos
        info["n_message"] += mensaje
        logging.debug(f" --- No existe el expediente {trip} {n_ref(trip)}")
        return [ipda, cpda, info, trip]

    def line_process(self, row, ipda, cpda, info):
        n_row = self.ba.linea_arcese(row)
        mensaje = ""

        # Si tiene nº ipda (partida) y barcode (algún bulto)
        if ipda > 0 and n_row["Barcode"].strip():

            # Comprobamos que no existe ese bulto
            query_barcode = query = (f"SELECT COUNT(1) AS cuenta FROM ttemereti WHERE ipda={ipda} "
                                     f"AND dcodbar='{n_row['Barcode'].strip()}'")
            query_barcode_reply = self.bm.n_consulta(query=query_barcode)

            # Si no existe barcode (el bulto)
            if not query_barcode_reply:
                json_etiqueta = {
                    "codigobarras": n_row["Barcode"].strip(),
                    "altura": float(n_row["Hight"].strip()) / 100,
                    "ancho": float(n_row["Width"].strip()) / 100,
                    "largo": float(n_row["Lenght"].strip()) / 100,
                }
                #  Añadimos el bulto
                resp_etiqueta = self.bm.post_partida_etiqueta(id=ipda, data_json=json_etiqueta)
                # Actualizamos el mensaje en función del resultado
                if resp_etiqueta["cod_error"] == 201:
                    mensaje += f"\nSubida Etiqueta. {n_row['Barcode'].strip()}"
                else:
                    # TODO ¿No debería decir aquí "Error  al añadir la etiquetq"?
                    mensaje += (f"\nYa existe la etiqueta {n_row['Barcode'].strip()} de la partida {cpda}")

            # Si existe el bulto
            else:
                mensaje += f"\nSe encontró el bulto {n_row['Barcode'].strip()}"
                logging.info("Bulto encontrado")

        else:
            logging.info(
                f"No es cabecera, cpda: {cpda}, ipda: {ipda}, barcode: {n_row['Barcode'].strip()}")

        # Guardar el mensaje (+=) en info["n_message"]
        info["n_message"] += mensaje
        ...


    def run(self):
        # Nos ocupamos primero de los descargados antes y sin iexp/cexp
        logger.info(" --- Iniciando proceso de archivos que estaban pendientes de procesar")
        self.files = self.load_dir_files(self.local_work_pof_process)
        if self.files:
            self.files_process(self.local_work_pof_process)
            logging.info(" --- Procesados los archivos que estaban pendientes de procesar")

        # Procesamos los recién descargados del FTP
        logger.info(" --- Iniciando proceso de archivos descargados")
        logger.info(" --- Descargando nuevos archivos")
        # self.download_files()
        self.files = self.load_dir_files()
        if self.files:
            self.files_process(self.local_work_directory)
            logging.info(" --- Procesados los archivos descargados de FTP")


if __name__ == "__main__":
    partida = PartidaArcAne()
    partida.run()
