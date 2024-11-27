# src/pda_arc_ane

import os
import logging
from dotenv import load_dotenv
from jsonmerge import merge
from utils.sftp_connect import SftpConnection
from utils.bordero import BorderoArcese
from utils.bmaster_api import BmasterApi as BmApi
from utils.misc import n_ref
from utils.buscar_empresa import busca_destinatario
# import psycopg2

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

if ENTORNO == "pro":
    load_dotenv(dotenv_path=f"../conf/.env.{INTEGRATION_CUST}{ENTORNO}")
    logger.info(f"\nCargando configuración: ../conf/.env.{INTEGRATION_CUST}{ENTORNO}")
else:
    load_dotenv(dotenv_path=f"../conf/.env.{INTEGRATION_CUST}dev")
    logger.info(f"\nCargando configuración: ../conf/.env.{INTEGRATION_CUST}dev")


class PartidaArcAne:

    def __init__(self):
        self.entorno = ENTORNO
        self.sftp_url = os.getenv("SFTP_SERVER_ARC")
        self.sftp_user = os.getenv("SFTP_USER_ARC")
        self.sftp_pw = os.getenv("SFTP_PW_ARC")
        self.sftp_port = os.getenv("SFTP_PORT_ARC")
        self.remote_work_out_directory = os.getenv("SFTP_PDA_PATH_ARC")
        self.local_work_directory = "../fixtures/arc/edi"
        self.local_work_pof_process = "/process_pending"
        self.local_work_processed = "/processed"
        self.files = None

    @staticmethod
    def expediente_ordenante(cabecera_=None):
        if cabecera_ is None:
            cabecera_ = {}
        empresa = busca_destinatario(rsocial=cabecera_["Sender Company Name"].strip(),
                                     codpostal=cabecera_["Sender ZipCode"].strip(),
                                     cpais=cabecera_["Sender Country Code"].strip())
        # print(empresa)
        logging.info(empresa)
        if empresa["ient"] > 0:
            json_ordenante = {
                "expedidor": {
                    "id": empresa["ient"],
                }
            }
        else:
            json_ordenante = {
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
        return json_ordenante

    @staticmethod
    def expediente_destinatario(cabecera_=None):
        if cabecera_ is None:
            cabecera_ = {}
        empresa = busca_destinatario(
            rsocial=cabecera_["Destination Company Name"].strip(),
            codpostal=cabecera_["Destination ZipCode"].strip(),
            cpais=cabecera_["Destination Country Code"].strip()
            )
        if empresa["ient"] > 0:
            json_destinatario = {
                "destinatario": {
                    "id": empresa["ient"],
                }
            }
        else:
            return {
                "destinatario": {
                    "division": "VLC",
                    "descripcion":
                        cabecera_["Destination Company Name"].strip() + " +++(BORRAR)+++",
                    "codigorelacioncliente":
                        cabecera_["Destination Company Name"].strip()[:19],
                    "empresa": {
                        "codigopaisfiscal":
                            cabecera_["Destination Country Code"].strip(),
                        "direccionfiscal": {
                            "direccion": cabecera_["Destination Address"].strip(),
                            "poblacion": cabecera_["Destination Place"].strip(),
                            "codigopostal": cabecera_["Destination ZipCode"].strip(),
                            "codigopais":
                                cabecera_["Destination Country Code"].strip(),
                        },
                    },
                    "direcciones": [{
                        "direccion":
                            cabecera_["Destination Address"].strip(),
                        "poblacion":
                            cabecera_["Destination Place"].strip(),
                        "codigopostal":
                            cabecera_["Destination ZipCode"].strip(),
                        "codigopais":
                            cabecera_["Destination Country Code"].strip(),
                    }],
                }
            }

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

        if destinatario_json_data is not None:
            partida_json = merge(partida_json, destinatario_json_data)
        return partida_json

    def download_files(self):
        # sourcery skip: extract-method, swap-if-else-branches, use-named-expression
        n_sftp = SftpConnection()
        try:
            logger.info("\nEstableciendo conexión SFTP...")
            n_sftp.connect(self.sftp_url, self.sftp_port, self.sftp_user, self.sftp_pw)
            logger.info("\nConexión SFTP establecida correctamente.")

            if not os.path.exists(self.local_work_directory):
                os.makedirs(self.local_work_directory)
                logger.info(f"\nCreado directorio local: {self.local_work_directory}")

            n_sftp.sftp.chdir(self.remote_work_out_directory)
            logger.info(f"\nAccediendo al directorio remoto: {self.remote_work_out_directory}")

            archivos_remotos = n_sftp.sftp.listdir()
            archivos_filtrados = [
                file for file in archivos_remotos
                if 'BOLLE' in file.upper() and (n_sftp.sftp.stat(file).st_mode & 0o170000 == 0o100000)
            ]

            if not archivos_filtrados:
                logger.info("\nNo se encontraron para descargar.")
            else:
                for file in archivos_filtrados:
                    local_path = os.path.join(self.local_work_directory, file)
                    try:
                        logger.info(f"\nDescargando {file} a {local_path}")
                        n_sftp.sftp.get(file, local_path)
                        # logger.info(f"Eliminando {file} del servidor SFTP")
                        # n_sftp.sftp.remove(file)
                    except Exception as e:
                        logger.error(f"Error al descargar {file}: {e}")
                logger.info("\nDescarga de archivos completada.")

        except Exception as e:
            logger.error(f"\nError durante la descarga de archivos: {e}")
        finally:
            n_sftp.disconnect()
            logger.info("\nDesconectado del servidor SFTP.")

    def load_dir_files(self, subdir=""):
        directory = os.path.join(f"{self.local_work_directory}{subdir}")
        return {
            file: {
                "success": False,
                "n_message": "",
                "process_again": False,
                "path": ""
            }
            for file in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, file)) and 'BOLLE' in file
        }

    def files_process(self, n_path):
        logger.info("\nIniciando el procesamiento de archivos...")
        bm = BmApi()
        encontrado_expediente = False
        mensaje = ""

        for file, info in self.files.items():
            file_path = os.path.join(n_path, file)
            info['path'] = file_path

            try:
                logger.info(f"\nProcesando archivo: {file_path}")

                # Proceso del archivo individual
                with open(file_path, 'rt') as archivo:
                    ipda = 0
                    cpda = ""
                    ba = BorderoArcese()

                    # Procesamos la fila
                    for fila in archivo:
                        # Si es cabecera
                        if fila[:2] == '01':
                            cab_partida = ba.cabecera_arcese(fila=fila)
                            trip = ba.expediente_ref_cor()  # 2024MO12103

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
                                pda_json_arc = self.partida_json(cabecera=cab_partida, expediente=expediente_bm)

                                nref_value = n_ref(cab_partida["Order Full Number"].strip())
                                query_existe = f"SELECT ipda, cpda FROM trapda WHERE nrefcor='{nref_value}'"

                                # Buscamos la partida
                                query_existe_reply = bm.n_consulta(query=query_existe)

                                # Si no existe la partidda, enchufamos partida
                                if not query_existe_reply["contenido"]:
                                    resp_partida = bm.post_partida(data_json=pda_json_arc)

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
                            n_linea = ba.linea_arcese(fila)

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
                        ba.genera_json_bordero(path=f"{self.local_work_directory}{self.local_work_processed}/Bordero"
                                                f"_{trip}{file}")

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

            # Movemos a las carpetas según el resultado del proceso
            if info["process_again"]:
                os.rename(info["path"], f"{self.local_work_directory}{self.local_work_pof_process}/{file}")
            else:
                os.rename(info["path"], f"{self.local_work_directory}{self.local_work_processed}/{file}")


        ...

        # TODO Preguntas:
        # ¿LOCAL. Qué hacemos con los ficheros procesados con éxito?
        # ¿FTP. Qué hacemos con los ficheros descargado correctamente?

    def run(self):

        # Nos ocupamos primero de los descargados antes y sin iexp/cexp
        logger.info("\nIniciando proceso de archivos que estaban pendientes de procesar")
        pda_arc = PartidaArcAne()
        pda_arc.files = self.load_dir_files(self.local_work_pof_process)
        if pda_arc.files:
            pda_arc.files_process(f"{self.local_work_directory}{self.local_work_pof_process}")
            logging.info("Procesados los archivos que estaban pendientes de procesar")

        # Procesamos los recién descargados del FTP
        logger.info("\nIniciando proceso de archivos descargados")
        logger.info("Descargando nuevos archivos")
        pda_arc.download_files()
        # pda_arc.files = None
        pda_arc.files = self.load_dir_files()
        if pda_arc.files:
            pda_arc.files_process(f"{self.local_work_directory}")
            logging.info("Procesados los archivos descargados de FTP")


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