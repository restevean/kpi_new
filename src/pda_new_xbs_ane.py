# src/pda_new_xbs_ane.py

import logging
from send_email import EmailSender
from utils.logger_config import setup_logger
from pathlib import Path
from dotenv import load_dotenv
import os
# from utils.bordero_xbs import BorderoXBS
from utils.bmaster_api import BmasterApi as BmApi
from utils.ftp_connect import FtpConnection
from typing import Dict, Any
from utils.bor_XBS import BorXBT as BorXBS
import json
from jsonmerge import merge
from utils.buscar_empresa import busca_destinatario

setup_logger()
logger = logging.getLogger(__name__)

base_dir = Path(__file__).resolve().parent

load_dotenv(dotenv_path=base_dir.parent / "conf" / "env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=base_dir.parent / "conf" / f".env.{INTEGRATION_CUST}{ENTORNO}")
logger.info(f"Cargando configuración: {INTEGRATION_CUST}{ENTORNO}")


class PdaNewXbsAne:
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
        self.bx = BorXBS()
        self.bm = BmApi()
        self.enc_exp = False
        self.partida = {}
        self.barcodes = []

    @staticmethod
    def expediente_ordenante(cabecera_=None):

        if cabecera_ is None:
            cabecera_ = {}

        empresa = busca_destinatario(
            rsocial=cabecera_["Sender Company Name"].strip(),
            codpostal=cabecera_["Sender ZipCode"].strip(),
            cpais=cabecera_["Sender Country Code"].strip()
        )

        logging.info(empresa)

        if empresa["ient"] > 0:
            return {"expedidor": {"id": empresa["ient"], }}
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
    def expediente_destinatario(cabecera_=None):

        if cabecera_ is None:
            cabecera_ = {}

        empresa = busca_destinatario(
            rsocial=cabecera_["Destination Company Name"].strip(),
            codpostal=cabecera_["Destination ZipCode"].strip(),
            cpais=cabecera_["Destination Country Code"].strip()
        )

        logging.info(empresa)

        if empresa["ient"] > 0:
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

    # def load_dir_files(self, subdir=''):
    def load_dir_files(self, subdir: Path = None) -> Dict[str, Dict[str, Any]]:
        # subdir = self.local_work_directory if subdir == '' else subdir
        subdir = self.local_work_directory if subdir is None else subdir
        subdir = Path(subdir)
        filtered_files = {
            file: {
                "success": False,
                "n_message": "",
                "process_again": False,
                "path": ""
            }
            for file in os.listdir(subdir)
            if os.path.isfile(os.path.join(subdir, file))
               and 'BORD512' in file
               and not file.endswith('.json')
        }
        return filtered_files

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
            archivos_filtrados = [archivo for archivo in archivos_remotos if "BORD512" in archivo]

            if not archivos_filtrados:
                logger.info(" --- No se encontraron para descargar.")
            else:
                for file in archivos_filtrados:
                    local_path = os.path.join(self.local_work_directory, file)
                    logger.info(" --- Descargando nuevos archivos")

                    try:
                        n_ftp.download_file(file, local_path)
                        logger.info(f" --- Descargando {file} a {local_path}")
                        # Hay que eliminar el archivo descargado del remoto
                        # n_ftp.ftp.delete(file)
                        # logger.info(f" --- Eliminando {file} del servidor FTP")
                    except Exception as e:
                        logger.error(f" --- Error al descargar {file}: {e}")
                logger.info(" --- Descarga de archivos completada")

        except Exception as e:
            logger.error(f" --- Error durante la descarga de archivos: {e}")
        finally:
            n_ftp.disconnect()
            logger.info(" --- Desconectado del servidor FTP")


    def files_process(self, n_path: Path):
        logger.info(" --- Iniciando el procesamiento de archivos...")

        for file, info in self.files.items():
            file_path = Path(n_path) / file
            info['path'] = file_path
            trip = ""
            mensaje = ""

            # n_ref_cor = ""
            self.enc_exp = False

            # try:
            logger.info(f" --- Procesando archivo: {file_path}")
            bor = BorXBS()

            # with open(base_dir / "bor_xbs.txt", "r", encoding="utf-8") as archivo:
            with open(self.local_work_directory / file, "r", encoding="utf-8") as archivo:
                for linea in archivo:
                    if linea[:3] == "A00":
                        trip = linea[9:44].strip()
                    bor.procesar_linea(linea)
                bor.exportar_json(self.local_work_directory / f"{file}.json")


            # Buscamos el expediente
            query = f"select * from traexp where nrefcor in ('{trip}')"
            query_reply = self.bm.n_consulta(query=query)

            # Si lo encontramos
            if query_reply["contenido"]:

                # Hemos encontrado el expediente
                self.enc_exp = True
                expediente_bm = query_reply["contenido"][0]
                mensaje += f"Expediente {trip} encontrado {expediente_bm['cexp']}"
                logging.info(f" --- Hemos encontrado el expediente {trip} con ref. Bmaster {expediente_bm['cexp']}"
                             f"cexp: {expediente_bm['cexp']}/iexp: {expediente_bm['iexp']}")

                # Nos creamos un diccionario con los datos que nos interesa (transform)
                data_to_load = self.transform_results_to_dict(self.local_work_directory / f"{file}.json",
                                                              expediente_bm)

                # Cargamos los datos transformados

                # Recorrer el diccionario 'data'
                for partida in data_to_load.get("partidas", []):

                    # Extraer el consignment_number_sending_depot
                    ref_cor = partida.get("refcorresponsal", "")

                    # Llamar a partida_add() con el consignment_number_sending_depot
                    partida_added = self.partida_load(ref_cor, partida, info)
                    mensaje += partida_added[1]
                    ipda = partida_added[2]
                    cpda = partida_added[3]
                    barcodes = data_to_load.get("barcodes", [])

                    if partida_added[0]:
                        # Recorrer la lista de barcodes
                        barcodes = data_to_load.get("barcodes", [])
                        # for barcode in barcodes:
                        for barcode in (b for b in barcodes if b.get("sequential_waybill_item") == ref_cor):
                            sequential_waybill_item = barcode.get("sequential_waybill_item", "")
                            consignment_position = barcode.get("consignment_position", "")
                            code = barcode.get("barcode", "")

                            # Llamar a barcode_add() con la información de los barcodes de la partida
                            mensaje += self.barcode_load(ref_cor, barcode, ipda, cpda, info)

            else:
                logger.info(f"Expediente {trip} no encontrado")
                mensaje += f"Expediente {trip} no encontrado\n"
                info["process_again"] = True

            info["success"] = True
            info["n_message"] = mensaje
            logger.info(f"Archivo procesado exitosamente {file}.")
            # except Exception as e:
            #     info["success"] = False
            #     mensaje += str(e)
            #     info["n_message"] = mensaje
            #     logger.error(f"\nError al procesar {file}: {e}\n")
            # finally:
            #     logger.info("Procesamiento de archivo completado.")

        # Movemos los archivos a processed o process_pending según corresponda
        # Enviamos los correos
        for file, info in self.files.items():
            # email_sender.send_email("javier@kpianalisis.com", self.email_to, f"Partida: {cpda}", info["message"])
            # Movemos a las carpetas según el resultado del proceso
            if info["process_again"]:
                # os.rename(info["path"], f"{self.local_work_pof_process / file}")
                # os.rename(self.local_work_processed / f"{file}_Bordero{trip}.json",
                #           self.local_work_pof_process /
                #           f"{file}_Bordero{trip}.json")
                ...
            else:
                # os.rename(info["path"], f"{self.local_work_processed / file}")
                ...

    def partida_load(self, consignment_number, partida, info):

        success = False
        mensaje = ""
        ipda = ""
        cpda = ""

        # Buscamos la partida
        query_existe_partida = f"SELECT ipda, cpda FROM trapda WHERE nrefcor='{consignment_number}'"
        query_reply_existe_partida = self.bm.n_consulta(query=query_existe_partida)

        if not query_reply_existe_partida["contenido"]:
            logging.debug(f" --- Hemos de comunicar la partida {consignment_number}")
            resp_partida = self.bm.post_partida(data_json=partida)

            # Si exito al comunicar la partida
            if resp_partida["status_code"] == 201:
                ipda = resp_partida["contenido"]["id"]
                cpda = resp_partida["contenido"]["codigo"]
                mensaje += f"\nCreada partida {cpda}/{ipda}"
                logging.debug(f" --- Creada partida {cpda}/{ipda}")
            else:
                mensaje += f"\nError al comunicar la partida {consignment_number}"
                logging.error(f" --- Error al comunicar la partida {consignment_number}")
                success = False
        else:
            ipda = query_reply_existe_partida["contenido"][0]["ipda"]
            cpda = query_reply_existe_partida["contenido"][0]["cpda"]
            mensaje += f"\nLa partida {cpda}/{ipda} ya existe"
            logging.debug(f" --- Partida {cpda}/{ipda} ya existe")
            success = True

        return success, mensaje, ipda, cpda


    def barcode_load(self, ref_cor, barcode, ipda, cpda, info):

        # Verificamos que la partida no tiene bultos
        logging.info(f" --- Barcode: {barcode['Barcode']}")
        # n_row = self.bx.linea_xbs(row)
        mensaje = ""

        # Si tenemos nº ipda (partida) y barcode (algún bulto)
        if ipda > 0 and barcode["Barcode"].strip():

            # Comprobamos que no existe rl barcode
            query_barcode = query = (f"SELECT COUNT(1) AS cuenta FROM ttemereti WHERE ipda={ipda} "
                                     f"AND dcodbar='{barcode['Barcode'].strip()}'")
            query_barcode_reply = self.bm.n_consulta(query=query_barcode)

            # Si no existe
            if not query_barcode_reply:
                json_etiqueta = {
                    "codigobarras": barcode["Barcode"].strip(),
                    "altura": float(barcode["Hight"].strip()) / 100,
                    "ancho": float(barcode["Width"].strip()) / 100,
                    "largo": float(barcode["Lenght"].strip()) / 100,
                }
                # Cargamos el barcode
                resp_etiqueta = self.bm.post_partida_etiqueta(paet_id=ipda, data_json=json_etiqueta)

                # Actualizamos el mensaje en función del resultado
                if resp_etiqueta["cod_error"] == 201:
                    mensaje += f"\nSubida Etiqueta. {barcode['Barcode'].strip()}"
                else:
                    mensaje += (f"\nYa existe la etiqueta {barcode['Barcode'].strip()} "
                                f"de la "
                                f"partida {cpda}")
            # Si existe el bulto
            else:
                mensaje += f"\nSe encontró el bulto {barcode['Barcode'].strip()}"
                logging.info("Bulto encontrado")

        else:
            logging.info(f"No es cabecera, cpda: {cpda}, ipda: {ipda}, barcode: {barcode['Barcode'].strip()}")

        return mensaje





    def transform_results_to_dict(self, json_path: Path, expediente: dict) -> Dict[str, Any]:
        with Path(json_path).open("r", encoding="utf-8") as f:
            data = json.load(f)

        partidas_list = []
        barcodes_list = []

        for partida in data.get("partidas", []):

            # Obtenemos los registros que contienen los datos necesarios
            b00_shp = partida.get("B00-SHP", {})
            b00_con = partida.get("B00-CON", {})
            g00 = partida.get("G00", {})

            shipper = {
                "Sender Company Name": b00_shp.get("name_1", ""),
                "Sender Address": b00_shp.get("street_and_street_number", ""),
                "Sender ZipCode": b00_shp.get("postcode", ""),
                "Sender Place": b00_shp.get("place", ""),
                "Sender Country Code": b00_shp.get("country_code", "")

            }
            consignee = {
                "Destination Company Name": b00_con.get("name_1", ""),
                "Destination Address": b00_con.get("street_and_street_number", ""),
                "Destination ZipCode": b00_con.get("postcode", ""),
                "Destination Place": b00_con.get("place", ""),
                "Destination Country Code": b00_con.get("country_code", "")}

            shipper_json = self.expediente_ordenante(cabecera_=shipper)
            consignee_json = self.expediente_destinatario(cabecera_=consignee)

            # Extraemos los datos de G00
            consignment_number_sending_depot = g00.get("consignment_number_sending_depot", "").strip()
            total_gross_weight_str = g00.get("actual_consignment_gross_weight_in_kg", "0").strip()
            total_volume_str = g00.get("cubic_meters", "0").strip()
            insured_amount_str = g00.get("goods_value", "0").strip()

            total_gross_weight = float(total_gross_weight_str) / 100
            total_volume = float(total_volume_str) / 100
            insured_amount = float(insured_amount_str) / 100

            # Obtenemos de D00 (num_err_of/número bultos) de la partida
            bultos = partida.get("bultos", [])
            d00_num_err_of = None
            if bultos:
                primer_bulto = bultos[0]
                d00 = primer_bulto.get("D00", {})
                d00_num_err_of = d00.get("num_err_of", "").strip()

            # Construir la partida
            partida_dict = {
                "division": "VLC",
                "estado": "FBLE",
                "expediente": expediente["cexp"],
                "trafico": "TI",
                "tipotrafico": "TER",
                "flujo": "I",
                "refcliente": expediente["ientrefcli"],
                "refcorresponsal": consignment_number_sending_depot,
                "tipocarga": "C",
                "puertoorigen": "TIRARCCAM",
                "puertodestino": "TIRANERIB",
                "paisorigen": b00_shp.get("country_code", "").strip(),
                "paisdestino": b00_con.get("country_code", "").strip(),
                "portes": "s",
                "incoterm": "XXX",
                "servicio": "NOR",
                "bultos": d00_num_err_of,
                "tipobultos": "PX",
                "pesobruto": total_gross_weight,
                "pesoneto": total_gross_weight,
                "pesotasable": total_gross_weight,
                "volumen": total_volume,
                "seguro": "S",
                "valorasegurable": insured_amount,
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
                "fechaprevistasalida": expediente["fpresal"],
                "fechasalida": expediente["fsal"],
                "fechaprevistallegada": expediente["fprelle"],
                "fechallegada": expediente["flle"],
            }
            partidas_list.append(partida_dict)

            if shipper_json is not None:
                partidas_list[-1] = merge(partidas_list[-1], shipper_json)

            # TODO Falta el incoterm
            if partidas_list[-1]["incoterm"] == "DAP":
                json_fact = {
                    "clientefacturacion": {
                        "id": expediente["ientcor"]
                    }
                }
                partidas_list[-1] = merge(partidas_list[-1], json_fact)

            if consignee_json is not None:
                partidas_list[-1] = merge(partidas_list[-1], consignee_json)


            # Extraemos todos los códigos de barras (F00) de esta partida
            for bulto in bultos:
                barcodes_in_bulto = bulto.get("barcodes", [])
                for barcode_element in barcodes_in_bulto:
                    f00 = barcode_element.get("F00", {})
                    codigo_barras = f00.get("barcode", "").strip()
                    if codigo_barras:
                        barcodes_list.append({
                            "partida": consignment_number_sending_depot,
                            "codigobarras": codigo_barras,
                            "altura": 0.0,
                            "ancho": 0.0,
                            "largo": 0.0
                        })

        return {
            "partidas": partidas_list,
            "barcodes": barcodes_list
        }

    def run(self):

        # Nos ocupamos primero de los descargados antes y sin iexp/cexp
        logger.info("\nIniciando proceso de archivos que estaban pendientes de procesar")
        # self.files = self.load_dir_files(self.local_work_pof_process)
        # if self.files:
        #     # self.files_process(self.local_work_pof_process)
        #     logging.info("Procesados los archivos que estaban pendientes de procesar")
        #     self.files = None

        # Procesamos los recién descargados del FTP
        logger.info("\nIniciando proceso de archivos descargados")
        # self.download_files()
        self.files = self.load_dir_files()
        if self.files:
            self.files_process(self.local_work_directory)
            logging.info("Procesados los archivos descargados de FTP")


if __name__ == "__main__":
    pda_new_xbs_ane = PdaNewXbsAne()
    pda_new_xbs_ane.run()