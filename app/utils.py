# app/utils.py

import os
import logging
from starlette.templating import Jinja2Templates
from datetime import datetime

## Configuración de plantillas
templates = Jinja2Templates(directory="app/templates")

# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Configurado a DEBUG para capturar todos los niveles de logs

# Agregar un manejador de logs a la consola si aún no lo tienes
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)


def ensure_logs_directory():
    """
    Asegura que el directorio 'logs' exista.
    """
    logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'logs')
    os.makedirs(logs_dir, exist_ok=True)


def write_log(task_name: str, start_datetime: str, end_time: str, status: str):
    """
    Escribe una entrada en el archivo de log diario.
    """
    try:
        # Obtener la fecha actual en formato YYMMDD
        date_str = datetime.now().strftime('%y%m%d')
        log_filename = f"app_scheduler_{date_str}.txt"
        logs_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')

        # Asegurarse de que el directorio de logs exista
        os.makedirs(logs_dir, exist_ok=True)
        logger.debug(f"Directorio de logs verificado: {logs_dir}")

        log_filepath = os.path.join(logs_dir, log_filename)
        logger.debug(f"Ruta completa del archivo de log: {log_filepath}")

        # Formatear la entrada de log
        log_entry = f"{task_name}, {start_datetime}, {end_time}, {status}\n"
        logger.debug(f"Entrada de log formateada: {log_entry.strip()}")

        # Escribir la entrada en el archivo de log
        with open(log_filepath, 'a', encoding='utf-8') as log_file:
            log_file.write(log_entry)
            logger.debug(f"Entrada de log escrita exitosamente en {log_filepath}")
    except Exception as e:
        logger.error(f"Error al escribir en el log: {e}")
