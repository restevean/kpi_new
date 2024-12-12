# utils/ftp_connect.py

import ftplib
import logging


logger = logging.getLogger(__name__)


class FtpConnection:
    def __init__(self, host, username, password, port=21):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        self.ftp = None

    def connect(self):
        try:
            self.ftp = ftplib.FTP()
            self.ftp.connect(self.host, self.port)
            self.ftp.login(self.username, self.password)
            logging.debug(f" --- Conexión FTP establecida con {self.host}:{self.port}")
        except ftplib.all_errors as e:
            logging.debug(f" --- Error al conectar FTP: {e}")
            self.disconnect()

    def disconnect(self):
        if self.ftp:
            try:
                self.ftp.quit()
                logging.debug(" --- Conexión FTP cerrada")
            except ftplib.all_errors as e:
                logging.debug(f" --- Error al cerrar conexión FTP: {e}")

    def download_file(self, remote_path, local_path):
        if not self.ftp:
            logging.debug(" --- No hay conexión activa.")
            return
        try:
            with open(local_path, "wb") as file:
                self.ftp.retrbinary(f"RETR {remote_path}", file.write)
                logging.info(f" --- Archivo descargado: {remote_path} -> {local_path}")
        except ftplib.all_errors as e:
            logging.debug(f" --- Error al descargar el archivo FTP: {e}")

    def change_directory(self, directory):
        if not self.ftp:
            logging.debug(" --- No hay conexión activa.")
            return
        try:
            self.ftp.cwd(directory)
            logging.debug(f" --- Cambiado al directorio: {directory}")
        except ftplib.all_errors as e:
            logging.debug(f" --- Error al cambiar al directorio {directory}: {e}")
