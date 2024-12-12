# utils/sftp_connect.py

import paramiko
import logging

logger = logging.getLogger(__name__)


"""
from dotenv import load_dotenv
from pathlib import Path


base_dir = Path(__file__).resolve().parent

load_dotenv(dotenv_path=base_dir.parent / "conf" / ".env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=base_dir.parent / "conf" / f".env.{INTEGRATION_CUST}{ENTORNO}")
SFTP_SERVER = os.getenv("SFTP_SERVER")
SFTP_USER = os.getenv("SFTP_USER")
SFTP_PW = os.getenv("SFTP_PW")
SFTP_PORT = os.getenv("SFTP_PORT")
"""

class SftpConnection:
    def __init__(self, hostname, username, password, port=22):
        self.host = hostname
        self.username = username
        self.password = password
        self.port = port
        self.sftp = None
        self.transport = None

    def connect(self):
        self.transport = paramiko.Transport((self.host, int(self.port)))
        self.transport.connect(username=self.username, password=self.password)  # Conectar usando la contraseña
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        logging.info(f" --- Conexión SFTP establecida en {self.host}")

    def disconnect(self):
        self.sftp.close()
        self.transport.close()
        print(f" --- Conexión SFTP en {self.host} cerrada")
