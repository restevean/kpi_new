# utils/sftp_connect.py

import os
import paramiko
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

class SftpConnection:
    def __init__(self):
        self.host = os.getenv("SFTP_SERVER")
        self.username = os.getenv("SFTP_USER")
        self.password = os.getenv("SFTP_PW")
        self.port = int(os.getenv("SFTP_PORT"))
        self.sftp = None
        self.transport = None

    def connect(self, host=SFTP_SERVER, port=SFTP_PORT, username=SFTP_USER, password=SFTP_PW):
        self.transport = paramiko.Transport((host, int(port)))
        self.transport.connect(username=username, password=password)  # Conectar usando la contraseña
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        print("Conexión establecida")

    def disconnect(self):
        self.sftp.close()
        self.transport.close()
        print("Conexión cerrada")
