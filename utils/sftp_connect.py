import os
import paramiko
from dotenv import load_dotenv


load_dotenv(dotenv_path="../conf/.env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=f"../conf/.env.'+{INTEGRATION_CUST}")
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
        # self.local_work_directory = "../fixtures"
        # self.remote_work_directory = "/IN/STAT_TEST"
        self.sftp = None
        self.transport = None

    def connect(self):
        self.transport = paramiko.Transport((self.host, self.port))
        self.transport.connect(username=self.username, password=self.password)  # Conectar usando la contraseña
        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        print("Conexión establecida")

    def disconnect(self):
        self.sftp.close()
        self.transport.close()
        print("Conexión cerrada")