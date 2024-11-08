import os
from utils.load_environment import load_environment

import paramiko

if os.getenv("ENTORNO") is None:
    load_environment()

class SftpConnection:
    def __init__(self):
        self.host = os.getenv("SFTP_SERVER")
        self.username = os.getenv("SFTP_USER")
        self.password = os.getenv("SFTP_PW")
        self.port = int(os.getenv("SFTP_PORT"))
        self.local_work_directory = "../fixtures"
        self.remote_work_directory = "/IN/STAT_TEST"
        self.sftp = None
        self.transport = None

    def connect(self):
        self.transport = paramiko.Transport((self.host, self.port))
        self.transport.connect(username=self.username, password=self.password)  # Conectar usando la contraseña

        self.sftp = paramiko.SFTPClient.from_transport(self.transport)
        print("Conexión establecida")

        if not os.path.exists(self.local_work_directory):
            os.makedirs(self.local_work_directory)

        self.sftp.chdir(self.remote_work_directory)

    def disconnect(self):
        self.sftp.close()
        self.transport.close()
        print("Conexión cerrada")
