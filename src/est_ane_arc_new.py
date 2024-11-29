# src/est_ane_arc_new.py

import os
from dotenv import load_dotenv
from utils.bmaster_api import BmasterApi as BmApi


load_dotenv(dotenv_path="../conf/.env.base")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=f"../conf/.env.{INTEGRATION_CUST}")
ENTORNO = os.getenv("ENTORNO")


class EstadoAmeArc:

    def __init__(self):
        self.host = os.getenv("SFTP_SERVER_ARC")
        self.username = os.getenv("SFTP_USER_ARC")
        self.password = os.getenv("SFTP_PW_ARC")
        self.port = int(os.getenv("SFTP_PORT_ARC"))
        self.local_work_directory = "../fixtures"
        # TODO is self.sftp_dev_stat_out_dir necessary?
        self.sftp_dev_stat_out_dir = os.getenv("SFTP_DEV_STAT_OUT_DIR")
        self.remote_work_out_directory = os.getenv("SFTP_OUT_PATH_ARC")
        self.remote_work_in_directory = os.getenv("SFTP_IN_PATH_ARC")
        self.partidas = None
        self.max_itrk = None
        self.bm = BmApi()
