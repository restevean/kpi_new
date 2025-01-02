# src/exp_ane_xbs.py

import os
import sys
from dotenv import load_dotenv
from utils.bmaster_api import BmasterApi as BmApi
from utils.send_email import EmailSender as Email
import logging
from datetime import datetime, timezone
from pathlib import Path
import paramiko


# Activamos logging
logging.basicConfig(
    level=logging.INFO,     # Nivel mínimo de mensajes a mostrar
    format='%(message)s',   # Formato del mensaje
    # format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje
)
logger = logging.getLogger(__name__)
base_dir = Path(__file__).resolve().parent
load_dotenv(dotenv_path=base_dir.parent / "conf" / ".env.base")

ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
EMAIL_OURS = os.getenv("EMAIL_OURS")
load_dotenv(dotenv_path=base_dir.parent / "conf" /f".env.{INTEGRATION_CUST}{ENTORNO}")
EMAIL_TO_XBS = os.getenv("EMAIL_TO_XBS")


class ExpteAneXbs:
    def __init__(self):
        self.bm = BmApi()
        self.ftp_server_xbs = os.getenv("FTP_SERVER_XBS")
        self.ftp_user_xbs = os.getenv("FTP_USER_XBS")
        self.ftp_pw_xbs = os.getenv("FTP_PW_XBS")
        self.ftp_port_xbs = os.getenv("FTP_PORT_XBS")
        self.ftp_pda_path_xbs = os.getenv("FTP_PDA_PATH_XBS")
        self.ftp_in_path_xbs = os.getenv("FTP_IN_PATH_XBS")
        self.ftp_out_path_xbs = os.getenv("FTP_OUT_PATH_XBS")
        self.sftp_server_ane = os.getenv("FTP_SERVER_ANE")
        self.sftp_user_ane = os.getenv("SFTP_USER_ANE")
        self.sftp_pw_ane = os.getenv("SFTP_PW_ANE")
        self.sftp_port_ane = os.getenv("SFTP_PORT_ANE")
        self.sftp_json_path_ane = os.getenv("SFTP_JSON_PATH_ANE")
        self.local_work_dir = base_dir.parent / "fixtures" / "xbs" / "edi"
        self.email_from = "Expediente Anexa-XBS" if ENTORNO == "pro" else "(TEST) Expediente Anexa-XBS"
        # TODO En caso de que sea ENTORNO == "pro", suponemos que además recibe correo "trafico6@anexalogistica.com"
        self.email_to = [EMAIL_OURS, EMAIL_TO_XBS]
        self.email_to += ["trafico6@anexalogistica.com"] if ENTORNO == "pro" else []
        self.smtp_server = os.getenv("SMTP_SERVER")
        self.smtp_port = os.getenv("SMTP_PORT")
        self.smtp_username = os.getenv("SMTP_USERNAME")
        self.smtp_pw = os.getenv("SMTP_PW")
        self.email_subject = "Expediente Anexa-XBS"
        self.email = Email(self.smtp_server, self.smtp_port, self.smtp_username, self.smtp_pw)


    def transform(self, query_reply):
        # Creamos el archivo json en la carpeta local /fixtures/xbs/edi
        # Creamos el archivo edi en la carpeta local /fixtures/xbs/edi
        ...


    def load_results(self):
        # Guardamos el json en el SFTP de ane
        # Guiardamos el edi en el FTP de xbs
        ...


    def send_emails(self):
        self.email.send_email(
            from_addr=self.email_from,
            to_addrs=self.email_to,
            subject=self.email_subject,
            body="Este es un correo de prueba."
        )
        ...


    def run(self):
        query = """
            SELECT 
                hitos.ihit,
                [iexp],
                [cexp],
                exp.[idvn],
                [itra],
                [fsal],
                [flle],
                [fcie],
                [ipaiori],
                [ipaides],
                [ipueori],
                [ipuedes],
                [nmaw],
                [icia],
                [ipry],
                [dmedtra],
                [dalm],
                [tneu],
                [tfle],
                [ientcor],
                [ientcol],
                [ienttra],
                exp.[cusualt],
                exp.[faltrto],
                exp.[cusumod],
                exp.[fmod],
                exp.[hmod],
                [imedtra],
                [idivfle],
                [vcamfle],
                [ientnav],
                [ipdatip],
                [nrefcor],
                [nref],
                [itrnest],
                [imedtralin],
                [vcstkildeb],
                [vkil],
                [vmaw],
                [ktasmaw],
                [vcstmaw],
                [iser],
                [fvue],
                [tgtc],
                [ilocmer],
                [tdescon],
                [nbkgpda],
                [ipdaedihis],
                [ipaimedtra],
                [fllnctn],
                [hllnctn],
                [tcol],
                [iexpcol],
                [ientcol2],
                [dpueoriexp],
                [dpuedesexp],
                [fpresal],
                [fprelle],
                [ndiatra],
                [ientrefcli],
                [ientord],
                [fclodat],
                [ttrabdo],
                [ienthan],
                [icodposori],
                [dpobori],
                [icodposdes],
                [dpobdes],
                [npre],
                [npresec],
                [imedtratra],
                [imedtrabuq],
                [ipercdt],
                [ntelcdt],
                [temb],
                [dnomcomcor2],
                [tgesdocadjedp],
                [inumcon]
            FROM 
                traexp exp
            INNER JOIN 
                (
                    SELECT 
                        hit.*, 
                        hit2.ihit 
                    FROM 
                        (
                            SELECT 
                                MAX(itrk) AS itrk,
                                creg 
                            FROM 
                                [dbo].[aebtrk]
                            WHERE 
                                dtab = 'traexp' 
                                AND ihit IN (627, 628)
                            GROUP BY 
                                creg
                        ) hit
                    INNER JOIN 
                        [dbo].[aebtrk] hit2 
                        ON hit2.itrk = hit.itrk
                ) hitos 
                ON hitos.creg = exp.iexp
            WHERE 
                hitos.ihit = 627 
                AND exp.ientcor IN (
                    248320, 249456
                ) 
                AND exp.cexp LIKE 'TE%'
            ORDER BY 
                iexp DESC
            """

        query_reply = self.bm.n_consulta(query)
        if query_reply["contenido"]:
            self.transform(query_reply)
            self.load_results()
            self.send_emails()


if __name__ == "__main__":
    exp = ExpteAneXbs()
    exp.run()
