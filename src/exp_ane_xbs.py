# src/exp_ane_xbs.py

import os
import json
from dotenv import load_dotenv
from utils.bmaster_api import BmasterApi as BmApi
from utils.send_email import EmailSender as Email
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Any


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
        self.iexp = None

    @property
    def query_partidas(self):
        query = f"""
            SELECT 
                par.[ipda],
                [itra],
                [cpda],
                par.[iexp],
                par.[idvn],
                [tflu],
                [ipdatip],
                [tfle],
                [ipor],
                [itrnest],
                [ipueori],
                [ipuedes],
                [icmr],
                par.[faltrto],
                [iser],
                [ientexp],
                expe.dnomcom AS expe_dnomcom,
                emp_exp.cemp AS expe_cemp,
                dir_exp.ddir AS dir_exp_ddir,
                dir_exp.ddi2 AS dir_exp_ddi2,
                dir_exp.dpob AS dir_exp_dpob,
                cp_exp.ccodpos AS cp_exp_ccodpos,
                pro_exp.dpro AS pro_exp_dpro,
                pais_exp.cpai AS pais_exp_cpai,
                [ientdes],
                dest.dnomcom AS dest_dnomcom,
                emp_dest.cemp AS dest_cemp,
                dir_dest.ddir AS dir_dest_ddir,
                dir_dest.ddi2 AS dir_dest_ddi2,
                dir_dest.dpob AS dir_dest_dpob,
                cp_dest.ccodpos AS cp_dest_ccodpos,
                pro_dest.dpro AS pro_dest_dpro,
                pais_dest.cpai AS pais_dest_cpai,
                [ientadu],
                [ientcor],
                [ienttraint],
                [ienttraexj],
                [ienttranac],
                [ipaiori],
                [ipaides],
                [nord],
                [ibultipexo],
                [qbulexo],
                par.[kbru],
                [knet],
                par.[qvol],
                [ttraintexj],
                [ttraintnac],
                [frec],
                [fsal],
                par.[flle],
                par.[fprelle],
                [femb],
                [fembmax],
                [tpda],
                [nrefcli],
                [nrefcor],
                [qmetlin],
                [ktas],
                [ktascli],
                [ktascor],
                [qvolcor],
                [qmetlincor],
                [tdua],
                [idirtraintexj],
                [idirtraintnac],
                [inotemb],
                [itraexpdes],
                [ientord],
                par.[imedtra],
                medtra.cmedtra,
                [tgtc],
                [icodposdelfin],
                [tcoc],
                [tnofac],
                [kbrucor]
            FROM 
                [dbo].[trapda] par
            LEFT JOIN 
                aetent AS expe ON expe.ient = par.ientexp
            LEFT JOIN 
                aetemp AS emp_exp ON emp_exp.iemp = expe.iemp
            LEFT JOIN 
                aebdir AS dir_exp ON dir_exp.idir = emp_exp.idirfis
            LEFT JOIN 
                aebcodpos cp_exp ON cp_exp.icodpos = dir_exp.icodpos
            LEFT JOIN 
                aebpro pro_exp ON pro_exp.ipro = dir_exp.ipro
            LEFT JOIN 
                aebpai pais_exp ON pais_exp.ipai = dir_exp.ipai
            LEFT JOIN 
                aetent AS dest ON dest.ient = par.ientdes
            LEFT JOIN 
                aetemp AS emp_dest ON emp_dest.iemp = dest.iemp
            LEFT JOIN 
                aebdir AS dir_dest ON dir_dest.idir = emp_dest.idirfis
            LEFT JOIN 
                aebcodpos cp_dest ON cp_dest.icodpos = dir_dest.icodpos
            LEFT JOIN 
                aebpro pro_dest ON pro_dest.ipro = dir_dest.ipro
            LEFT JOIN 
                aebpai pais_dest ON pais_dest.ipai = dir_dest.ipai
            LEFT JOIN 
                [dbo].[tramedtra] medtra ON medtra.imedtra = par.imedtra
            WHERE 
                par.iexp = {self.iexp}
                AND [ientcor] IN (
                    248320, 249456
                )
                AND ttraintexj <> 'L'
            ORDER BY 
                iexp DESC
        """
        return query

    def extract(self) -> Dict[str, Any]:
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
        return query_reply


    def transform(self, query_reply: Dict[str, Any]) -> None:
        transformed = []
        for expediente in query_reply["contenido"]:
            self.iexp = expediente["iexp"]
            # n_corresponsal = expediente["ientcor"]
            # n_send_email = True
            # n_subject = f"Expdte. Anexa-XBS | Expediente {expediente["cexp"]}"
            # n_message = f"Expediente {expediente["cexp"]} \n"
            logging.info(f"Expediente: {expediente["cexp"]}")
            expediente["partidas"] = []
            partidas = self.bm.n_consulta(self.query_partidas)
            if partidas["contenido"]:
                for partida in partidas["contenido"]:
                    expediente["partidas"].append(partida)
                    ...
            transformed.append(expediente)

        # Creamos el archivo json en la carpeta local /fixtures/xbs/edi
        self.save_contenido_to_json(transformed, self.local_work_dir /
                                    f"TEXPDTES-{datetime.now().strftime("%Y%m%d-%H%M")}.json")

        # Creamos el edi en la carpeta local /fixtures/xbs/edi
        self.convert_to_edi(transformed, self.local_work_dir /
                                    f"TEXPDTES-{datetime.now().strftime("%Y%m%d-%H%M")}.json")
        ...

    @staticmethod
    def save_contenido_to_json(contents: List[Dict[str, Any]], file_path: Path) -> None:
        try:
            with open(file_path, 'w', encoding='utf-8') as json_file:
                json.dump(contents, json_file, ensure_ascii=False, indent=4)
            logging.info(f"Datos correctamente guardados en {file_path}")
        except IOError as e:
            logging.error(f"Error al escribir en el archivo JSON: {e}")
            raise

    def convert_to_edi(self, expedientes: List[Dict[str, Any]], file_path: Path) -> None:
        # Convertimos el json a edi
        ...


    def load_results(self) -> None:
        # Cargamos el edi en el FTP de xbs
        ...


    def send_emails(self) -> None:
        self.email.send_email(
            from_addr=self.email_from,
            to_addrs=self.email_to,
            subject=self.email_subject,
            body="Este es un correo de prueba."
        )
        ...


    def run(self) -> None:
        expedientes = self.extract()
        if expedientes["contenido"]:
            self.transform(expedientes)
            self.load_results()
            self.send_emails()


if __name__ == "__main__":
    exp = ExpteAneXbs()
    exp.run()
