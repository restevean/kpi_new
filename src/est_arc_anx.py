import os
from dotenv import load_dotenv
from utils.fortras_stat import MensajeEstado as Fot
from utils.bmaster_api import BmasterApi as BmApi
from utils.sftp_connect import SftpConnection as py_Sftp
from utils.send_email import EmailSender as email
from datetime import datetime 
class Estado_corresponsal_Anexa:
    def __init__(self):
        load_dotenv(dotenv_path="../conf/.env.base")
        self.entorno = os.getenv("ENTORNO")
        self.host = os.getenv("SFTP_SERVER")
        self.username = os.getenv("SFTP_USER")
        self.password = os.getenv("SFTP_PW")
        self.port = int(os.getenv("SFTP_PORT"))
        self.local_work_directory = "../fixtures"
        self.sftp_stat_in_dir = os.getenv("SFTP_STAT_IN_DIR")
        self.sftp_dev_stat_in_dir = os.getenv("SFTP_DEV_STAT_IN_DIR")
        self.sftp_stat_out_dir = os.getenv("SFTP_STAT_OUT_DIR")
        self.sftp_dev_stat_out_dir = os.getenv("SFTP_DEV_STAT_OUT_DIR")
        self.sftp_remote_dir = self.sftp_stat_out_dir if self.entorno=="prod" else self.sftp_dev_stat_out_dir
        self.remote_work_out_directory = self.sftp_stat_out_dir if self.entorno == "prod" else self.sftp_dev_stat_out_dir
        self.remote_work_in_directory = self.sftp_stat_in_dir if self.entorno == "prod" else self.sftp_dev_stat_in_dir
        self.bm = BmApi()
    def run (self):
        
        sftp= py_Sftp(username=self.username, password=self.password, hostname=self.host, port=self.port)
        sftp.connect()
        lista_ficheros = sftp.listdir(remote_path=self.remote_work_out_directory)
        
        enviar_email=False
        mensaje=""
        for fichero in lista_ficheros:
            if(fichero[:6]=="ESITI"):
                enviar_email=True
                
                ruta_descarga = self.local_work_directory+fichero
                stat = Fot()
                sftp.download(remote_path=self.sftp_remote_dir+"/"+fichero, target_local_path=ruta_descarga)
                estados=stat.leer_stat_arcese(fichero=ruta_descarga)
                
                for est in estados:
                    est_anexa = stat.conversion_stat_arcese_anexa(est.get("EventCode").strp())
                    cpda = est.get('Customer Reference').strip()
                    fecha=datetime()
                    subir_hito=False #lo uso para controlar si lo subimso o enviamos un mensaje de error según vamos haciendo validaciones
                    if est.get("Event Date").strp() =="":
                        mensaje+=f"No se ha incluido fecha de evento para la partida {cpda}\n"
                    else:
                        fecha = datetime.strptime(est.get("Event Date").strp()+est.get("Event Time").strp(), "%Y%m%d%H:%M:%S")
                        subir_hito=True
                    if est_anexa[:5]=="Error":
                        mensaje+=f"\nEstado de Arcese no conocido {est.get("EventCode").strp()} para la partida {cpda}"
                        subir_hito=False
                    else:
                        if subir_hito:
                            resp_partida = BmApi.consulta_(query=f"select ipda, ientcor from trapda where cpda='{cpda}'")
                            if len(resp_partida.get("contenido"))>0:
                                if resp_partida["contenido"][0]["ientcor"] in [89322,96202,77904,84019,84054,88931,185744,244692,244788,244799,246489,251061,251265,252995,252997]:
                                    #comprobamos que el corresponsal es Arcese ... sino por error, podría otro corresponsal por la TEP o referir a otro corresposnal. No todas las TEP son correctas
                                    dia=est.get("Event Date").strp()
                                    hora = est.get("Event Time").strp()
                                    hito_json ={
                                            "codigohito": est_anexa,
                                            "descripciontracking": est.get("Event Description").strp(),
                                            "fechatracking": fecha.strftime("%Y-%m-%dT%H:%M:00.000Z")  #"2024-11-30T12:43:55.101Z"
                                            }
                                    resp_hito = BmApi.post_partida_tracking(ipda=resp_partida["contenido"][0]["ipda"], data_json=hito_json)
                                    if resp_hito.get("cod_error")==201:
                                        mensaje+=f"Hito {est_anexa} -- {est.get("Event Description").strp()} para la partida {cpda} subido.\n"
                                else:
                                    mensaje+= f"La partida {cpda} no corresponde a ARcese con el ientcor {str(resp_partida["contenido"][0]["ientcor"])}"
                            else:
                                mensaje += f"\n Estado Arcese para la partida {cpda} que no está en BM."
            if enviar_email:
                print("enviamso email")
                            
                 