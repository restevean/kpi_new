import datetime
import logging
import os
from dotenv import load_dotenv
from pathlib import Path
from utils.logger_config import setup_logger
from utils.send_email import EmailSender as email
from utils.bmaster_api import BmasterApi as BmApi

# Activamos logging
setup_logger()
logger = logging.getLogger(__name__)

# Gestionamos adecuadamente los path
base_dir = Path(__file__).resolve().parent

load_dotenv(dotenv_path=base_dir.parent / "conf" / ".env.base")
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=base_dir.parent / "conf" / f".env.{INTEGRATION_CUST}{ENTORNO}")

logger.info(f"Cargada configuración: {INTEGRATION_CUST}{ENTORNO}")

class maritimo_expedientes:
    def __init__(self):
        self.bm = BmApi()
        self.resumen=""
        self.asunto="RESUMEN VESSEL | AIRPLANE  - NEW | MODIFICATIONS"
        self.to_ = ["aortiz@anexalogistica.com", "javier@kpianalisis.com"]
        if ENTORNO=="dev":
            self.resumen = "TEST -"
            self.asunto ="TEST -" +self.asunto
    def anadir_resumen (self, res):
        self.resumen+=res
    def get_resumen(self):
        return self.resumen
    def get_asunto(self):
        return self.asunto
    def get_to_(self):
        return self.to_

    def expedientes(self):
        query = """
        select traexp.iexp, traexp.cexp, traexp.flle, traexp.fsal, traexp.dpobori, traexp.dpobdes, traexp.ientord 
                from traexp 
                inner join aebtrk on aebtrk.creg = traexp.iexp and aebtrk.dtab='traexp' 
                inner join aebtra on aebtra.itra = traexp.itra 
                inner join ( 
                    select HITOS.*, aebtrk.ihit 
                    from ( 
                        select max(itrk) MAXITRK, creg 
                            from aebtrk  
                            where ihit in (607,606,608)  and dtab = 'traexp' 
                            group by creg 
                     ) HITOS 

               left join aebtrk on aebtrk.itrk = HITOS.MAXITRK 
               ) HITOS2 on HITOS2.creg =  traexp.iexp 

                where  
                    traexp.imedtra IS NOT NULL 
                     and left(cexp,2) in ('MI','ME','AI','AE') 
                    and aebtrk.ihit in (606,608,607) 
                    and HITOS2.ihit in (607,608) 
                group by traexp.iexp, traexp.cexp, traexp.flle, traexp.fsal, traexp.dpobori, traexp.dpobdes, traexp.ientord
            """
        resp_expedientes = self.bm.n_consulta(query=query)
        return resp_expedientes["contenido"]

class maritimo ():
    def __init__(self, exp={}):
        self.resumen=f'\nExpediente {exp["cexp"]} '
        self.ordenantes = []
        self.bm = BmApi()
        self.iexp=exp["iexp"]
        self.cexp=exp["cexp"]
        self.ientord_exp = exp["ientord"]
        self.dpobdes =  exp["dpobdes"]
        self.dpobori = exp["dpobori"]
        self.flleexp =exp["flle"]
        self.fsalexp = exp["fsal"]
        self.asunto=''
        self.mensaje_actualizacion=''
        self.mensaje_nuevo_buque=''
        self.mensaje_contenedores=''
        self.mensaje_hitos=''
        self.nuevo_buque_bool=False
        self.modificacion_fecha_bool=False
        self.mensaje_email_enviar=''
        self.enviar_mensaje = False

        self.HITOS606_MAXITRK= 0
        self.HITOS607_MAXITRK= 0
        self.HITOS608_MAXITRK= 0
        self.HITOS606_count= 0
        self.HITOS607_count= 0
        self.HITOS608_count= 0

        self.num_contenedores = 0
        self.tiposnct =""
        self.qbul=0
        self.kbru=0.
        self.qvol=0.

    def set_asunto (self,asunto=""):
        if ENTORNO =="dev":
            self.asunto="TEST " + asunto
        else:
            self.asunto = asunto
    def get_asunto (self):
        return self.asunto

    def get_resumen(self):
        return self.resumen

    def set_to_list (self, lista=[]):
        if lista is None:
            to_list = ["aortiz@anexalogistica.com", "javier@kpianalisis.com"]
            return to_list
        if ENTORNO == "dev":
            to_list= ["aortiz@anexalogistica.com", "javier@kpianalisis.com"]
            return to_list
        else:
            to_list = lista
            return to_list

    def nuevo_buque(self):
        # self.contenedores(iexp=exp) #si el buque no tiene contenedores, no se puede enviar información al cliente:
        if True: #self.num_contenedores>0:

            self.mensaje_nuevo_buque =''

            sql_new = f"""select exp.iexp, exp.cexp, exp.ipuedes, exp.flle, exp.fsal, exp.tdescon, exp.temb, exp.nrefcor, exp.nref ,exp.ientord , exp.nmaw 
                    , pdes.dpue puertodestino, pdes.cpue, pori.dpue puertoorigen, pori.cpue 
                    , cor.dnomcom consignee, cor.cent 
                    , tramedtra.cmedtra, tramedtra.dmedtra 
                    , exp.ientnav, navaetent.dnomcom shipper
              from traexp  exp 
              left join aebpue pdes on pdes.ipue=exp.ipuedes 
              left join aebpue pori on pori.ipue=exp.ipueori 
              left join aetent cor on cor.ient=exp.ientord 
              left join tramedtra on tramedtra.imedtra = exp.imedtra 
              LEFT JOIN AETENT navaetent ON navaetent.ient=exp.ientnav 
              where exp.iexp={str(self.iexp)}"""

            resp_sql_new = self.bm.n_consulta(query=sql_new)

            if resp_sql_new["status_code"] == 200:
                self.resumen += "\nNuevo buque. "
                exp =resp_sql_new["contenido"][0]

                ETA_m, ETD_m ="", ""
                if (exp["flle"] != None):
                    fecha = datetime.datetime.strptime(exp["flle"], "%Y-%m-%dT%H:%M:%S")
                    ETA_m = "ETA: " + fecha.strftime("%Y-%m-%d")
                    self.resumen += ETA_m
                if (exp["fsal"] != None):
                    fecha = datetime.datetime.strptime(exp["fsal"], "%Y-%m-%dT%H:%M:%S")
                    ETD_m = "ETD: " + fecha.strftime("%Y-%m-%d")
                    self.resumen += ETD_m

                self.mensaje_nuevo_buque +=  'NEW BOOKING CONFIRMATION  REF: '+exp["cexp"] + "\n\n"
                self.mensaje_nuevo_buque += 'Dear Client, \n\n Find below the booking details for the reference routing:'
                self.mensaje_nuevo_buque += "\n\nCLIENT REFERENCE: " + exp["nrefcor"] if exp["nrefcor"] != None else ""
                self.mensaje_nuevo_buque += "\nPORT OF ORIGIN: " + exp["puertoorigen"] if exp["puertoorigen"] != None else ""
                self.mensaje_nuevo_buque += "\nPORT OF DESTINATION: " + exp["puertodestino"] if exp["puertodestino"] != None else ""
                self.mensaje_nuevo_buque += "\nCONSIGNEE: " +  exp["consignee"] if exp["consignee"] != None else ""
                self.mensaje_nuevo_buque += "\nTYPE OF CONTAINERS: "+self.tiposnct if self.tiposnct!=None else ""
                self.mensaje_nuevo_buque += "\nPALLETS/BULKS: " +str(self.qbul)
                self.mensaje_nuevo_buque += "\nWEIGHT: " + str(self.kbru)
                self.mensaje_nuevo_buque += "\nVOL: "+str(self.qvol)
                self.mensaje_nuevo_buque += "\nVESSEL: " + exp["dmedtra"] if exp["dmedtra"] != None else ""
                self.mensaje_nuevo_buque += "\nVESSEL Company: "+ exp["shipper"] if exp["shipper"] != None else ""
                self.mensaje_nuevo_buque += "\n"+ETD_m
                self.mensaje_nuevo_buque += "\n"+ETA_m
                self.resumen +=f"PALLETS/BULKS: {str(self.qbul)}, VESSEL:{exp["dmedtra"]}"
                nrefcor= " - " +exp["nrefcor"] if exp["nrefcor"] != None else ""

                self.set_asunto("BOOKING CONFIRMATION REF: " + self.cexp +  nrefcor)


            self.ordenantes_partidas()

    def contenedores (self,iexp=0):

        query_ctn =f"""select ctn.ictn, ctn.npre, ctn.qbul, ctn.qvol, ctn.kbru , aebctn.ictntip , aebctnt.dctntip, aebctnt.cctntip 
                    from traexpctn  ctn 
                    LEFT JOIN aebctn ON dbo.aebctn.ictn = ctn.ictn 
                    left join aebctnt on aebctnt.ictntip=aebctn.ictntip 
                    where ctn.iexp={str(iexp)}"""

        resp_sql_ctn = self.bm.n_consulta(query=query_ctn)

        ETA_m = ""
        ETD_m = ""
        qbul=0.
        qvol=0.
        kbru=0.
        tipoonct=''
        self.num_contenedores=len(resp_sql_ctn["contenido"])
        if self.num_contenedores >1:
            self.mensaje_contenedores="\n\nCONTAINERS:"
        elif self.num_contenedores >0:
            self.mensaje_contenedores = "\n\nCONTAINER:"

        for ctn in resp_sql_ctn["contenido"]:
            self.qbul+=ctn["qbul"] if ctn["qbul"]!=None else 0
            self.kbru+=ctn["kbru"] if ctn["kbru"]!=None else 0
            self.qvol+=ctn["qvol"] if ctn["qvol"]!=None else 0
            self.tiposnct+=ctn["cctntip"] if ctn["cctntip"]!=None else ""

            self.mensaje_contenedores+="\nCONTAINER: "+ctn["npre"]  if ctn["npre"]!=None else ""
            self.mensaje_contenedores+="\nEQUIPMENT: "+ctn["cctntip"] if ctn["cctntip"]!=None else ""
            self.mensaje_contenedores+="\nPALLETS/BULKS: "+str(ctn["qbul"]) if ctn["qbul"]!=None else ""
            self.mensaje_contenedores+="\nWEIGHT: "+str(ctn["kbru"]) if ctn["kbru"]!=None else ""
            self.mensaje_contenedores+= "\nVOL: "+str(ctn["qvol"]) if ctn["qvol"]!=None else ""
            self.mensaje_contenedores+= "\n\n"

    def actualizacion_buque(self):

        ordenantes_flag = False
        self.set_asunto(f" NOTIFICATION OF VESSEL MODIFIED DEPARTURE AND ARRIVAL DATES REF: {self.cexp}")
        self.mensaje_actualizacion   = "Dear client, \n\n"
        self.mensaje_actualizacion  += "\nPlease, be informed the assigned vessel to transport your goods has modified the estimated departure and arrival dates:\n"
        self.mensaje_actualizacion  += "\nTrip " + self.cexp
        self.mensaje_actualizacion  += " from "+self.dpobori  if self.dpobori != None else ""
        self.mensaje_actualizacion  += " to "+self.dpobdes  if self.dpobdes != None else ""
        self.resumen += "\nModified departure and arrival."
        if (self.HITOS608_MAXITRK >self.HITOS606_MAXITRK):# 608 --- salida
            fecha = datetime.datetime.strptime(expediente["fsal"], "%Y-%m-%dT%H:%M:%S")
            self.mensaje_actualizacion  += "\nNew ETD: " + fecha.strftime("%Y-%m-%d")
            self.mensaje_hitos += "\nNew ETD: " + fecha.strftime("%Y-%m-%d")
            ordenantes_flag =True

        if (self.HITOS607_MAXITRK > self.HITOS606_MAXITRK): # 607 --- llegada
            fecha = datetime.datetime.strptime(expediente["flle"], "%Y-%m-%dT%H:%M:%S")
            self.mensaje_actualizacion  += "\nNew ETA: " + fecha.strftime("%Y-%m-%d")
            self.mensaje_hitos += "\nNew ETA: " + fecha.strftime("%Y-%m-%d")
            ordenantes_flag = True
        if ordenantes_flag:
            self.ordenantes_partidas()

    def ordenantes_emails(self,iexpord=0):
        redo_ord = {}
        emails_enviar = []

        query_emails = f"""select  distinct nptocom 
                            from aetentpto  
                            inner join aebptocom on aebptocom.iptocom = aetentpto.iptocom 
                            where aebptocom.iptocomtip= 3 and ient={str(iexpord)} """
        rep_emails_exp = self.bm.consulta_(query=query_emails)
        self.resumen+= "\nEmails: "
        if len(rep_emails_exp["contenido"]) > 0:
            redo_ord["mensaje_emails"] = ""
            for correo in rep_emails_exp["contenido"]:
                if correo["nptocom"] not in emails_enviar:
                    emails_enviar.append(correo["nptocom"])
                    self.resumen+= f"{correo["nptocom"]} "
                self.mensaje_email_enviar += correo["nptocom"]+"\n"
            return emails_enviar
        else:
            self.mensaje_email_enviar = "\n\n There are not emails associeted to the Ordenant at Trip level."
            self.resumen += f"No hay emails asociados {str(iexpord)}"

        return redo_ord

    def expedientes_hitos (self):

        query_hitos = f"""select max(itrk) itrk, ihit, count(itrk) count_itrk 
                        from aebtrk 
                        where ihit in (606,607,608) 
                        and dtab='traexp' and creg= str({self.iexp})
                        group by ihit"""
        rexp_hitos = self.bm.n_consulta(query=query_hitos)

        for resp in rexp_hitos["contenido"]:
            if resp["ihit"] == 606:
                self.HITOS606_MAXITRK = resp["itrk"]
                self.HITOS606_count = resp["count_itrk"]
            if resp["ihit"] == 607:
                self.HITOS607_MAXITRK = resp["itrk"]
                self.HITOS607_count = resp["count_itrk"]
            if resp["ihit"] == 608:
                self.HITOS608_MAXITRK = resp["itrk"]
                self.HITOS608_count = resp["count_itrk"]

        if self.HITOS606_count == 0 and ( self.HITOS608_count == 1 or  self.HITOS607_count == 1):
            self.nuevo_buque_bool=True
            self.nuevo_buque()
        else:
            self.modificacion_fecha_bool=True
            self.actualizacion_buque()

    def ordenantes_partidas (self):
        querytrapda = f"""select aetent_dest.dnomcom dest_dnomcom,aetent_exp.dnomcom exp_dnomcom
                        , trapda.ientdes ,trapda.ientexp,coalesce(trapda.ientord, traexp.ientord) as ientord
                        ,trapda.ipda,trapda.cpda
                        from trapda 
                        left join aetent aetent_dest on trapda.ientdes=aetent_dest.ient 
                        left join aetent aetent_exp on trapda.ientexp=aetent_dest.ient 
                        left join traexp on traexp.iexp = trapda.iexp
                        where trapda.iexp= {str(self.iexp)}"""
        resp_partidas = self.bm.n_consulta(querytrapda)

        ordenantes = []
        for partida in resp_partidas["contenido"]:

            encontrado = False

            for ordenante in ordenantes:  # Existe el ordenante
                if (ordenante["ientord"] == partida["ientord"]):
                    encontrado = True
                    ordenante["partidas"].append(partida)
            if not encontrado:
                ordenante = {"partidas": [], "emails": [], "ientord": partida["ientord"]}
                ordenante ["partidas"].append(partida)
                ordenante["emails"] = self.ordenantes_emails(partida["ientord"])
                self.ordenantes.append(ordenante)

    def goods_affected (self, ordenante = {}):
        print ("Goods affected")
        mensaje_goods_affected ='\nGoods Affected: \n\n'
        for partida in ordenante ["partidas"]:
            mensaje_goods_affected+=f"CPDA: {partida["cpda"]},  Company Destination: {partida['exp_dnomcom']}"
        return mensaje_goods_affected

    def comunicar_hito(self):

        ahora = datetime.datetime.now()
        desc=""
        if self.nuevo_buque_bool:
            desc="Nuevo Buque"
        else:
            desc=self.mensaje_hitos
        hito_comunicado_json = {
            "codigohito": "AVMODFE",
            "descripciontracking": desc,
            "fechatracking": ahora.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        }

        resp_hito=self.bm.post_expediente_hito(data_json=hito_comunicado_json, id_expediente=self.iexp)
        if resp_hito["status_code"]==201:
            self.resumen=f"\nHito comunicado con éxito. Descripción {desc}"
        else:
            self.resumen="\nHito no comunicado. Revisa si no quieres que se vuelva a envair."

if __name__ == '__main__':

    m = maritimo_expedientes()
    expedientes=m.expedientes()
    e = email()
    for expediente in expedientes:
        exp=maritimo(expediente)
        exp.enviar_mensaje=True
        exp.expedientes_hitos()
        print("Fin del proceso")

        for ordenante in exp.ordenantes:

            if exp.nuevo_buque_bool:
                mensaje = exp.mensaje_nuevo_buque
                mensaje += exp.mensaje_email_enviar
                mensaje += exp.goods_affected(ordenante)

            elif exp.modificacion_fecha_bool:
                mensaje=exp.mensaje_actualizacion
                mensaje+=exp.mensaje_email_enviar
                mensaje += exp.goods_affected(ordenante)

            e.send_email(body=mensaje, subject=exp.get_asunto(), from_addr="Anexa Notifications",to_addrs=exp.set_to_list(ordenante["emails"]))


        exp.comunicar_hito()

    if len(expedientes)>0:
        e.send_email(body=m.get_resumen(), subject=m.get_asunto(), from_addr="Anexa Notifications Summarize",to_addrs=m.get_to_())