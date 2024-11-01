import requests
from requests.exceptions import HTTPError
import os
from dotenv import load_dotenv
from utils.safe_get_token import safe_get_token
# from datetime import datetime, timedelta
# import json


load_dotenv(dotenv_path='../conf/.env')
ENTORNO = os.getenv("ENTORNO")
BM_API_URL = os.getenv("BM_API_URL")
BM_API_AUTH_URL = os.getenv("BM_API_AUTH_URL")
USER_ANE_TEST = os.getenv("USER_ANE_TEST")
PW_ANE_TEST = os.getenv("PW_ANE_TEST")
USER_ANE = os.getenv("USER_ANE")
PW_ANE = os.getenv("PW_ANE")


class BmasterApi:

    def __init__(self) -> None:
        self.url= BM_API_URL
        self.token="Bearer " + safe_get_token(context=ENTORNO)  # Ojo!!! Especificar que token queremos,
        # "dev" o "prod",
                                                        # si no la función no devolverá token
        self.headers={'content-type':'application/json', 'encoding': 'charset=utf-8','Authorization': self.token}

    @staticmethod
    def build_in_clause(self, refs=None):
        if refs is None:
            refs = []
        in_clause = "(" + ", ".join(str(ref["iref"]) for ref in refs) + ")"
        return in_clause

    """
    Parece que no se usa
    @staticmethod
    def fecha_iso(fecha_entrada: str = "", formato: str = "%Y%m%d") -> str:
        fecha = datetime.strptime(fecha_entrada, formato)
        return fecha.strftime("%Y-%m-%d") + "T01:00.000Z"
    """

    def post_expediente(self, data_json):
        url_=self.url+"Expediente"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_entrada(self, data_json):
        url_=self.url+'entrada/'
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_entrada_linea(self, line_id=0, data_json=None):
        if data_json is None:
            data_json = {}
        url_=self.url+'entrada/'+str(line_id)+"/lineas/"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_entrada_etiqueta(self, data_json=None, ialment=0):
        if data_json is None:
            data_json = {}
        url_=self.url+'Entrada/'+str(ialment)+"/etiquetas"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def put_entrada(self, data_json=None, ialment=0):
        if data_json is None:
            data_json = {}
        url_=self.url+'Entrada/'+str(ialment)
        try:
            respuesta=self.peticion_put(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_expediente_hito(self, data_json,id_expediente):
        url_=self.url+"Expediente/"+str(id_expediente)+"/Tracking"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
    def post_albaran_vinculos(self, alvi_id, data_json):
        url_=self.url+'Albaran/'+str(alvi_id)+"/vinculos"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_albaran_vinculos_tracking(self, vitr_id, data_json):
        url_=self.url+'Albaran/'+str(vitr_id)+"/tracking"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def get_albaran_vinculos(self, itdialb):
        url_=self.url+'Albaran/'+str(itdialb)+"/vinculos"
        try:
            respuesta= self.peticion_get(url_)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_partida_tracking(self, ipda, data_json):
        url_=self.url+'Partida/'+str(ipda)+"/tracking" # ?id="+str(ipda)
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_partida(self, data_json):
        url_=self.url+'Partida/'
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def put_partida(self, ipda=0, data_json=None):
        if data_json is None:
            data_json = {}
        url_=self.url+'Partida/'+str(ipda)
        try:
            respuesta=self.peticion_put(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_partida_vinculos(self, ipda, data_json):
        url_=self.url+'Partida/'+str(ipda)+"/vinculos"#?id="+str(ipda)
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_partida_vinculos_tracking(self, vitr_id, data_json):
        url_=self.url+'Partida/'+str(vitr_id)+"/tracking"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_partida_lineas_mercancia(self, lime_id, data_json):
        url_=self.url+'Partida/'+str(lime_id)+"/lineas"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_partida_etiqueta(self, paet_id, data_json):
        url_=self.url+'Partida/'+str(paet_id)+"/etiquetas"
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def get_partida_vinculos(self, ipda=0):
        url_=self.url+'Partida/'+str(ipda)+"/vinculos"
        try:
            respuesta= self.peticion_get(url_)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def get_partida_relacionados(self, ipda=0):
        url_=self.url+'Partida/'+str(ipda)+"/relacionados"
        try:
            respuesta= self.peticion_get(url_)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def get_partida(self, ipda):
        url_=self.url+'Partida/'+str(ipda)+"/vinculos"
        try:
            respuesta= requests.get(url_,headers=self.headers)
            return respuesta.json()
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def post_albaran_entrada(self, data_json):
        url_=self.url+'Entrada'
        try:
            respuesta=self.peticion_post(url_,data_json)
            return respuesta
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    def referencias_propietarios(self):
        print("BM - referencias propietarios")
        fecha="'2022-09-01'"
        query="select distinct ient from LAXREF where FMOD >"+fecha
        return self.consulta_(query)

    def referencias (self, propietario):
        print("BM - referencias")
        try:
            fecha="'2022-09-01'"
            q_propietario=""
            if propietario is not None:
                q_propietario=" and laxref.ient="+str(propietario)
            else:
                q_propietario=" and laxref.ient is null"

            query_ref="select distinct iref from ("
            query_ref+="select distinct iref from laxref WHERE tbas='A' and FMOD>"+fecha
            query_ref+=q_propietario
            query_ref+=" union all "
            query_ref+=" select distinct creg as iref from laxcar "
            query_ref+= " inner join laxref on laxref.iref=laxcar.creg"
            query_ref+=" WHERE  laxcar.FMOD>"+fecha
            query_ref+=q_propietario
            query_ref+=" union all "
            query_ref+=" select distinct laxrefcod.iref as iref from laxrefcod "
            query_ref+=" inner join laxref on laxref.iref=laxrefcod.iref"
            query_ref+=" WHERE  laxrefcod.FMOD>"+fecha
            query_ref+=q_propietario
            query_ref+=") ref"
            query_ref+=" where iref>0"
            query_ref+=" order by iref asc"

            print(query_ref)
            refs=self.consulta_(query_ref)
            in_refs=[]

            x_1=0
            total=len(refs)
            if 50<total:
                x=50
            else:
                x=total
            flag=True
            while x<=total or flag:
                in_refs.append(self.build_in_clause(refs[x_1:x]))
                x_1=x

                if x+50<total:
                    x=x+50
                elif x==total:
                    break
                else:
                    x=total

            resultado_final=[]
            ean_final=[]

            for in_iref in in_refs:
                ient_query=" and ient="+str(propietario)
                if propietario is None: #Referencias sin propietarios
                    ient_query=""

                if in_iref!="()":
                    query_referencias="select laxref.* "
                    query_referencias+=", format(Isnull((select TOP 1 qca1 from laxcar A where A.icartip = 212 and A.dtab = 'laxref' and A.creg = laxref.iref)*10, 0), '000000')as 'alto'" #centímetros
                    query_referencias+=", format(Isnull((select TOP 1 qca1 from laxcar A where A.icartip = 214 and A.dtab = 'laxref' and A.creg = laxref.iref)*10, 0), '000000')as 'largo'" #centímetros
                    query_referencias+=", format(Isnull((select TOP 1 qca1 from laxcar A where A.icartip = 213 and A.dtab = 'laxref' and A.creg = laxref.iref)*10, 0), '000000')as 'ancho'" #centímetros
                    query_referencias+=", format(Isnull((select TOP 1 qca1 from laxcar A where A.icartip = 3 and A.dtab = 'laxref' and A.creg = laxref.iref), 0), '000000')as 'peso'" #gramos
                    query_referencias+=" from laxref"
                    query_referencias+=" where iref in "+in_iref +" and laxref.tbas = 'A'"
                    query_referencias+=ient_query
                    query_referencias+=" order by laxref.iref asc"

                    resultado=[]
                    resultado=self.consulta_(query_referencias)
                    resultado_final.append(resultado)

                    ean=[]
                    query_ean ="select * from laxrefcod"
                    query_ean +=" where iref in "+in_iref
                    query_ean +=" order by iref asc"
                    ean=self.consulta_(query_ean)
                    ean_final.append(ean)
            return resultado_final, ean_final
        except Exception as e:
            print("Problemas cogiendo refenencias")
            print(e)

    def consulta_ (self, query):
        url=self.url+"Consulta"
        resp_dic={}
        try:
            peticion=requests.post(url, json=query, headers=self.headers)
            #print(peticion.json())
            # print(query)
            resp_dic["status_code"]=peticion.status_code
            resp_dic["contenido"]=peticion.json()
            return resp_dic#peticion.json()
        except Exception as e:
            print("Problemas cogiendo en la CONSULTA_   \n" + query)
            print(e)
            resp_dic["status_code"]=peticion.status_code
            resp_dic["contenido"]=peticion.json()
            return resp_dic

    def cabecera_alb_entrada (self, cabecera):
        url_=self.url+'Entrada'
        fecha=cabecera["Data"].split("-")
        cabecera_json={
            "entrada": cabecera["numero"],
            "fechallegada": fecha[2]+"-"+fecha[1]+"-"+fecha[0],#"2022-12-26T20:59:05.636Z",
            "horallegada": "00:00",
            "almacenfisico": "ALM",
            "almacenlogico": "string",
            "division": "string",
            "trafico": "string",
            "normativa": "string",
            "tipotrafico": "string",
            "unidadcarga": "string",
            "projecto": "string",
            "personaresponsable": "string",
            "chofer": "string",
            "numeroreferencia": "string",
            "tipocontenedor": "string",
            "contenedor": "string",
            "tipomediotransporte": "string",
            "mediotransporte": "string",
            "localizacionmercancia": "string",
            "observaciones": "string",
            "ordenante": {
                "id": 0,
                "division": "string",
                "codigo": "string",
                "descripcion": "string",
                "cif": "string",
                "codigorelacioncliente": "string",
                "codigorelacionproveedor": "string",
                "direcciones": [
                    {
                        "tipodireccion": "string",
                        "direccion": "string",
                        "poblacion": "string",
                        "codigopostal": "string",
                        "codigopais": "string",
                        "horario": "string"
                    }
                ],
                "puntoscomunicacion": [
                    {
                        "tipocontacto": "string",
                        "contacto": "string"
                    }
                ]
            },
            "clientefacturacion": {
                "id": 0,
                "division": "string",
                "codigo": "string",
                "descripcion": "string",
                "cif": "string",
                "codigorelacioncliente": "string",
                "codigorelacionproveedor": "string",
                "direcciones": [
                    {
                        "tipodireccion": "string",
                        "direccion": "string",
                        "poblacion": "string",
                        "codigopostal": "string",
                        "codigopais": "string",
                        "horario": "string"
                    }
                ],
                "puntoscomunicacion": [
                    {
                        "tipocontacto": "string",
                        "contacto": "string"
                    }
                ]
            },
            "expedidor": {
                "id": 0,
                "division": "string",
                "codigo": "string",
                "descripcion": "string",
                "cif": "string",
                "codigorelacioncliente": "string",
                "codigorelacionproveedor": "string",
                "direcciones": [
                    {
                        "tipodireccion": "string",
                        "direccion": "string",
                        "poblacion": "string",
                        "codigopostal": "string",
                        "codigopais": "string",
                        "horario": "string"
                    }
                ],
                "puntoscomunicacion": [
                    {
                        "tipocontacto": "string",
                        "contacto": "string"
                    }
                ]
            },
            "generarentidadexpedidora": False,
            "destinatario": {
                "id": 0,
                "division": "string",
                "codigo": "string",
                "descripcion": "string",
                "cif": "string",
                "codigorelacioncliente": "string",
                "codigorelacionproveedor": "string",
                "direcciones": [
                    {
                        "tipodireccion": "string",
                        "direccion": "string",
                        "poblacion": "string",
                        "codigopostal": "string",
                        "codigopais": "string",
                        "horario": "string"
                    }
                ],
                "puntoscomunicacion": [
                    {
                        "tipocontacto": "string",
                        "contacto": "string"
                    }
                ]
            },
            "generarentidaddestinataria": False,
            "transportista": {
                "id": 0,
                "division": "string",
                "codigo": "string",
                "descripcion": "string",
                "cif": "string",
                "codigorelacioncliente": "string",
                "codigorelacionproveedor": "string",
                "direcciones": [
                    {
                        "tipodireccion": "string",
                        "direccion": "string",
                        "poblacion": "string",
                        "codigopostal": "string",
                        "codigopais": "string",
                        "horario": "string"
                    }
                ],
                "puntoscomunicacion": [
                    {
                        "tipocontacto": "string",
                        "contacto": "string"
                    }
                ]
            },
            "representante": {
                "id": 0,
                "division": "string",
                "codigo": "string",
                "descripcion": "string",
                "cif": "string",
                "codigorelacioncliente": "string",
                "codigorelacionproveedor": "string",
                "direcciones": [
                    {
                        "tipodireccion": "string",
                        "direccion": "string",
                        "poblacion": "string",
                        "codigopostal": "string",
                        "codigopais": "string",
                        "horario": "string"
                    }
                ],
                "puntoscomunicacion": [
                    {
                        "tipocontacto": "string",
                        "contacto": "string"
                    }
                ]
            },
            "aduanaorigen": "string",
            "aduanadestino": "string",
            "paisexpedidor": "string",
            "paistransportefrontera": "string",
            "paisdestinatario": "string",
            "estatutomercancia": "string",
            "fechadmision": "2022-12-26T20:59:05.637Z",
            "valores": [
                {
                    "concepto": "string",
                    "descripcionconceptoespecial": "string",
                    "valor": 0,
                    "divisa": "string",
                    "cambio": 0,
                    "identidad": 0,
                    "fechamovimiento": "2022-12-26T20:59:05.637Z",
                    "tipolinea": "G"
                }
            ],
            "lineas": [
                {
                    "partidaarancelaria": "string",
                    "almacenlogico": "string",
                    "referencia": "string",
                    "lote": "string",
                    "fechacaducidad": "2022-12-26T20:59:05.637Z",
                    "numeroserie": "string",
                    "unidades": 1,
                    "tipobultointerno": "string",
                    "tipobultoexterno": "string",
                    "pesobruto": 0,
                    "pesoneto": 0,
                    "pesotasable": 0,
                    "volumen": 0,
                    "metrolineal": 0,
                    "metrocuadrado": 0,
                    "importedivsabase": 0,
                    "cantidadpalets": 0,
                    "palet": "string",
                    "cantidadcajas": 0,
                    "caja": "string",
                    "mercancia": "string",
                    "marcas": "string",
                    "personaresponsable": "string",
                    "ubicacion": "string",
                    "situacion": "string",
                    "tipounidades": "string",
                    "dimensiones": [
                        {
                            "bultos": 1,
                            "altura": 0,
                            "ancho": 0,
                            "largo": 0,
                            "pesobrutounitario": 0
                        }
                    ]
                }
            ],
            "servicios": [
                {
                    "servicio": "string",
                    "fechaservicio": "2022-12-26T20:59:05.637Z",
                    "observaciones": "string",
                    "cantidad": 0,
                    "valor": 0,
                    "divisa": "string",
                    "cambio": 0
                }
            ],
            "etiquetas": [
                {
                    "codigobarras": "string",
                    "altura": 0,
                    "ancho": 0,
                    "largo": 0
                }
            ],
            "tracking": [
                {
                    "codigohito": "string",
                    "descripciontracking": "string",
                    "fechatracking": "2022-12-26T20:59:05.637Z"
                }
            ],
            "comentarios": [
                {
                    "id": 0,
                    "tipo": "string",
                    "comentario": "string"
                }
            ],
            "vinculos": [
                {
                    "id": 0,
                    "tipo": "string",
                    "nombrevinculo": "string",
                    "b64vinculo": "string",
                    "descripcion": "string"
                }
            ]
        }
        try:
            albaran=requests.post(url_,json=cabecera_json,headers=self.headers)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return albaran

    def peticion_post (self,url, _json):
        try:
            peticion=requests.post(url,json=_json,headers=self.headers)
            resp_dic={}
            resp_dic["status_code"]=peticion.status_code
            resp_dic["contenido"]=peticion.json()
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return resp_dic

    def peticion_put (self,url, _json):
        try:
            peticion=requests.put(url,json=_json,headers=self.headers)
            resp_dic={}
            resp_dic["status_code"]=peticion.status_code
            resp_dic["contenido"]=peticion.json()
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return resp_dic  #peticion.json()

    def peticion_get (self,url):
        try:
            peticion=requests.get(url,headers=self.headers)
            resp_dic={}
            resp_dic["status_code"]=peticion.status_code
            resp_dic["contenido"]=peticion.json()
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return resp_dic  #peticion.json()

    def cabecera_alb_salida_post(self, cabecera):
        url_=self.url+'Salida'
        peticion={}
        try:
            peticion=self.peticion_post( url_, cabecera)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return peticion

    def linea_alb_salida_post(self, id, linea_json=None):
        if linea_json is None:
            linea_json = {}
        url_=self.url+'Salida/'+str(id)+'/lineas'
        peticion={}
        try:
            peticion=self.peticion_post( url_, linea_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return peticion     
