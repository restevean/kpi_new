import requests
from requests.exceptions import HTTPError
import logging
from typing import Any, Dict, Optional
import os
from dotenv import load_dotenv
from utils.safe_get_token import safe_get_token
# from datetime import datetime, timedelta
# import json


load_dotenv(dotenv_path='../conf/.env.base')
ENTORNO = os.getenv("ENTORNO")
INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
load_dotenv(dotenv_path=f'../conf/.env.{INTEGRATION_CUST}')
BM_API_URL = os.getenv("BM_API_URL")
BM_API_AUTH_URL = os.getenv("BM_API_AUTH_URL")
USER_ANE_TEST = os.getenv("USER_ANE_TEST")
PW_ANE_TEST = os.getenv("PW_ANE_TEST")
USER_ANE = os.getenv("USER_ANE")
PW_ANE = os.getenv("PW_ANE")


class BmasterApi:

    def __init__(self) -> None:
        self.url = BM_API_URL
        self.token = f"Bearer {safe_get_token(context=ENTORNO)}"
        self.headers={'content-type':'application/json', 'encoding': 'charset=utf-8','Authorization': self.token}


    @staticmethod
    def build_in_clause(self, refs=None):
        if refs is None:
            refs = []
        return "(" + ", ".join(str(ref["iref"]) for ref in refs) + ")"


    def post_expediente(self, data_json):
        url_ = f"{self.url}Expediente"
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def post_entrada(self, data_json):
        url_ = f'{self.url}entrada/'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def post_entrada_linea(self, line_id=0, data_json=None):
        if data_json is None:
            data_json = {}
        url_ = f'{self.url}entrada/{str(line_id)}/lineas/'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def post_entrada_etiqueta(self, data_json=None, ialment=0):
        if data_json is None:
            data_json = {}
        url_ = f'{self.url}Entrada/{str(ialment)}/etiquetas'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def put_entrada(self, data_json=None, ialment=0):
        if data_json is None:
            data_json = {}
        url_ = f'{self.url}Entrada/{str(ialment)}'
        try:
            return self.peticion_put(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def post_expediente_hito(self, data_json,id_expediente):
        url_ = f"{self.url}Expediente/{str(id_expediente)}/Tracking"
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def post_albaran_vinculos(self, alvi_id, data_json):
        url_ = f'{self.url}Albaran/{str(alvi_id)}/vinculos'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def post_albaran_vinculos_tracking(self, vitr_id, data_json):
        url_ = f'{self.url}Albaran/{str(vitr_id)}/tracking'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def get_albaran_vinculos(self, itdialb):
        url_ = f'{self.url}Albaran/{str(itdialb)}/vinculos'
        try:
            return self.peticion_get(url_)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def post_partida_tracking(self, ipda, data_json):
        url_ = f'{self.url}Partida/{str(ipda)}/tracking'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    
    def post_partida(self, data_json):
        url_ = f'{self.url}Partida/'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    
    def put_partida(self, ipda=0, data_json=None):
        if data_json is None:
            data_json = {}
        url_ = f'{self.url}Partida/{str(ipda)}'
        try:
            return self.peticion_put(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

   
    def post_partida_vinculos(self, ipda, data_json):
        url_ = f'{self.url}Partida/{str(ipda)}/vinculos'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    
    def post_partida_vinculos_tracking(self, vitr_id, data_json):
        url_ = f'{self.url}Partida/{str(vitr_id)}/tracking'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

   
    def post_partida_lineas_mercancia(self, lime_id, data_json):
        url_ = f'{self.url}Partida/{str(lime_id)}/lineas'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    
    def post_partida_etiqueta(self, paet_id, data_json):
        url_ = f'{self.url}Partida/{str(paet_id)}/etiquetas'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)

    
    def get_partida_vinculos(self, ipda=0):
        url_ = f'{self.url}Partida/{str(ipda)}/vinculos'
        try:
            return self.peticion_get(url_)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def get_partida_relacionados(self, ipda=0):
        url_ = f'{self.url}Partida/{str(ipda)}/relacionados'
        try:
            return self.peticion_get(url_)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def get_partida(self, ipda):
        url_ = f'{self.url}Partida/{str(ipda)}/vinculos'
        try:
            respuesta= requests.get(url_,headers=self.headers)
            return respuesta.json()
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def post_albaran_entrada(self, data_json):
        url_ = f'{self.url}Entrada'
        try:
            return self.peticion_post(url_,data_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)


    def consulta_(self, query):  # sourcery skip: extract-duplicate-method
        url = f"{self.url}Consulta"
        resp_dic={}
        peticion = None
        try:
            peticion = requests.post(url, json=query, headers=self.headers)
            resp_dic["status_code"] = peticion.status_code
            resp_dic["contenido"] = peticion.json()
            return resp_dic
        except Exception as e:
            print("Problemas cogiendo en la CONSULTA_   \n" + query)
            print(e)
            resp_dic["status_code"] = peticion.status_code
            resp_dic["contenido"] = peticion.json()
            return resp_dic

    def n_consulta(self, query: str) -> Dict[str, Any]:
        url = f"{self.url}Consulta"
        resp_dic: Dict[str, Any] = {}

        try:
            response = self.session.post(
                url,
                json=query,
                headers=self.headers,
                timeout=10  # Timeout en segundos
            )
            response.raise_for_status()  # Lanza una excepción para códigos de estado 4xx/5xx

            try:
                contenido = response.json()
            except ValueError as json_err:
                logging.error(f"Error al parsear JSON: {json_err} - Consulta: {query}")
                contenido = {"error": "Respuesta JSON inválida"}

            resp_dic["status_code"] = response.status_code
            resp_dic["contenido"] = contenido

            logging.info(f"Consulta ejecutada exitosamente: {query}")
            return resp_dic

        except requests.exceptions.Timeout:
            logging.error(f"Timeout al ejecutar la consulta: {query}")
            resp_dic["status_code"] = None
            resp_dic["contenido"] = {"error": "Timeout en la solicitud"}
            return resp_dic

        except requests.exceptions.HTTPError as http_err:
            status = response.status_code if 'response' in locals() else None
            logging.error(f"HTTP error al ejecutar la consulta: {http_err} - Consulta: {query}")
            resp_dic["status_code"] = status
            resp_dic["contenido"] = {"error": str(http_err)}
            return resp_dic

        except requests.exceptions.RequestException as req_err:
            logging.error(f"Error de conexión al ejecutar la consulta: {req_err} - Consulta: {query}")
            resp_dic["status_code"] = None
            resp_dic["contenido"] = {"error": str(req_err)}
            return resp_dic

        except Exception as e:
            logging.error(f"Error inesperado al ejecutar la consulta: {e} - Consulta: {query}")
            resp_dic["status_code"] = None
            resp_dic["contenido"] = {"error": "Error inesperado"}
            return resp_dic



    def peticion_post(self,url, _json):
        resp_dic = {}
        try:
            peticion=requests.post(url,json=_json,headers=self.headers)
            resp_dic = {"status_code": peticion.status_code, "contenido": peticion.json()}
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return resp_dic

    
    def peticion_put(self,url, _json):
        resp_dic = None
        try:
            peticion = requests.put(url,json=_json,headers=self.headers)
            resp_dic = {"status_code": peticion.status_code, "contenido": peticion.json()}
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return resp_dic  #peticion.json()

    
    def peticion_get(self,url):
        resp_dic = None
        try:
            peticion = requests.get(url,headers=self.headers)
            resp_dic = {"status_code": peticion.status_code, "contenido": peticion.json()}
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return resp_dic  #peticion.json()

    
    def cabecera_alb_salida_post(self, cabecera):
        url_ = f'{self.url}Salida'
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
        url_ = f'{self.url}Salida/{str(id)}/lineas'
        peticion={}
        try:
            peticion=self.peticion_post( url_, linea_json)
        except HTTPError as e:
            print (e.strerror)
        except Exception as fallo:
            print (fallo)
        return peticion     
