# utils.bordero_xbs.py

import json
import logging


class BorderoXBS:

    def __init__(self):
        self.bordero = {"partidas": []}
        self.a00 = {}
        self.linea = {}

    def registro_a00(self, fila=""):
        reg_a00 = {
            "record_type": fila[0:2],
            "data_type_qualifier": fila[4:6],
            "release_version": fila[7:9],
            "waybill_number": fila[10:44],
            "waybill_date": fila[45:52],
            "transport_type": fila[53:55],
            "product": fila[56:58],
            "code_list": fila[59:61],
            "currency": fila[62:64],
            "identification_of_waybill_consignor": fila[65:99],
            "identification_of_waybill_consignee": fila[100:134],
            "freight_operator": fila[135:169],
            "freight_operator_country": fila[170:172],
            "freight_operator_postcode": fila[173:181],
            "freight_operator_town": fila[182:216],
            "vehicle_license_number_1": fila[217:251],
            "vehicle_license_number_2": fila[252:286],
            "routing_id_1": fila[287:321],
            "routing_id_2": fila[322:356],

        }
        self.bordero["partidas"].append(reg_a00)
        self.a00 = reg_a00
        return reg_a00

    def expediente_ref_cor (self):
        return self.cabecera[" waybill_number g add ---¿?--- "].strip()

    def partida_ref_cor(self):
        return self.cabecera[' ---¿?--- '].strip