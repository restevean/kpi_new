# utils.bordero_xbs.py

import json
import logging


class BorderoXBS:

    def __init__(self):
        self.bordero = {"partidas": []}
        self.a00 = {}
        self.line = {}

    @staticmethod
    def read_xbs_file(fichero):
        """Leer el estado del fichero de Gruber"""
        resultado = {"Lineas": []}

        with open(fichero, "rt") as f:
            for fila in f:
                print(fila[:11])
                if fila[:11] == "@@PHBORD512":  # Mensaje tipo estado
                    resultado["header"] = fila  # Por el momento no tiene valor desagregar la línea
                if fila[:3] == "A00":  # cabecera
                    resultado["A00"] = {
                        "record_type_a00": fila[0:3],
                        "data_type_qualifier": fila[4:6],
                        "release_version": fila[7:9],
                        "waybill_number": fila[10:44],
                        "waybill_date": fila[45:52],
                        "transport_type": fila[53:55],
                        "product": fila[56:58],
                        "code_list_(cooperation)": fila[59:61],
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
                if fila[:3] == "A10":  # linea
                    resultado["Lineas"].append(
                        {
                            "record_type_a10": fila[0:3],
                            "loading_units_no._1_(lui)": fila[4:38],
                            "lead_seal_number_1_for_lu": fila[39:73],
                            "lead_seal_number_2_for_lu": fila[74:108],
                            "lead_seal_number_3_for_l": fila[109:143],
                            "lead_seal_number_4_for_lu": fila[144:178],
                            "loading_units_no._2_(lu2)": fila[179:213],
                            "lead_seal_number_1_for_lu_2": fila[214:248],
                            "lead_seal_number_2_for_lu_2": fila[249:283],
                            "lead_seal_number_3_for_lu_2": fila[284:318],
                            "lead_seal_number_4_for_lu_2": fila[319:353],
                        }
                    )
                if fila[:3] == "B00":  # linea
                    resultado["Lineas"].append(
                        {
                            "record_type_b00": fila[0:3],
                            "sequential_waybill_item": fila[4:6],
                            "qualifier_(address_type)": fila[7:9],
                            "name_1": fila[10:44],
                            "street_and_street_number": fila[45:79],
                            "country_code": fila[80:82],
                            "postcode": fila[83:91],
                            "place": fila[92:126],
                            "customer/partner_id": fila[127:161],
                            "name_2": fila[162:196],
                            "name_3": fila[197:231],
                            "town_area": fila[232:266],
                            "international_localisation_number_(iln)": fila[267:301],
                            "customs_id": fila[302:336],
                        }
                    )
                if fila[:3] == "B10":  # linea
                    resultado["Lineas"].append(
                        {
                            "record_type_b10": fila[0:3],
                            "sequential_waybill_item": fila[4:6],
                            "communication_type_qualifier": fila[7:9],
                            "content_of_communication": fila[10:265],
                        }
                    )

                if fila[:3] == "C00":  # linea
                    resultado["Lineas"].append(
                        {
                            "recorde_c00": fila[0:3],
                            "se_uential_wa_bill_item": fila[4:6],
                            "dua_ment": fila[7:9],
                            "customs_procedure": fila[10:13],
                            "customs_office": fila[14:21],
                            "country_of_origin1": fila[22:24],
                            "country_of_origin2": fila[25:27],
                            "consignor_country": fila[28:30],
                            "destination_country": fila[31:33],
                            "destination_land_federal_state": fila[34:36],
                            "country_of_importer": fila[37:39],
                            "statistics_status": fila[40:42],
                            "point_of_delivery_(delivery_term_town)": fila[43:77],
                            "transit_value": fila[78:86],
                            "currency_of_transit_value": fila[87:89],
                            "business_type": fila[90:92],
                            "mode_of_trans_ort_to_border": fila[93:95],
                            "domestic_mode_of_trans": fila[96:98],
                            "customs_office_of_entry": fila[99:106],
                            "preceding_document_type": fila[107:116],
                            "preceding_document_number": fila[117:141],
                            "appendix_type_01": fila[142:147],
                            "appendix_number_01": fila[148:167],
                            "appendix_date_01": fila[168:175],
                            "appendix_type_02": fila[176:181],
                            "appendix_number_02": fila[182:201],
                            "appendix_date_02": fila[202:209],
                            "appendix_type_03": fila[210:215],
                            "appendix_number_03": fila[216:235],
                            "appendix_date_03": fila[236:243],
                        }
                    )
                    if fila[:4] == "@@PT":  # linea
                        resultado["Lineas"].append(
                            {
                                'Record type@@PT': fila[:4],
                                'Empty record': fila[4:128],
                            }
                        )
        return resultado


    def expediente_ref_cor (self):
        return self.cabecera["waybill_number"].strip()

    # TODO Ojo! cuál es nrefcor? expediente_ref_cor o partida_ref_cor
    def partida_ref_cor(self):
        return self.cabecera[' ---¿?--- '].strip