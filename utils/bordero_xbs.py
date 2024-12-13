# utils.bordero_xbs.py

import json
import logging

logger = logging.getLogger(__name__)

class BorderoXBS:

    def __init__(self):
        self.bordero = {"partidas": []}
        self.cabecera = {}
        self.line = {}

    def cabecera_xbs(self, fila):
        cabecera = {
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
            "lines": []
        }
        self.bordero["partidas"].append(cabecera)
        self.cabecera = cabecera
        return cabecera



    def linea_xbs(self, fila):

        if fila[:3] == "A10":  # linea
            linea = {
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

        if fila[:3] == "B00":  # linea
            linea = {
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

        if fila[:3] == "B10":  # linea
            linea = {
                "record_type_b10": fila[0:3],
                "sequential_waybill_item": fila[4:6],
                "communication_type_qualifier": fila[7:9],
                "content_of_communication": fila[10:265],
            }

        if fila[:3] == "C00":  # linea
            linea = {
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

        if fila[:3] == "D00":  # linea
            linea = {
                "record_type_d00": fila[1:3],
                "sequential_waybill_item": fila[4:6],
                "cons_nnent": fila[7:9],
                "num_err_of": fila[10:13],
                "pack": fila[14:16],
                "number_of_packages_on,'in_pales": fila[17:20],
                "pa": fila[21:23],
                "content_of": fila[24:58],
                "code_and_nun-oer": fila[59:93],
                "actual_weight": fila[94:102],
                "chargeable_weight": fila[103:111],
                "lenght_in_meters": fila[112:115],
                "width_in_meters": fila[116:119],
                "heigth_in_rneters": fila[120:123],
                "cubicmeters": fila[124:128],
                "loading_ræters": fila[129:131],
                "number_of_locations": fila[132:135],
            }

        if fila[:3] == "D10":  # linea
            linea = {
                "record_type_d10": fila[1:3],
                "sequential_waybill_item": fila[4:6],
                "consignment_position": fila[7:9],
                "product_number": fila[10:24],
                "country_of_origin": fila[25:27],
                "raw_mass_in_kg": fila[28:36],
                "fixed_load_in_kg": fila[37:45],
                "procedure_code": fila[46:50],
                "customs_value": fila[51:61],
                "currency_of_customs_value": fila[62:64],
                "statistical_value": fila[65:75],
                "currency_of_statistical_value": fila[76:78],
                "appendix_type_01": fila[79:84],
                "appendix_number_01": fila[85:104],
                "appendix_date_01": fila[105:112],
                "appendix_type_02": fila[113:118],
                "appendix_number_02": fila[119:138],
                "appendix_date_02": fila[139:146],
                "appendix_type_03": fila[147:152],
                "appendix_number_03": fila[153:172],
                "appendix_date_03": fila[173:180],
                "appendix_type_04": fila[181:186],
                "appendix_number_04": fila[187:206],
                "appendix_date_04": fila[207:214],
            }

        if fila[:3] == "E00":  # linea
            linea = {
                "record_type_e00": fila[1:3],
                "sequential_waybill_item": fila[4:6],
                "consignment_position": fila[7:9],
                "gg_release": fila[10:12],
                "number_of_packages": fila[13:16],
                "gross_weight_in_kg": fila[17:25],
                "un_number": fila[26:],
                "description_of_packaging": fila[30:64],
                "multiplie": fila[65:68],
                "gg_database_id": fila[69:71],
                "unique_key": fila[72:81],
                "material_name/dangerous_goods_description": fila[82:291],
                "additional_text_for_n.a.g.": fila[292:361],
                "dangerous_goods_sample": fila[362:364],
                "dangerous_goods_sample_1": fila[365:367],
                "dangerous_goods_sample_2": fila[368:370],
                "dangerous_goods_sample_3": fila[371:373],
                "packaging_group/classification_code": fila[374:377],
                "net_explosive_mass_in_kg": fila[378:386],
                "transport_class": fila[387:387],
                "limited_amount_lq_yn": fila[388:388],
                "calculated_dangerous_goods_points": fila[389:397],
            }

        if fila[:3] == "F00":  # linea
            linea = {
                "record_type_f00": fila[1:3],
                "sequential_waybill_item": fila[4:6],
                "consignment_position": fila[7:9],
                "barcode": fila[10:44],
                "reference_qualifier": fila[45:47],
                "reference_number": fila[48:82],
                "reference_qualifier_1": fila[83:85],
                "reference_number_1": fila[86:120],
                "reference_qualifier_2": fila[121:123],
                "reference_number_2": fila[124:158],
                "reference_qualifier_3": fila[159:161],
                "reference_number_3": fila[162:196],
                "reference_qualifier_4": fila[197:199],
                "reference_number_4": fila[200:234],
            }

        if fila[:3] == "G00":  # linea
            linea = {
                "record_type_g00": fila[1:3],
                "sequential_waybill_item": fila[4:6],
                "consignment_number_sending_depot": fila[7:41],
                "actual_consignment_gross_weight_in_kg": fila[42:50],
                "delivery_term": fila[51:53],
                "direct_delivery_yn": fila[54:54],
                "pickup_date:": fila[55:62],
                "picku_time_from": fila[63:66],
                "picku_time_to": fila[67:700],
                "logistics_model": fila[71:76],
                "consignment_number_for_receiving_depot": fila[77:111],
                "consignor_id_original_waybill": fila[112:146],
                "consignee_id_original_waybill": fila[147:181],
                "material_group": fila[182:184],
                "goods_val_ue": fila[185:195],
                "currency_of_goods_value": fila[196:198],
                "chargeable_consignment_weight_in_kg": fila[199:207],
                "cubic_meters": fila[208:212],
                "loading_meters": fila[213:215],
                "number_of_pallet_locations": fila[216:219],
                "number_of_additional_loading_tools_1": fila[220:221],
                "packaging_type_for_additional_loading_tools_1": fila[222:224],
                "number_of_additional_loading_tools_2": fila[225:226],
                "packaging_type_for_additional_loading_tools_2": fila[227:229],
            }

        if fila[:3] == "H00":  # linea
            linea = {
                "record_type_h00": fila[1:3],
                "sequential_waybill_item": fila[4:6],
                "text_code_1": fila[7:9],
                "additional_text_1": fila[10:44],
                "text_code_2": fila[45:47],
                "additonal_text_2": fila[48:82],
                "text_code_3": fila[83:85],
                "additional_text_3": fila[86:120],
                "text_code_4": fila[121:123],
                "additional_text_4": fila[124:158],
                "text_code_5'": fila[159:161],
                "additonal_text_5": fila[162:196],
                "text_code_6": fila[197:199],
                "additional_text_6": fila[200:234],
            }

        if fila[:3] == "H10":  # linea
            linea = {
                "record_type_h10": fila[:3],
                "sequential_waybill_item": fila[4:6],
                "qualifier_for_text_usage_1": fila[7:9],
                "any_text_1": fila[10:79],
                "qualifier_for_text_usage_2": fila[80:82],
                "any_text_2": fila[83:152],
                "qualifier_for_text_usage_3": fila[153:155],
                "any_text_3": fila[156:225],
            }

        if fila[:3] == "I00":  # linea
            linea = {
                "record_type_i00": fila[1:3],
                "se_uential": fila[4:6],
                "service_1": fila[7:9],
                "tax_code": fila[10:10],
                "amount_1": fila[11:10],
                "service_2": fila[20:22],
                "tax_code_2": fila[23:23],
                "amount_2": fila[24:32],
                "service_3": fila[33:35],
                "tax_code_3": fila[36:36],
                "amount_3": fila[37:45],
                "service_4": fila[46:48],
                "tax_code_4": fila[49:49],
                "amount_4": fila[50:58],
                "service_5": fila[59:61],
                "tax_code_5": fila[62:62],
                "amount_5": fila[63:71],
                "service_6": fila[72:74],
                "tax_code_6": fila[75:75],
                "amount_6": fila[76:84],
                "service_7": fila[85:87],
                "tax_code_7": fila[88:88],
                "amount_7": fila[89:97],
                "service_8": fila[98:100],
                "tax_code_8": fila[101:101],
                "amount_8": fila[102:110],
                "service_9": fila[111:113],
                "tax_code_9": fila[114:114],
                "amount_9": fila[115:123],
                "service_10": fila[124:126],
                "tax_code_10": fila[127:127],
                "amount_10": fila[128:136],
            }

        if fila[:3] == "J00":  # linea
            linea = {
                "record_type_j00": fila[1:3],
                "total_number_of_consignments": fila[4:6],
                "total_number_of_packages": fila[7:12],
                "actual_gross_weight_in_kg": fila[13:21],
                "number_of_box_pallets": fila[22:25],
                "number_of_euro_flat_pallets": fila[26:29],
                "number_of_additional_loading_tools_flat": fila[30:33],
                "number_of_additional_oadlng_tools_box": fila[34:37],
                "totals_taxable_from100": fila[38:48],
                "totals_not_taxable_from100": fila[49:59],
                "totals_of_roduct_value_cod_from_100": fila[60:70],
                "totals_of_customs_from_100": fila[71:81],
                "totals_of_im": fila[82:92],
                "totas_of_value_added_tax": fila[93:103],
            }

        if fila[:3] == "Z00":  # linea
            linea = {
                "record_type_z00": fila[1:3],
                "total_number_of_data_records": fila[4:9],
                "date_of_creation": fila[10:17],
                "time_of_creation": fila[18:23],
            }

        if fila[:4] == "@@PT":  # linea
            linea = {
                'Record type@@PT': fila[:4],
                'Empty record': fila[4:128],
            }

        # for i in range(len(self.bordero["partidas"])):
        #     if self.bordero["partidas"][i]["waybill_number"]==linea["waybill_number"]:
        #          self.bordero["partidas"][i]["lines"].append(linea)
        self.bordero["partidas"][0]["lines"].append(linea)
        self.linea=linea
        return linea


    def expediente_ref_cor (self):
        return self.cabecera["waybill_number"].strip()

    # TODO Ojo! cuál es nrefcor? expediente_ref_cor o partida_ref_cor
    def partida_ref_cor(self):
        return self.cabecera[' ---¿?--- '].strip

    def genera_json_bordero(self, path):
        logging.info("Generando jsin")
        file_name = f"{path}_Bordero{self.cabecera["waybill_number"].strip()}.json"
        with open(file_name, "wt") as file_cab:
            json.dump(self.bordero, file_cab, indent=3)