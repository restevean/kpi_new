import json
import logging
from utils.logger_config import setup_logger
from pathlib import Path
from typing import Dict, Any


setup_logger()
logger = logging.getLogger(__name__)

base_dir = Path(__file__).resolve().parent


class BorXBT:
    def __init__(self):
        self.expediente: Dict[str, Any] = {
            "level_1": {},
            "partidas": [],
            "J00": {},
            "Z00": {}
        }
        self.current_partida = None     # Variable temporal para la partida actual
        self.current_bulto = None       # Variable temporal para el bulto actual
        self.b00_counter = 0            # Contador para líneas B00

    def procesar_linea(self, linea):
        if linea.startswith("@@P"):
            return                      # No procesar líneas que empiezan por @@P

        linea = self.rellena_linea(linea)
        clave = linea[:3]
        metodo = {
            "A00": self.level_1_xbs,
            "A10": self.level_1_xbs,
            "B00": self.level_2_xbs,
            "B10": self.level_2_xbs,
            "G00": self.level_2_xbs,
            "H00": self.level_2_xbs,
            "H10": self.level_2_xbs,
            "I00": self.level_2_xbs,
            "D00": self.level_3_xbs,
            "D10": self.level_4_xbs,
            "E00": self.level_4_xbs,
            "F00": self.level_4_xbs,
            "J00": self.level_1_totales,
            "Z00": self.level_1_totales,
        }.get(clave, None)

        if metodo:
            metodo(linea)
        else:
            logging.error(f"Línea con clave '{clave}' no reconocida: {linea}")

    def level_1_xbs(self, fila):
        if fila[:3] == "A00":
            self.expediente["level_1"]["A00"] = {
                "record_type_a00": fila[:3],
                "data_type_qualifier": fila[3:6],
                "release_version": fila[6:9],
                "waybill_number": fila[9:44],
                "waybill_date": fila[44:52],
                "transport_type": fila[52:55],
                "product": fila[55:58],
                "code_list_(cooperation)": fila[58:61],
                "currency": fila[61:64],
                "identification_of_waybill_consignor": fila[64:99],
                "identification_of_waybill_consignee": fila[99:134],
                "freight_operator": fila[134:169],
                "freight_operator_country": fila[169:172],
                "freight_operator_postcode": fila[172:181],
                "freight_operator_town": fila[181:216],
                "vehicle_license_number_1": fila[216:251],
                "vehicle_license_number_2": fila[251:286],
                "routing_id_1": fila[286:321],
                "routing_id_2": fila[321:356],
            }
        elif fila[:3] == "A10":
            self.expediente["level_1"]["A10"] = {
                "record_type_a10": fila[:3],
                "loading_units_no_1": fila[3:38],
                "lead_seal_number_1_for_lu_1": fila[38:73],
                "lead_seal_number_2_for_lu_1": fila[73:108],
                "lead_seal_number_3_for_lu_1": fila[108:143],
                "lead_seal_number_4_for_lu_1": fila[143:178],
            }

    def level_1_totales(self, fila):
        if fila[:3] == "J00":
            self.expediente["J00"] = {
                "record_type_j00": fila[:3],
                "total_number_of_consignments": fila[3:6],
                "total_number_of_packages": fila[6:12],
                "actual_gross_weight_in_kg": fila[12:21],
                "number_of_box_pallets": fila[21:25],
                "number_of_euro_flat_pallets": fila[25:29],
                "number_of_additional_loading_tools_flat": fila[29:33],
                "number_of_additional_loading_tools_box": fila[33:37],
                "totals_taxable_from100": fila[37:48],
                "totals_not_taxable_from100": fila[48:59],
                "totals_of_product_value_cod_from_100": fila[59:70],
                "totals_of_customs_from_100": fila[70:81],
                "totals_of_im": fila[81:92],
                "totals_of_value_added_tax": fila[92:103],
            }
        elif fila[:3] == "Z00":
            self.expediente["Z00"] = {
                "record_type_z00": fila[0:3],
                "total_number_of_data_records": fila[3:9],
                "date_of_creation": fila[9:17],
                "time_of_creation": fila[17:23],
            }

    def level_2_xbs(self, fila):
        if fila[:3] == "B00":
            self.b00_counter += 1
            if self.b00_counter == 1:
                self.current_partida = {
                    "B00-SHP": {
                        "record_type_b00": fila[0:3],
                        "sequential_waybill_item": fila[3:6],
                        "qualifier_(address_type)": fila[6:9],
                        "name_1": fila[9:44],
                        "street_and_street_number": fila[44:79],
                        "country_code": fila[79:82],
                        "postcode": fila[82:91],
                        "place": fila[91:126],
                    }
                }
            elif self.b00_counter == 2:
                self.current_partida["B00-CON"] = {
                    "record_type_b00": fila[0:3],
                    "sequential_waybill_item": fila[3:6],
                    "qualifier_(address_type)": fila[6:9],
                    "name_1": fila[9:44],
                    "street_and_street_number": fila[44:79],
                    "country_code": fila[79:82],
                    "postcode": fila[82:91],
                    "place": fila[91:126],
                }
                self.current_partida["bultos"] = []  # Inicializa la lista bultos
                self.expediente["partidas"].append(self.current_partida)
                self.b00_counter = 0  # Reinicia el contador para la siguiente partida
        elif fila[:3] in ("B10", "G00", "H00", "H10", "I00"):
            if self.current_partida is not None:
                self.current_partida[fila[:3]] = self._procesar_datos_extra(fila)

    def level_3_xbs(self, fila):
        if fila[:3] == "D00":
            self.current_bulto = {
                "D00": {
                    "record_type_d00": fila[0:3],
                    "sequential_waybill_item": fila[3:6],
                    "consignment": fila[6:9],
                    "num_err_of": fila[9:13],
                },
                "barcodes": []
            }
            if "bultos" in self.current_partida:
                self.current_partida["bultos"].append(self.current_bulto)

    def level_4_xbs(self, fila):
        clave = fila[:3]
        if clave in ("D10", "E00", "F00") and self.current_bulto is not None:
            self.current_bulto["barcodes"].append({
                clave: {
                    # "record_type": fila[0:3],
                    # "sequential_waybill_item": fila[3:6],
                    # "product_number": fila[10:24],
                    "record_type_f00": fila[0:3],
                    "sequential_waybill_item": fila[3:6],
                    "consignment_position": fila[6:9],
                    "barcode": fila[9:44],
                    "reference_qualifier": fila[44:47],
                    "reference_number": fila[47:82],
                    "reference_qualifier_1": fila[82:85],
                    "reference_number_1": fila[85:120],
                    "reference_qualifier_2": fila[120:123],
                    "reference_number_2": fila[123:158],
                    "reference_qualifier_3": fila[158:161],
                    "reference_number_3": fila[161:196],
                    "reference_qualifier_4": fila[196:199],
                    "reference_number_4": fila[199:234],
                }
            })

    def exportar_json(self, path):
        with open(path, 'w', encoding='utf-8') as file:
            json.dump(self.expediente, file, ensure_ascii=False, indent=4)

    def rellena_linea(self, linea):
        return linea.ljust(400)

    def _procesar_datos_extra(self, fila):
        clave = fila[:3]
        if clave == "G00":
            return {
                "record_type_g00": fila[0:3],
                "sequential_waybill_item": fila[3:6],
                "consignment_number_sending_depot": fila[6:41],
                "actual_consignment_gross_weight_in_kg": fila[41:50],
                "delivery_term": fila[50:53],
                "direct_delivery_yn": fila[53:54],
                "pickup_date": fila[54:62],
                "pickup_time_from": fila[62:66],
                "pickup_time_to": fila[66:70],
                "logistics_model": fila[70:76],
                "consignment_number_for_receiving_depot": fila[76:111],
                "consignor_id_original_waybill": fila[111:146],
                "consignee_id_original_waybill": fila[146:181],
                "material_group": fila[181:184],
                "goods_value": fila[184:195],
                "currency_of_goods_value": fila[195:198],
                "chargeable_consignment_weight_in_kg": fila[198:207],
                "cubic_meters": fila[207:212],
                "loading_meters": fila[212:215],
                "number_of_pallet_locations": fila[215:219],
                "number_of_additional_loading_tools_1": fila[219:221],
                "packaging_type_for_additional_loading_tools_1": fila[221:224],
                "number_of_additional_loading_tools_2": fila[224:226],
                "packaging_type_for_additional_loading_tools_2": fila[226:229],
            }
        elif clave == "H00":
            return {
                "record_type_h00": fila[0:3],
                "sequential_waybill_item": fila[3:6],
                "text_code_1": fila[6:9],
                "additional_text_1": fila[9:44],
                "text_code_2": fila[44:47],
                "additional_text_2": fila[47:82],
                "text_code_3": fila[82:85],
                "additional_text_3": fila[85:120],
                "text_code_4": fila[120:123],
                "additional_text_4": fila[123:158],
                "text_code_5": fila[158:161],
                "additional_text_5": fila[161:196],
                "text_code_6": fila[196:199],
                "additional_text_6": fila[199:234],
            }
        elif clave == "H10":
            return {
                "record_type_h10": fila[:3],
                "sequential_waybill_item": fila[3:6],
                "qualifier_for_text_usage_1": fila[6:9],
                "any_text_1": fila[9:79],
                "qualifier_for_text_usage_2": fila[79:82],
                "any_text_2": fila[82:152],
                "qualifier_for_text_usage_3": fila[152:155],
                "any_text_3": fila[155:225],
            }
        elif clave == "I00":
            return {
                "record_type_i00": fila[0:3],
                "se_uential": fila[3:6],
                "service_1": fila[6:9],
                "tax_code": fila[9:10],
                "amount_1": fila[10:20],
                "service_2": fila[20:22],
                "tax_code_2": fila[22:23],
                "amount_2": fila[23:32],
                "service_3": fila[32:35],
                "tax_code_3": fila[35:36],
                "amount_3": fila[36:45],
                "service_4": fila[45:48],
                "tax_code_4": fila[48:49],
                "amount_4": fila[49:58],
                "service_5": fila[58:61],
                "tax_code_5": fila[61:62],
                "amount_5": fila[62:71],
                "service_6": fila[71:74],
                "tax_code_6": fila[74:75],
                "amount_6": fila[76:84],
                "service_7": fila[84:87],
                "tax_code_7": fila[87:88],
                "amount_7": fila[88:97],
                "service_8": fila[97:100],
                "tax_code_8": fila[100:101],
                "amount_8": fila[101:110],
                "service_9": fila[110:113],
                "tax_code_9": fila[113:114],
                "amount_9": fila[114:123],
                "service_10": fila[124:126],
                "tax_code_10": fila[128:127],
                "amount_10": fila[127:136],
            }
        return {}




if __name__ == "__main__":
    bor = BorXBT()
    with open(base_dir / "bor_xbs.txt", "r", encoding="utf-8") as archivo:
        for linea in archivo:
            bor.procesar_linea(linea)
    bor.exportar_json("bor_xbs.json")
