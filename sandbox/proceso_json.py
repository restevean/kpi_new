# sandbox/proceso_json.py

import json
from pathlib import Path

# Cargar el JSON desde un archivo
ruta_json = Path("resultados.json")
with ruta_json.open("r", encoding="utf-8") as json_file:
    data = json.load(json_file)

# Aquí almacenaremos los resultados
resultado = {
    "partidas": []
}

import json
from pathlib import Path


import json
from pathlib import Path

def generar_partidas_y_barcodes(json_path: str, expediente: dict) -> dict:

    with Path(json_path).open("r", encoding="utf-8") as f:
        data = json.load(f)

    partidas_list = []
    barcodes_list = []

    for p in data.get("partidas", []):
        # Obtener registros clave
        b00_shp = p.get("B00-SHP", {})
        b00_con = p.get("B00_CON", {})
        g00 = p.get("G00", {})

        # Extraer datos relevantes de G00
        consignment_number_sending_depot = g00.get("consignment_number_sending_depot", "").strip()
        total_gross_weight_str = g00.get("actual_consignment_gross_weight_in_kg", "0").strip()
        total_volume_str = g00.get("cubic_meters", "0").strip()
        insured_amount_str = g00.get("goods_value", "0").strip()

        total_gross_weight = float(total_gross_weight_str) / 100
        total_volume = float(total_volume_str) / 100
        insured_amount = float(insured_amount_str) / 100

        # Obtener D00 (num_err_of) del primer bulto
        bultos = p.get("bultos", [])
        d00_num_err_of = None
        if bultos:
            primer_bulto = bultos[0]
            d00 = primer_bulto.get("D00", {})
            d00_num_err_of = d00.get("num_err_of", "").strip()

        # Construir la partida
        partida_dict = {
            "division": "VLC",
            "estado": "FBLE",
            "expediente": expediente["cexp"],
            "trafico": "TI",
            "tipotrafico": "TER",
            "flujo": "I",
            "refcliente": expediente["ientrefcli"],
            "refcorresponsal": consignment_number_sending_depot,
            "tipocarga": "C",
            "puertoorigen": "TIRARCCAM",
            "puertodestino": "TIRANERIB",
            "paisorigen": b00_shp.get("country_code", "").strip(),
            "paisdestino": b00_con.get("country_code", "").strip(),
            "portes": "s",
            "incoterm": "XXX",
            "servicio": "NOR",
            "bultos": d00_num_err_of,
            "tipobultos": "PX",
            "pesobruto": total_gross_weight,
            "pesoneto": total_gross_weight,
            "pesotasable": total_gross_weight,
            "volumen": total_volume,
            "seguro": "S",
            "valorasegurable": insured_amount,
            "divisa": "EUR",
            "divisavinculacion": "EUR",
            "tipovinculacion": "SV",
            "aduanas": "N",
            "gdp": "N",
            "almacenlogico": "CROS",
            "generarentradaalmacen": False,
            "generaralbarandistribucion": False,
            "tipotransportenacional": "S",
            "tipotransporteextranjero": "S",
            "fechaprevistasalida": expediente["fpresal"],
            "fechasalida": expediente["fsal"],
            "fechaprevistallegada": expediente["fprelle"],
            "fechallegada": expediente["flle"],
        }

        partidas_list.append(partida_dict)

        # Extraer todos los códigos de barras (F00) de esta partida
        for bulto in bultos:
            barcodes_in_bulto = bulto.get("barcodes", [])
            for barcode_element in barcodes_in_bulto:
                f00 = barcode_element.get("F00", {})
                codigo_barras = f00.get("barcode", "").strip()
                if codigo_barras:
                    barcodes_list.append({
                        "partida": consignment_number_sending_depot,
                        "codigobarras": codigo_barras,
                        "altura": 0.0,
                        "ancho": 0.0,
                        "largo": 0.0
                    })

    return {
        "partidas": partidas_list,
        "barcodes": barcodes_list
    }


if __name__ == "__main__":
    expediente_data = {
        "cexp": "CE12345",
        "ientrefcli": "ENT56789",
        "fpresal": "2024-01-17T22:57:24.737Z",
        "fsal": "2024-01-17T22:57:24.737Z",
        "fprelle": "2024-01-17T22:57:24.737Z",
        "flle": "2024-01-17T22:57:24.737Z"
    }
    result = generar_partidas_y_barcodes("resultados.json", expediente_data)
    print(json.dumps(result, indent=4, ensure_ascii=False))
