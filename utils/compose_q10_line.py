# TODO Include this function as a method in fortras
def compose_q10_line(**kwargs):
    fields = {
        "record_type": ("Q10", 3),
        "consignment_number_sending_depot": ("", 35),
        "consignment_number_receiving_depot": ("", 35),
        "Pick-up_order_number": ("", 35),
        "status_code": ("", 3),
        "date_of_event": ("", 8),  # Formato "YYYYMMDD"
        "time_of_event": ("", 4),  # Formato "HHMM"
        "consignment_number_delivering_party": ("", 35),
        "wait_downtime_in_minutes": ("", 4),
        "name_of_acknowledging_party": ("", 35),
        "additional_text": ("", 70),
        "reference_number": (" " * 12, 12),  # 12 caracteres de espacio en blanco
        "latitude": ("", 11),
        "longitude": ("", 11),
    }

    result = ""
    for field, (default, width) in fields.items():
        value = kwargs.get(field, default)

        if field == "date_of_event" and isinstance(value, str) and len(value) >= 10:
            value = value[:10].replace("-", "")  # Convierte "YYYY-MM-DD" en "YYYYMMDD"
        elif field == "time_of_event" and isinstance(value, str) and len(value) >= 5:
            value = value.replace(":", "")[:4]  # Convierte "HH:MM" en "HHMM"
        value_str = str(value).ljust(width)[:width]
        result += value_str
    return result + "\n"  # Añadir un salto de línea al final


def compose_arc_header(**kwargs):
    header = {
        'record_type': ("01", 2), # linea[0:2],
        'order_number': ("", 20),  # linea[2:22],
        'order_date': ("", 8),  # linea[22:30],
        'customer_sender_code': ("", 10),  # linea[30:40],
        'Transport Document Number': ("", 20),  # 'Tranport Document Number': linea[40:60],
        'Transport Document Date': ("", 8),  # 'Tranport Document Date': linea[60:68],
        'Warehouse ID': ("", 10),  # linea[68:78],
        'document_type': ("", 1),  # linea[78:79],
        'Customer Reference': ("", 20),  # linea[79:99],
        'Agent Reference': ("", 20),  # linea[99:119],
        'Trip Number': ("", 20),  # linea[119:139],
        'Agent Trip': ("", 20), # linea[139:159],
        'event_code': ("", 10),  # linea[159:169],
        'event_description': ("", 35),  # linea[169:204],
        'event_date': ("", 8), # linea[204:212],
        'event_time': ("", 4), # linea[212:216],
        'EventPlaceID': ("", 20),  # linea[216:236],
        "Sender Company Name": ("", 35),  # linea[236:271]
        "Sender Address": ("", 35),  # linea[271:306]
        "Sender Place": ("", 35),  # linea[306:341]
        "Sender District": ("", 2),  # linea[341:343]
        "Sender Code": ("", 5),  # linea[343:348]
        "Sender Country Code": ("", 3),  # linea[348:351]
        "Sender Contact": ("", 15),  # linea[351:366]
        "Sender Notes": ("", 50),  # linea[366:416]
        "ConsigneeID' ID": ("", 10),  # linea[416:426]
        "Consignee Company Name": ("", 35),  # linea[426:461]
        "Consignee Address": ("", 35),  # linea[461:496]
        "Consignee Place": ("", 35),  # linea[496:531]
        "Consignee District": ("", 2),  # linea[531:533]
        "Consignee Code": ("", 5),  # linea[533:538]
        "Consignee Country Code": ("", 3),  # linea[538:541]
        "Consignee Contact": ("", 15),  # linea[541:556]
        "Consignee Notes": ("", 50),  # linea[556:606]
        "Total Weight": ("", 8),  # linea[606:614]
        "Total Volume": ("", 8),  # linea[614:622]
        "Quantity' Quantity": ("", 4),  # linea[622:626]
        "Remark' Remark": ("", 200),  # linea[626:826]
        }

    n_string = ""
    for field, (default, width) in header.items():
        value = kwargs.get(field, default)

        if field == "event_date": # and isinstance(value, str) and len(value) >= 10:
            value = value[:10].replace("-", "")  # Convierte "YYYY-MM-DD" en "YYYYMMDD"
        elif field == "event_time": # and isinstance(value, str) and len(value) >= 5:
            value = value.replace(":", "")[:4]  # Convierte "HH:MM" en "HHMM"
        value_str = str(value).ljust(width)[:width]
        n_string += value_str
    return n_string+"\n"


if __name__ == "__main__":
    # Ejemplo de uso
    # line = compose_q10_line(
    #     status_code="A",
    #     date_of_event="2024-10-01T00:00:00",
    #     time_of_event="11:38",
    # )

    line = compose_arc_header(
         event_code="A",
         event_description="DELIVERED",
         event_date="2024-10-01T00:00:00",
         event_time="11:38",
    )

    print(repr(line))  # Muestra la cadena con un salto de línea al final


