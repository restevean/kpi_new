from datetime import datetime as dt


class MensajeEstado:

    def __init__(self):
        self.contenido=""

    def header(self, action=None):
        m_string = '@@PH' + 'STAT512'
        m_string += ' '
        m_string += '0512'
        m_string += '  '
        m_string += '35'
        m_string += '  '
        m_string += '1'
        m_string += ' '
        m_string += '7'
        m_string += ' '
        m_string += '  ANEXA'
        m_string += '  GRUBERV'
        m_string += ' '
        m_string += "\n"
        if action == "w":
            return m_string
        self.contenido += m_string

    def header_arc(self, action=None):
        m_string = "01" # 'Record Type': linea[0:2],
        m_string += " " * 20    # 'Order Number': linea[2:22],
        m_string += " " * 8     # 'Order Date': linea[22:30],
        m_string += " " * 10    # 'Customer Sender Code': linea[30:40],
        m_string += " " * 20    # 'Tranport Document Number': linea[40:60],
        m_string += " " * 8     # 'Tranport Document Date': linea[60:68],
        m_string += " " * 10    # 'Warehouse ID': linea[68:78],
        m_string += " "         # 'Document Type': linea[78:79],
        m_string += " " * 20    # 'Customer Reference': linea[79:99],
        m_string += " " * 20    # 'Agent Reference': linea[99:119],
        m_string += " " * 20    # 'Trip Number': linea[119:139],
        m_string += " " * 20    # 'Agent Trip': linea[139:159],
        m_string += " " * 10    # 'EventCode': linea[159:169],
        m_string += " " * 35    # 'Event Description': linea[169:204],
        m_string += " " * 8     # 'Event Date': linea[204:212],
        m_string += " " * 4     # 'Event Time': linea[212:216],
        m_string += " " * 20    # 'EventPlaceID': linea[216:236],
        m_string += " " * 35    # 'Sender Company Name': linea[236:271],
        m_string += " " * 35    # 'Sender Address': linea[271:306],
        m_string += " " * 35    # 'Sender Place': linea[306:341],
        m_string += " " * 2     # 'Sender District': linea[341:343],
        m_string += " " * 5     # 'Sender ZipCode': linea[343:348],
        m_string += " " * 3     # 'Sender Country Code': linea[348:351],
        m_string += " " * 15    # 'Sender Contact': linea[351:366],
        m_string += " " * 50    # 'Sender Notes': linea[366:416],
        m_string += " " * 10    # 'ConsigneeID': linea[416:426],
        m_string += " " * 35    # 'Consignee Company Name': linea[426:461],
        m_string += " " * 35    # 'Consignee Address': linea[461:496],
        m_string += " " * 35    # 'Consignee Place': linea[496:531],
        m_string += " " * 2     # 'Consignee District': linea[531:533],
        m_string += " " * 5     # 'Consignee ZipCode': linea[533:538],
        m_string += " " * 3     # 'Consignee Country Code': linea[538:541],
        m_string += " " * 15    # 'Consignee Contact': linea[541:556],
        m_string += " " * 50    # 'Consignee Notes': linea[556:606],
        m_string += " " * 8     # 'Total GrossWeight': linea[606:614],
        m_string += " " * 8     # 'Total Volume': linea[614:622],
        m_string += " " * 4     # 'Quantity': linea[622:626],
        m_string += " " * 200   # 'Remark': linea[626:826],
                                # "Linea": []}
        if action == "w":
            return m_string
        self.contenido += m_string


    def header_q00(self, action=None):
        m_string = "Q00" + "100"
        m_string += "G01" # codeList
        m_string += self.rellenar(texto="GRUVR", n_total=35) #consignorId
        m_string += self.rellenar(texto="ANEXA", n_total=35)#consigneeId
        m_string += self.rellenar(texto="GRUVR", n_total=35)#causingPartyId
        m_string += "\n"
        if action == "w":
            return m_string
        self.contenido += m_string


    # TODO No entiendo porqué "000004"
    def z_control_record(self, records=None):
        hoy=dt.now()
        ahora_str=dt.strftime(hoy.now(),"%d%m%Y%H%M%S")
        m_string = "Z00" + (str(records).zfill(6) if records else "000004")
        m_string += ahora_str
        m_string += "\n"
        if records:
            return m_string
        self.contenido += m_string


    def cierre(self, action=None):
        if action == "w":
            return "@@PT"
        self.contenido += "@@PT"


    @staticmethod
    def rellenar(n_total=10, char_relleno=' ', texto='', lado_relleno='r'):
        return texto.ljust(n_total - len(texto), char_relleno) if lado_relleno == "l" else texto.rjust(n_total - len(texto),
                                                                                                  char_relleno)

    # TODO Se usa para algo?
    def devolver_mensaje(self):
        return self.contenido


    @staticmethod
    def leer_stat_arcese(fichero):

        resultado = []
        icab = -1

        with open(fichero, "rt") as f:
            for linea in f:
                if (linea[:2] == "01"):  # linea
                    cabecera = {
                        'Record Type': linea[0:2],
                        'Order Number': linea[2:22],
                        'Order Date': linea[22:30],
                        'Customer Sender Code': linea[30:40],
                        'Tranport Document Number': linea[40:60],
                        'Tranport Document Date': linea[60:68],
                        'Warehouse ID': linea[68:78],
                        'Document Type': linea[78:79],
                        'Customer Reference': linea[79:99],
                        'Agent Reference': linea[99:119],
                        'Trip Number': linea[119:139],
                        'Agent Trip': linea[139:159],
                        'EventCode': linea[159:169],
                        'Event Description': linea[169:204],
                        'Event Date': linea[204:212],
                        'Event Time': linea[212:216],
                        'EventPlaceID': linea[216:236],
                        'Sender Company Name': linea[236:271],
                        'Sender Address': linea[271:306],
                        'Sender Place': linea[306:341],
                        'Sender District': linea[341:343],
                        'Sender ZipCode': linea[343:348],
                        'Sender Country Code': linea[348:351],
                        'Sender Contact': linea[351:366],
                        'Sender Notes': linea[366:416],
                        'ConsigneeID': linea[416:426],
                        'Consignee Company Name': linea[426:461],
                        'Consignee Address': linea[461:496],
                        'Consignee Place': linea[496:531],
                        'Consignee District': linea[531:533],
                        'Consignee ZipCode': linea[533:538],
                        'Consignee Country Code': linea[538:541],
                        'Consignee Contact': linea[541:556],
                        'Consignee Notes': linea[556:606],
                        'Total GrossWeight': linea[606:614],
                        'Total Volume': linea[614:622],
                        'Quantity': linea[622:626],
                        'Remark': linea[626:826],
                        "Linea": []}
                    resultado.append(cabecera)

                    icab += 1

                if (linea[:2] == "02"):  # linea
                    lineaQ10 = {
                        'Record Type': linea[0:2],
                        'Order Number': linea[2:22],
                        'Order Date': linea[22:30],
                        'Customer Sender Code': linea[30:40],
                        'Tranport Document Number': linea[40:60],
                        'Tranport Document Date': linea[60:68],
                        'EventCode': linea[68:78],
                        'Event Date': linea[78:86],
                        'Event Time': linea[86:90],
                        'Barcode': linea[90:108]
                    }
                    resultado[icab]["Linea"].append(lineaQ10)
        return resultado

    def conversion_stat_arcese_anexa (self, estado =""):
        conversion_dict = {
            "TBD": "ANXE01",
            "COR": "ANXE05", 
            "CRI": "ANXE07", 
            "302": "ANXE11", 
            "VIA": "TRA0106",
            "VSC": "ANXE06", # SHIPMENT UNLOADED
            # "DESCARFAL": ("SMA", "Missing inbound"),
            # "DESCARFTOT": ("SMA", "Missing inbound"),
            # "DESCARKO": ("SMA", "Missing inbound"),
            "SMA": "DESCAROK", 
            # "ENT001": ("SMA", "Shipment scanned - no difference"),
            # "ENT004": ("SMA", "Shipment scanned - no difference"),
            # "ENT011": ("SMA", "Missing inbound"),
            # "LINTRK02": ("CRI", "Shipment delivered, with remarks"),
            "402": "SAL001", 
            "202": "TRA0081"
            # "TRA0102": ("402", "In delivery"),
            # "TRA0106": ("COR", "Shipment delivered"),
        }
        
        if estado in conversion_dict:
            return conversion_dict[estado]
        else:
            return "error"
    
    @staticmethod
    def leer_stat_gruber(fichero):
        """Leer el estado del fichero de Gruber"""
        resultado= {"Lineas": []}

        with open (fichero, "rt") as f:
            for fila in f:
                print(fila[:11])
                if fila[:11] == "@@PHSTAT512": #Mensaje tipo estado
                    resultado["header"] = fila #Por el momento no tiene valor desagregar la línea
                if fila[:3] == "Q00": #cabecera
                    resultado["Q00"] = {
                        'RecordQ00': fila[:3],
                        'Release version': fila[3:6],
                        'Code list (cooperation)': fila[6:9],
                        'Consignor ID of the consignments': fila[9:44],
                        'Consignee ID of the consignments': fila[44:79],
                        'Causing party ID': fila[79:114],
                        'Routing ID 1': fila[114:149],
                        'Routing ID 2': fila[149:184],
                    }
                if fila[:3] == "Q10": #linea
                    resultado["Lineas"].append(
                        {
                            'Record type ‘Q10’': fila[:3],
                            'Consignment number sending depot': fila[3:38],
                            'Consignment number receiving depot': fila[38:73],
                            'Pick-up order number': fila[73:108],
                            'Status code': fila[108:111],
                            'Date of event': fila[111:119],
                            'Time of event (HHMM)': fila[119:123],
                            'Consignment number delivering party': fila[123:158],
                            'Wait/Downtime in minutes': fila[158:162],
                            'Name of acknowledg-ing party': fila[162:197],
                            'Additional text': fila[197:267],
                            'Reference number': fila[267:279],
                            'Latitude (Lat)': fila[279:290],
                            'Longitude (Lon)': fila[290:301],
                        }
                    )
                if fila[:3] == "Q11": #linea
                    resultado["Lineas"].append(
                        {
                            'Record typeQ11': fila[:3],
                            'Additional text 1': fila[3:73],
                            'Additional text 2': fila[73:143],
                            'Additional text 3': fila[143:213],
                        }
                    )

                if fila[:3] == "Q30": #linea
                    resultado["Lineas"].append(
                        {
                            'Record type ‘Q30’': fila[:3],
                            'Reference qualifier 01': fila[3:6],
                            'Reference data 01': fila[6:41],
                            'Reference qualifier 02': fila[41:44],
                            'Reference data 02': fila[44:79],
                            'Reference qualifier 03': fila[79:82],
                            'Reference data 03': fila[82:117],
                            'Reference qualifier 04': fila[117:120],
                            'Reference data 04': fila[120:155],
                            'Reference qualifier 05': fila[155:158],
                            'Reference data 05': fila[158:193],
                        }
                    )
                    if fila[:4] == "@@PT": #linea
                        resultado["Lineas"].append(
                            {
                                'Record type@@PT': fila[:4],
                                'Empty record': fila[4:128],
                            }
                        )
        return resultado







