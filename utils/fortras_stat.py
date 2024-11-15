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
        else:
            self.contenido += m_string


    def header_q00(self, action=None):
        m_string = "Q00" + "100"
        m_string += "G01" # codeList
        m_string += self.rellenar(texto="GRUVR", ntotal=35) #consignorId
        m_string += self.rellenar(texto="ANEXA", ntotal=35)#consigneeId
        m_string += self.rellenar(texto="GRUVR", ntotal=35)#causingPartyId
        m_string += "\n"
        if action == "w":
            return m_string
        else:
            self.contenido += m_string


    def z_control_record(self, records=None):
        hoy=dt.now()
        ahora_str=dt.strftime(hoy.now(),"%d%m%Y%H%M%S")
        m_string = "Z00" + (str(records).zfill(6) if records else "000004")
        m_string += ahora_str
        m_string += "\n"
        if records:
            return m_string
        else:
            self.contenido += m_string


    def cierre(self, action=None):
        if action == "w":
            return "@@PT"
        else:
            self.contenido += "@@PT"


    @staticmethod
    def rellenar(ntotal=10, relleno=' ', texto='', ladorelleno='r'):
        return texto.ljust(ntotal - len(texto), relleno) if ladorelleno == "l" else texto.rjust(ntotal - len(texto),

                                                                                                relleno)
    # TODO Se usa para algo?
    def devolver_mensaje(self):
        return self.contenido


    @staticmethod
    def leer_stat_arcese(fichero):
        resultado= {"Lineas": []}

        with open (fichero, "rt") as f:
            for linea in f:
                print(linea[:11])
                if linea[:11] == "@@PHSTAT512": #Mensaje tipo estado
                    resultado["header"] = linea
                if linea[:3] == "Q00": #cabecera
                    resultado["Q00"] = {
                        "recordType": linea[:3],
                        "releaseVersion": linea[3:6],
                        "codeList": linea[6:9],
                        "consignorId": linea[9:44],
                        "consigneeId": linea[44:79],
                        "causingPartyId": linea[79:114],
                        "routingId1": linea[114:149],
                        "routingId2": linea[149:174],
                    }
                if linea[:3] == "Q10": #linea
                    resultado["Lineas"].append(
                        {
                            "recordType": linea[:3],
                            "consignmentNumberSendingDepot": linea[3:38],
                            "consignmentNumberReceivingDepot": linea[38:73],
                            "pickupOrderNumber": linea[73:108],
                            "statusCode": linea[108:111],
                            "dateOfEvent": linea[111:119],
                            "timeOfEvent": linea[119:123],
                            "consignmentNumberDeliveringParty": linea[123:158],
                            "waitDowntimeMinutes": linea[158:162],
                            "nameOfAcknowledgingParty": linea[162:197],
                            "additionalText": linea[197:267],
                            "referenceNumber": linea[267:279],
                        }
                    )
                if linea[:3] == "Q11": #linea
                    resultado["Lineas"].append(
                        {
                            "recordType": linea[:3],
                            "additionalText1": linea[3:73],
                            "additionalText2": linea[73:143],
                            "additionalText3": linea[143:213],
                        }
                    )

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

                # TODO Ojo! if fila[:3] == "Q30": se asignan claves repetidas ul diccionario
                if fila[:3] == "Q30": #linea
                    resultado["Lineas"].append(
                        {
                            'Record type ‘Q30’': fila[:3],
                            'Reference qualifier': fila[3:6],
                            'Reference data': fila[6:41],
                            'Reference qualifier': fila[41:44],
                            'Reference data': fila[44:79],
                            'Reference qualifier': fila[79:82],
                            'Reference data': fila[82:117],
                            'Reference qualifier': fila[117:120],
                            'Reference data': fila[120:155],
                            'Reference qualifier': fila[155:158],
                            'Reference data': fila[158:193],
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







