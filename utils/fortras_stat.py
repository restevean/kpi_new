from datetime import datetime as dt


class MensajeEstado:

    def __init__(self):
        self.contenido=""

    def header (self):
        self.contenido+='@@PH'
        self.contenido+='STAT512'
        self.contenido+=' '
        self.contenido+='0512'
        self.contenido+='  '
        self.contenido+='35'
        self.contenido+='  '
        self.contenido+='1'
        self.contenido+=' '
        self.contenido+='7'
        self.contenido+=' '
        self.contenido+='  ANEXA'
        self.contenido+='  GRUBERV'
        self.contenido+=' '
        self.contenido+="\n"
    
    def header_q00(self):
        self.contenido+="Q00" #recordType
        self.contenido+="100" #releaseVersion
        self.contenido+="G01"#codeList
        self.contenido+=self.rellenar(texto="GRUVR", ntotal=35) #consignorId
        self.contenido+=self.rellenar(texto="ANEXA", ntotal=35)#consigneeId
        self.contenido+=self.rellenar(texto="GRUVR", ntotal=35)#causingPartyId
        #routingId1
        #routingId2
        self.contenido+="\n"
        
    def consigment_level(self, status='000'):
        fechamov=dt.strptime(self.partida["fmov"],"%Y-%m-%dT%H:%M:%S")
        horamov=dt.strptime(self.partida["hmov"],"%H:%M:%S")
        status_edi={"Entrada":"000", "Salida":"SPC"}
        self.contenido+='Q10' 
        self.contenido+=self.rellenar(ntotal=35,texto=self.partida["nrefcor"]) # consignmentNumberSendingDepot
        self.contenido+=self.rellenar(ntotal=35,texto="") # consignmentNumberSendingDepot --> nrefcor
        self.contenido+=self.rellenar(ntotal=35,texto="") # pickupOrderNumber
        self.contenido+=self.rellenar(ntotal=3,texto=status_edi[status]) # status code
        self.contenido+=dt.strftime(fechamov,"%d%m%Y") # dateOfEvent
        self.contenido+=dt.strftime(horamov,"%H%M")
        self.contenido+=self.rellenar(ntotal=35,texto="") # consignmentNumberDeliveringParty
        self.contenido+=self.rellenar(ntotal=4,texto="") # waitDowntimeMinutes
        self.contenido+=self.rellenar(ntotal=35,texto="") # nameOfAcknowledgingParty
        self.contenido+=self.rellenar(ntotal=70,texto="") # additionalText
        self.contenido+=self.rellenar(ntotal=12,texto="") # referenceNumber
        self.contenido+="\n"
    
    def z_control_record(self):
        """Para el control del archivo"""
        hoy=dt.now()
        ahora_str=dt.strftime(hoy.now(),"%d%m%Y%H%M%S")
        self.contenido+="Z00" #Field reference
        self.contenido+= "000004" #Length 000004
        self.contenido+=ahora_str
        self.contenido+="\n"

    def cierre(self):
        """@@PT"""
        self.contenido+="@@PT"
        
    @staticmethod
    def rellenar(ntotal=10, relleno=' ', texto='', ladorelleno='r'):
        """Devuelve el texto relleno. Ladorelleno: r - pone los caracteres de relleno a la izq. l - pone el relleno a la izq"""
        if ladorelleno=="l":
            return texto.ljust(ntotal-len(texto),relleno)
        else: #ladorelleno=="r":
            return texto.rjust(ntotal-len(texto),relleno)    
    
    def devolver_mensaje(self):
        return self.contenido   
    
    @staticmethod
    def leer_stat_arcese(fichero):
        resultado= {"Lineas": []}

        with open (fichero, "rt") as f:
            for linea in f:
                print(linea[:11])
                if linea[:11]== "@@PHSTAT512": #Mensaje tipo estado
                    resultado["header"]=linea
                if linea[:3]== "Q00": #cabecera
                    resultado["Q00"]={
                        "recordType": linea[0:3],
                        "releaseVersion": linea[3:6],
                        "codeList": linea[6:9],
                        "consignorId": linea[9:44],
                        "consigneeId": linea[44:79],
                        "causingPartyId": linea[79:114],
                        "routingId1": linea[114:149],
                        "routingId2": linea[149:174]}
                if linea[:3]== "Q10": #linea
                    resultado["Lineas"].append(
                        {
                        "recordType":linea[0:3],
                        "consignmentNumberSendingDepot":linea[3:38],
                        "consignmentNumberReceivingDepot":linea[38:73],
                        "pickupOrderNumber":linea[73:108],
                        "statusCode":linea[108:111],
                        "dateOfEvent":linea[111:119],
                        "timeOfEvent":linea[119:123],
                        "consignmentNumberDeliveringParty":linea[123:158],
                        "waitDowntimeMinutes":linea[158:162],
                        "nameOfAcknowledgingParty":linea[162:197],
                        "additionalText":linea[197:267],
                        "referenceNumber":linea[267:279]

                        }
                    )
                if linea[:3]== "Q11": #linea
                    resultado["Lineas"].append(
                    {
                    "recordType":linea[0:3],
                    "additionalText1":linea[3:73],
                    "additionalText2":linea[73:143],
                    "additionalText3":linea[143:213]
                    }
                )
    @staticmethod
    def leer_stat_gruber(fichero):
        """Leer el estado del fichero de Gruber"""
        resultado= {"Lineas": []}

        with open (fichero, "rt") as f:
            for fila in f:
                print(fila[:11])
                if fila[:11]== "@@PHSTAT512": #Mensaje tipo estado
                    resultado["header"]=fila #Por el momento no tiene valor desagregar la línea
                if fila[:3]== "Q00": #cabecera
                    resultado["Q00"]={  
                                    'RecordQ00':fila[0:3],
                                    'Release version':fila[3:6],
                                    'Code list (cooperation)':fila[6:9],
                                    'Consignor ID of the consignments':fila[9:44],
                                    'Consignee ID of the consignments':fila[44:79],
                                    'Causing party ID':fila[79:114],
                                    'Routing ID 1':fila[114:149],
                                    'Routing ID 2':fila[149:184]
                                     }
                if fila[:3]== "Q10": #linea
                    resultado["Lineas"].append(
                        {
                        'Record type ‘Q10’':fila[0:3],
                        'Consignment number sending depot':fila[3:38],
                        'Consignment number receiving depot':fila[38:73],
                        'Pick-up order number':fila[73:108],
                        'Status code':fila[108:111],
                        'Date of event':fila[111:119],
                        'Time of event (HHMM)':fila[119:123],
                        'Consignment number delivering party':fila[123:158],
                        'Wait/Downtime in minutes':fila[158:162],
                        'Name of acknowledg-ing party':fila[162:197],
                        'Additional text':fila[197:267],
                        'Reference number':fila[267:279],
                        'Latitude (Lat)':fila[279:290],
                        'Longitude (Lon)':fila[290:301]
                        }
                    )
                if fila[:3]== "Q11": #linea
                    resultado["Lineas"].append(
                        {
                        'Record typeQ11':fila[0:3],
                        'Additional text 1':fila[3:73],
                        'Additional text 2':fila[73:143],
                        'Additional text 3':fila[143:213]
                        }
                    )                
                if fila[:3]== "Q30": #linea
                    resultado["Lineas"].append(
                        {
                        'Record type ‘Q30’':fila[0:3],
                        'Reference qualifier':fila[3:6],
                        'Reference data':fila[6:41],
                        'Reference qualifier':fila[41:44],
                        'Reference data':fila[44:79],
                        'Reference qualifier':fila[79:82],
                        'Reference data':fila[82:117],
                        'Reference qualifier':fila[117:120],
                        'Reference data':fila[120:155],
                        'Reference qualifier':fila[155:158],
                        'Reference data':fila[158:193]
                        }
                    )
                    if fila[:4]== "@@PT": #linea
                        resultado["Lineas"].append(
                            {
                        'Record type@@PT':fila[0:4],
                        'Empty record':fila[4:128]
                            }
                    )    
        return resultado