# utils/bordero-py
# TODO change name from bordero.py to bordero_arc.py

import json
import logging

logger = logging.getLogger(__name__)

class BorderoArcese:
    def __init__(self):
        self.bordero = {"partidas": []}
        # self.bordero["partidas"]["lineas"] =[]
        self.cabecera = {}
        self.linea = {}
        
    def cabecera_arcese(self, fila=''):
        cabecera = {
            'Record Type': fila[0:2],
            'Order Number': fila[2:22],
            'Order Creation Date': fila[22:34],
            'Customer Sender Code': fila[34:44],
            'Tranport Document Number': fila[44:64],
            'Tranport Document Date': fila[64:72],
            'Warehouse ID': fila[72:83],
            'Document Type': fila[83:84],
            'Shipment Type': fila[84:86],
            'Service Type': fila[86:88],
            'Line Code': fila[88:91],
            'Collection Date': fila[91:99],
            'Collection Time': fila[99:103],
            'Delivery Date': fila[103:111],
            'Delivery Time': fila[111:115],
            'SenderID': fila[115:125],
            'Sender Company Name': fila[125:195],
            'Sender Address': fila[195:230],
            'Sender Place': fila[230:265],
            'Sender District': fila[265:267],
            'Sender ZipCode': fila[267:272],
            'Sender Country Code': fila[272:275],
            'Sender Contact': fila[275:290],
            'Sender Notes': fila[290:340],
            'ConsigneeID': fila[340:350],
            'Consignee Company Name': fila[350:420],
            'Consignee Address': fila[420:455],
            'Consignee Place': fila[455:490],
            'Consignee District': fila[490:492],
            'Consignee ZipCode': fila[492:497],
            'Consignee Country Code': fila[497:500],
            'Consignee Contact': fila[500:515],
            'Consignee Notes': fila[515:565],
            'OriginID': fila[565:575],
            'Origin Company Name': fila[575:645],
            'Origin Address': fila[645:680],
            'Origin Place': fila[680:715],
            'Origin District': fila[715:717],
            'Origin ZipCode': fila[717:722],
            'Origin Country Code': fila[722:725],
            'Origin Contact': fila[725:740],
            'Origin Notes': fila[740:790],
            'DestinationID': fila[790:800],
            'Destination Company Name': fila[800:870],
            'Destination Address': fila[870:905],
            'Destination Place': fila[905:940],
            'Destination District': fila[940:942],
            'Destination ZipCode': fila[942:947],
            'Destination Country Code': fila[947:950],
            'Destination Contact': fila[950:965],
            'Destination Notes': fila[965:1015],
            'Customer Code': fila[1015:1023],
            'Order Full Number': fila[1023:1058],
            'Trip Reference Number': fila[1058:1093],
            'Pre/Additional Order Number': fila[1093:1103],
            'CashOnDelivery': fila[1103:1104],
            'PaymentTypeCode': fila[1104:1107],
            'Currency': fila[1107:1110],
            'Amount': fila[1110:1121],
            'Insured Currency Code': fila[1121:1124],
            'Insured Amount': fila[1124:1135],
            'Total GrossWeight': fila[1135:1143],
            'Total Volume': fila[1143:1151],
            'Quantity': fila[1151:1155],
            'Package Number Start': fila[1155:1160],
            'Package Number End': fila[1160:1165],
            'Sequence': fila[1165:1166],
            'DeliveryClosingDay': fila[1166:1168],
            'Equipment Type': fila[1168:1171],
            'PhoneBooking Required': fila[1171:1172],
            'Limited Traffic Zone': fila[1172:1173],
            'Floor Delivery': fila[1173:1174],
            'MandatoryDeliveryDate': fila[1174:1186],
            'Additional Requirements': fila[1186:1221],
            'Remark': fila[1221:1421],
            "lineas": []
                   }
        self.bordero["partidas"].append(cabecera)
        self.cabecera=cabecera
        return cabecera
    
    def linea_arcese(self, fila=''):
        linea= {
            'Record Type': fila[0:2],
            'Order Number': fila[2:22],
            'Customer Sender Code': fila[22:32],
            'Transport Document Number': fila[32:52],
            'Line Code': fila[52:55],
            'Package Code': fila[55:75],
            'Package Number': fila[75:125],
            'Package Id': fila[125:132],
            'Barcode': fila[132:150],
            'Goods Type': fila[150:170],
            'Package GrossWeight': fila[170:178],
            'Package Volume': fila[178:186],
            'Package Quantity': fila[186:190],
            'Length': fila[190:198],
            'Width': fila[198:206],
            'Height': fila[206:214],
            'ADR code': fila[214:219],
            'ADR class': fila[219:224],
            'ADR packaging group': fila[224:229],
            'ADR Quantity': fila[229:234]}

        for i in range(len(self.bordero["partidas"])):
            if self.bordero["partidas"][i]["Order Number"]==linea['Order Number']:
                 self.bordero["partidas"][i]["lineas"].append(linea)
        self.linea=linea
        return linea
    
    def expediente_ref_cor (self):
        return self.cabecera["Trip Reference Number"].strip()

    def partida_ref_cor(self):
        return self.cabecera['Order Number'].strip
    
    # def imprimir_bordero(self, path):
    #     print("imprimir bordeero")
    #     file_cab = open(path+"_Bordero"+self.cabecera['Trip Reference Number'].strip()+".json","wt")
    #     file_cab.write(json.dumps(self.bordero, indent=3))
    #     file_cab.close()

    def genera_json_bordero(self, path):
        logging.info("Imprimiendo bordero")
        file_name = f"{path}_Bordero{self.cabecera['Trip Reference Number'].strip()}.json"
        with open(file_name, "wt") as file_cab:
            json.dump(self.bordero, file_cab, indent=3)

    def imprimir_cabecera(self, path=''):
        file_cab = open(path+"_CaB"+self.cabecera['Order Number'].strip()+".json","wt")
        file_cab.write(json.dumps(self.cabecera, indent=3))
        file_cab.close()
    
    def imprimir_etiqueta(self, path=''):
        file = open(path+"_Linea"+self.cabecera['Order Number'].strip()+"_"+self.linea["Barcode"]+".json","wt")
        file.write(json.dumps(self.cabecera, indent=3))
        file.close()
                 
    