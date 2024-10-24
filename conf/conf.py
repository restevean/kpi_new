
import json

class Conf:
    
    def __init__(self) -> None:
        self.data={}
        try:
            file = open('./Conf.json')
            # returns JSON object as
            # a dictionary
            self.data = json.load(file)
            self.data["entorno"]=self.data["entorno"].lower()
            file.close()
        except Exception as e:
           
            self.data["entorno"]="no encontrado"
    
    def conf(self):
        return self.data
    