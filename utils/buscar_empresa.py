from bmaster_api import BmasterApi


def busca_destinatario(self,rsocial='', codpostal='', cpais=''):
    largo=len(rsocial)+1
    bm=BmasterApi()
    # bm.login()
    resp={}
    resp["ient"]=0
    while largo>len(rsocial)//2:

        largo=largo-1
        # query="select top 10 * from aetent where dnomcom like '"+rsocial[:largo]+"%'"
        querycount="select count(1) as cuenta from aetent where dnomcom like '"+rsocial[:largo]+"%'"
        query=" SELECT [aetent].[iemp],ient,[cemp],[dnomfis],[ncif],ccodpos,[aebcodpos].dpob,[idirfis],[aebdir].ipai,cpai "
        query+=" FROM [Anexa].[dbo].[aetent] "
        query+= " inner join aetemp on aetent.iemp=aetemp.iemp"
        query+="  inner join [aebdir] on aebdir.idir = aetemp.idirfis"
        query+="              inner join [dbo].[aebcodpos] on [aebcodpos].icodpos = aebdir.icodpos "
        query+="               inner join [dbo].[aebpai] on [aebpai].ipai=[aebdir].ipai "
        query+=" where dnomfis like  '"+rsocial[:largo]+"%' and cpai='"+cpais+"'" +" and not cent like 'ENT%' AND NOT dnomcom like '%BORRAR%'"
        query_codpost =query+"and ccodpos ='"+codpostal+"'"
        # print(query)

        respuesta_query=bm.consulta_(query)
        if respuesta_query["cod_error"]!=200 :
            return resp
        if len(respuesta_query["contenido"])==1:
            print("\n\n\n Devuelve "+str(respuesta_query["contenido"][0]["ient"])+"\n\n\n")
            return respuesta_query["contenido"][0]

        elif len(respuesta_query["contenido"])>1:
        # respuesta_id_codpost=bm.consulta_(query=query_codpost)
            print("Respuesta Query con denominación de origen")
            for entidad in respuesta_query["contenido"]:
                print(str(entidad['ient'])+" ---> "+entidad['dnomfis'] +" ---> "+str(entidad['cemp']) + " Codpostal: "+str(entidad['ccodpos']))
                if entidad['ccodpos']== codpostal.replace(" ",""):
                    return entidad
    print ("BUSCA DESTINATIARIO "+rsocial +" Codigo postal: "+str(codpostal)+": No lo encuentro")
    return resp