# utils/buscar_empresa.py

import logging
from utils.bmaster_api import BmasterApi


logger = logging.getLogger(__name__)


def busca_destinatario(rsocial='', codpostal='', cpais=''):
    largo = len(rsocial)

    # Ojo, instanciamos a bmasterApi
    bm = BmasterApi()
    respuesta_default = {"ient": 0}
    base_query = (
        "SELECT [aetent].[iemp], ient, [cemp], [dnomfis], [ncif], ccodpos, [aebcodpos].dpob, [idirfis], "
        "[aebdir].ipai, cpai "
        "FROM [Anexa].[dbo].[aetent] "
        "INNER JOIN aetemp ON aetent.iemp = aetemp.iemp "
        "left JOIN [aebdir] ON aebdir.idir = aetemp.idirfis "
        "INNER JOIN [dbo].[aebcodpos] ON [aebcodpos].icodpos = aebdir.icodpos "
        "INNER JOIN [dbo].[aebpai] ON [aebpai].ipai = [aebdir].ipai "
        f"WHERE dnomfis LIKE '{rsocial}%' AND cpai = '{cpais}' "
        "AND NOT cent LIKE 'ENT%' AND NOT dnomcom LIKE '%BORRAR%'"
    )
    respuesta_query = bm.n_consulta(base_query)
    if len (respuesta_query["contenido"])==1:
        return respuesta_query["contenido"][0]
    # Bucle para reducir el largo del nombre
    search_name=rsocial
    while largo > len(rsocial) // 2:
        search_name = rsocial[:largo]
        largo -= 1

        base_query = (
                         "SELECT [aetent].[iemp], ient, [cemp], [dnomfis], [ncif], ccodpos, [aebcodpos].dpob, [idirfis], "
                         "[aebdir].ipai, cpai "
                         "FROM [Anexa].[dbo].[aetent] "
                         "INNER JOIN aetemp ON aetent.iemp = aetemp.iemp "
                         "left JOIN [aebdir] ON aebdir.idir = aetemp.idirfis "
                         "INNER JOIN [dbo].[aebcodpos] ON [aebcodpos].icodpos = aebdir.icodpos "
                         "INNER JOIN [dbo].[aebpai] ON [aebpai].ipai = [aebdir].ipai "
                         "WHERE dnomfis LIKE '"+search_name+"%' AND cpai = '"+cpais+"' "
                         "AND NOT cent LIKE 'ENT%' AND NOT dnomcom LIKE '%BORRAR%'"
        )

        respuesta_query = bm.n_consulta(base_query)

        contenido = respuesta_query.get("contenido", [])

        if len(contenido) == 1:
            logger.info(f" --- Devuelve {contenido[0]['ient']}")
            return contenido[0]

        elif len(contenido) > 1:
            logger.info(" --- Respuesta Query con denominación de origen")
            for entidad in contenido:
                logger.info(f" --- {entidad['ient']} ---> {entidad['dnomfis']} ---> {entidad['cemp']} "
                            f"Codpostal: {entidad['ccodpos']}")

                if entidad['ccodpos'] == codpostal.replace(" ", ""):
                    return entidad

    logger.info( f" --- NO SE ENCUENTRA EL DESTINATARIO {rsocial} con codigo postal: {codpostal}")
    return respuesta_default


if __name__ == "__main__":
    print(busca_destinatario('HIDRO-WATER', '46960', 'ES'))
    print(busca_destinatario('MATIC SRL', '46960', 'ES'))
    print(busca_destinatario('SLUSH & BEVERAGE EQUIPMENT V AIR, S.L.U.', '46716', 'ES'))
