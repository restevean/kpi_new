# utils/buscar_empresa.py

import logging
from utils.bmaster_api import BmasterApi

# logging.basicConfig(
#     level=logging.INFO,  # Nivel mínimo de mensajes a mostrar
#     # format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje
#     format=' %(message)s',  # Formato del mensaje
# )

logger = logging.getLogger(__name__)

def busca_destinatario(rsocial='', codpostal='', cpais=''):
    largo = len(rsocial)
    bm = BmasterApi()
    respuesta_default = {"ient": 0}
    base_query = (
        "SELECT [aetent].[iemp], ient, [cemp], [dnomfis], [ncif], ccodpos, [aebcodpos].dpob, [idirfis], "
        "[aebdir].ipai, cpai "
        "FROM [Anexa].[dbo].[aetent] "
        "INNER JOIN aetemp ON aetent.iemp = aetemp.iemp "
        "INNER JOIN [aebdir] ON aebdir.idir = aetemp.idirfis "
        "INNER JOIN [dbo].[aebcodpos] ON [aebcodpos].icodpos = aebdir.icodpos "
        "INNER JOIN [dbo].[aebpai] ON [aebpai].ipai = [aebdir].ipai "
        "WHERE dnomfis LIKE '{rsocial}' AND cpai = '{cpais}' "
        "AND NOT cent LIKE 'ENT%' AND NOT dnomcom LIKE '%BORRAR%'"
    )

    # Bucle para reducir el largo del nombre
    while largo > len(rsocial) // 2:
        search_name = rsocial[:largo]
        largo -= 1

        query = base_query.format(rsocial=f"{search_name}%", cpais=cpais)
        # query_codpost = f"{query} AND ccodpos = '{codpostal}'"

        respuesta_query = bm.consulta_(query)
        if respuesta_query["status_code"] != 200:
            return respuesta_default
        contenido = respuesta_query.get("contenido", [])

        if len(contenido) == 1:
            logger.info(f"\nDevuelve {contenido[0]['ient']}\n")
            return contenido[0]

        elif len(contenido) > 1:
            logger.info("\nRespuesta Query con denominación de origen")
            for entidad in contenido:
                logger.info(f"\n{entidad['ient']} ---> {entidad['dnomfis']} ---> {entidad['cemp']} "
                            f"Codpostal: {entidad['ccodpos']}")

                if entidad['ccodpos'] == codpostal.replace(" ", ""):
                    return entidad

    logger.info( f"\nNO SE ENCUENTRA EL DESTINATARIO {rsocial} con codigo postal: {codpostal}")
    return respuesta_default
