def file_process(file_name):
    results = state.MensajeEstado().leer_stat_gruber(file_name)
    for lineas in results.get("Lineas", []):
        if lineas.get("Record type ‘Q10’") == "Q10":
            n_ref_cor = lineas.get("Consignment number sending depot")
            n_status = lineas.get("Status code")
            m_query = f"select ipda, * from trapda where nrefcor ='{n_ref_cor}'"
            print(m_query) # Eliminar en producción
            bm = BmApi()
            query_reply = bm.consulta_(m_query)
            print(query_reply) # Eliminar en producción
            if "contenido" in query_reply and query_reply["contenido"]:
                n_ipda = query_reply["contenido"][0].get("ipda")
                n_json = {
                    "codigohito": get_cod_hito(n_status),
                    "descripciontracking": file_name.rsplit("/", 1)[-1],
                    "fechatracking": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
                }
                #Escribimos el json
                json_file = ("./Fixtures/par_" + file_name.rsplit("/", 1)[-1] + "_" +
                             query_reply["contenido"][0].get('Pick-up order number').strip() + ".json")
                with open(json_file, "w") as f:
                    f.write(str(n_json))

                tracking_reply = bm.post_partida_tracking(n_ipda, n_json)
                if  tracking_reply["status_code"] == 201:
                    print("Success")
                    n_mensaje = f"\nCreada partida {n_ipda}"
                    return [True, n_mensaje, json_file]
                else:
                    print("Fail")
                    n_mensaje = f"\nNo ha sido creada la partida {n_ipda}"
                    return [False, n_mensaje, json_file]
            else:
                n_ipda = None
            print(f"El ipda es: {n_ipda}")
