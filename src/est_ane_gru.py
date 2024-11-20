import os
from datetime import timezone
from dotenv import load_dotenv
from utils.fortras_stat import MensajeEstado as Fort
from utils.bmaster_api import BmasterApi as BmApi
from utils.compose_q10_line import compose_q10_line as composeQ10
import pandas as pd
from datetime import datetime
import paramiko


class EstadoAneGru:

    def __init__(self):
        load_dotenv(dotenv_path="../conf/.env.base")
        self.entorno = os.getenv("ENTORNO")
        INTEGRATION_CUST = os.getenv("INTEGRATION_CUST")
        load_dotenv(dotenv_path=f"../conf/.env.'+{INTEGRATION_CUST}")
        self.host = os.getenv("SFTP_SERVER")
        self.username = os.getenv("SFTP_USER")
        self.password = os.getenv("SFTP_PW")
        self.port = int(os.getenv("SFTP_PORT"))
        self.local_work_directory = "../fixtures"
        self.sftp_stat_in_dir = os.getenv("SFTP_STAT_IN_DIR")
        self.sftp_dev_stat_in_dir = os.getenv("SFTP_DEV_STAT_IN_DIR")
        self.sftp_stat_out_dir = os.getenv("SFTP_STAT_OUT_DIR")
        self.sftp_dev_stat_out_dir = os.getenv("SFTP_DEV_STAT_OUT_DIR")
        self.remote_work_out_directory = self.sftp_stat_out_dir if self.entorno == "prod" else self.sftp_dev_stat_out_dir
        self.remote_work_in_directory = self.sftp_stat_in_dir if self.entorno == "prod" else self.sftp_dev_stat_in_dir
        self.partidas = None
        self.max_itrk = None
        self.bm = BmApi()
        self.query_partidas = """
        SELECT TOP 10
            itrk, aebtrk.ihit, aebhit.chit, aebhit.dhit, maxitrk, aebtrk.fmod, aebtrk.hmod, 
            aebtrk.fhit, aebtrk.hhit, trapda.cpda, trapda.ipda, nrefcor
        FROM 
            aebtrk
        INNER JOIN 
            trapda ON trapda.ipda = aebtrk.creg AND aebtrk.dtab = 'trapda'
        INNER JOIN 
            aebhit ON aebhit.ihit = aebtrk.ihit
        LEFT JOIN (
            SELECT 
                MAX(itrk) AS maxitrk, creg
            FROM 
                aebtrk
            INNER JOIN 
                trapda ON trapda.ipda = aebtrk.creg AND aebtrk.dtab = 'trapda'
            WHERE 
                trapda.ientcor IN (82861, 232829, 232830, 232831, 232833) 
                AND trapda.cpda LIKE 'TIP%'
                --AND aebtrk.ihit IN (647, 511, 523, 524) 
                AND aebtrk.ihit IN (647)
            GROUP BY 
                creg
        ) max_itrk ON max_itrk.creg = aebtrk.creg
        WHERE 
            aebtrk.ihit IN (541, 542, 543, 0, 544, 562, 630, 631, 632, 633, 635, 
                            513, 526, 527, 530, 568, 632, 633, 635, 636, 546, 547, 
                            511, 469, 302, 507, 512, 493, 508, 523, 524)
            AND trapda.ientcor IN (82861, 232829, 232830, 232831, 232833)
            AND trapda.cpda LIKE 'TIP%'
            AND (maxitrk < itrk OR maxitrk IS NULL)
            AND YEAR(fhit) * 100 + MONTH(fhit) > 202409
        ORDER BY 
            ipda, itrk ASC;
        """
        # self.query_repesca = None
        self.conversion_dict = {
            "ANXE01": "TBD",
            "ANXE02": "TBD",
            "ANXE05": "COR",
            "ANXE06": "COR",
            "ANXE07": "CRI",
            "ANXE09": "COR",
            "ANXE11": "302",
            "ANXE12": "CRI",
            "DESCARFAL": "SMA",
            "DESCARFTOT": "SMA",
            "DESCARKO": "SMA",
            "DESCAROK": "SMA",
            "ENT001": "SMA",
            "ENT004": "SMA",
            "ENT011": "SMA",
            "LINTRK02": "CRI",
            "SAL001": "402",
            "TRA0081": "202",
            "TRA0102": "402",
            "TRA0106": "COR"
        }


    @property
    def query_repesca(self):
        return f"""
                SELECT TOP 2000
                    trapda.cpda,
                    trapda.ipda,
                    nrefcor,
                    itrk,
                    aebtrk.ihit,
                    aebhit.chit,
                    aebhit.dhit,
                    aebtrk.fmod,
                    aebtrk.hmod,
                    aebtrk.fhit,
                    aebtrk.hhit
                FROM
                    aebtrk
                INNER JOIN
                    trapda
                    ON trapda.ipda = aebtrk.creg
                    AND aebtrk.dtab = 'trapda'
                INNER JOIN
                    aebhit
                    ON aebhit.ihit = aebtrk.ihit
                WHERE
                    aebtrk.ihit IN (
                        0, 302, 469, 493, 507, 508, 511, 512, 513, 523, 524, 526, 527, 530, 541,
                        542, 543, 544, 546, 547, 562, 568, 630, 631, 632, 633, 635, 636
                    )
                    AND trapda.ientcor IN (82861, 232829, 232830, 232831, 232833)
                    AND trapda.cpda LIKE 'TIP%'

                    --AND ipda = 5215  /* Hay que poner itrk e ipda según las variables acumuladas en el proceso */
                    --AND ipda = 5215  /* Hay que poner itrk e ipda según las variables acumuladas en el proceso */
                    AND itrk > {self.max_itrk}
                    AND YEAR(fhit) * 100 + MONTH(fhit) > 202409;
                    """


    def procesa_partida(self, cpda, q10_lines):
        number_of_records = len(q10_lines)
        fortras = Fort()
        txt_file_content = fortras.header("w")
        txt_file_content += fortras.header_q00("w")
        for q10_line in q10_lines:
            txt_file_content += composeQ10(
                status_code=self.conversion_dict.get(q10_line["status_code"], q10_line["status_code"]),
                date_of_event=q10_line["date_of_event"],
                time_of_event=q10_line["time_of_event"]
            )
        txt_file_content += fortras.z_control_record(number_of_records)
        txt_file_content += fortras.cierre("w")
        local_file_path = self.write_txt_file(cpda, txt_file_content)
        success = self.upload_file(local_file_path)
        if isinstance(self.partidas[cpda], list):
            self.partidas[cpda] = {
                'events': self.partidas[cpda],
                'success': success
            }
        else:
            self.partidas[cpda]['success'] = success

        if success:
            os.remove(local_file_path)
        else:
            print(f"Fallo al subir el archivo para cpda {cpda}")

        # print(txt_file_content)
        return txt_file_content


    def write_txt_file(self, cpda, content):
        filename = f"STATE-{cpda}-{datetime.now().strftime('%Y%m%d%H%M')}.txt"
        local_file_path = os.path.join(self.local_work_directory, filename)
        with open(local_file_path, 'w') as f:
            f.write(content)
        return local_file_path  # Devolver la ruta completa del archivo creado


    def process_query_response(self, query_reply):
        self.partidas = {}
        for entry in query_reply.get("contenido", []):
            cpda = entry.get("cpda")
            ipda = entry.get("ipda")  # Obtener ipda
            chit = entry.get("chit")
            fhit = entry.get("fhit")
            hhit = entry.get("hhit")
            event_data = {
                "status_code": chit,
                "date_of_event": fhit,
                "time_of_event": hhit
            }
            if cpda not in self.partidas:
                self.partidas[cpda] = {
                    'ipda': ipda,
                    'events': []
                }
            self.partidas[cpda]['events'].append(event_data)

        for cpda, data in self.partidas.items():
            events = data['events']
            self.procesa_partida(cpda, events)


    def upload_file(self, local_file_path):
        remote_directory = os.environ.get('SFTP_STAT_IN_DIR') if self.entorno == 'prod' else os.environ.get(
            'SFTP_DEV_STAT_IN_DIR')
        sftp_server = os.environ.get('SFTP_SERVER')
        sftp_user = os.environ.get('SFTP_USER')
        sftp_pw = os.environ.get('SFTP_PW')
        sftp_port = int(os.environ.get('SFTP_PORT', 22))
        remote_file_path = f"{remote_directory}/{os.path.basename(local_file_path)}"
        try:
            transport = paramiko.Transport((sftp_server, sftp_port))
            transport.connect(username=sftp_user, password=sftp_pw)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.put(local_file_path, remote_file_path)
            sftp.close()
            transport.close()
            return True
        except Exception as e:
            print(f"Error al subir el archivo {local_file_path} mediante SFTP: {e}")
            return False


    def actualizar_comunicado(self):
        for cpda, data in self.partidas.items():
            if data.get('success') is True:
                ipda = data.get('ipda')
                if not ipda:
                    print(f"No se encontró 'ipda' para cpda {cpda}")
                    continue

                fechatracking = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
                tracking_data = {
                    "codigohito": "ESTADOCOM",
                    "descripciontracking": "ESTADO COMUNICADO",
                    "fechatracking": fechatracking
                }
                response = self.bm.post_partida_tracking(ipda, tracking_data)
                if not isinstance(response, dict) or 'status_code' not in response:
                    print(f"Error al actualizar el comunicado para ipda {ipda}: Respuesta inválida")
                    continue
                if response['status_code'] not in [200, 201]:
                    print(
                        f"Error al actualizar el comunicado para ipda {ipda}: Código de estado {response['status_code']}")
                else:
                    print(f"Comunicado actualizado exitosamente para ipda {ipda}")


    def run(self):
        query_reply = self.bm.n_consulta(self.query_partidas)
        df = pd.DataFrame(query_reply['contenido'])
        self.max_itrk = df['itrk'].max()
        df = df.fillna("")
        pd.options.display.float_format = '{:.0f}'.format
        print(df.to_markdown(index=False))
        print(self.max_itrk)
        self.process_query_response(query_reply)
        self.actualizar_comunicado()
        second_query_reply = self.bm.n_consulta(self.query_repesca)
        df = pd.DataFrame(second_query_reply['contenido'])
        print(df.to_markdown(index=False))
        if second_query_reply.get("contenido"):
            self.partidas = None
            self.process_query_response(second_query_reply)
            self.actualizar_comunicado()


if __name__ == "__main__":
    estado_ane_gru = EstadoAneGru()
    estado_ane_gru.run()
