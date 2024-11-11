




class EstadoAneGru:

    def __init__(self):
        self.ultimo_hito_comunicacion = None
        self.query_partidas = None
        self.query_num_partidas = None




"""
Les comunicamos lo que ellos nos han enviado IMPORTACIÓN
tomamos su chit y devolvemos su edi

Recorremos la BBDD cada 5 minutos
Si encontramos algún hito porterior a nuestro último hito de comunicación

Si obtengo datos, es decir partidas pendioentes de comunicar:
lanzo consulta por partida y comunico:

Comunicación:
=============
@HP00
Q00
Compopnemos la línea q10

"""