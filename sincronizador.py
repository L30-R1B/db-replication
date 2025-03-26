import mysql.connector
import json
import time
import pyodbc
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
import argparse

def configurar_argumentos():
    parser = argparse.ArgumentParser(description='Sincronizador de bancos de dados SQL Server para MySQL')
    
    # Argumentos para SQL Server
    parser.add_argument('--sqlserver-host', required=True, help='Host do SQL Server')
    parser.add_argument('--sqlserver-port', type=int, default=1433, help='Porta do SQL Server (padrão: 1433)')
    parser.add_argument('--sqlserver-database', required=True, help='Nome do banco de dados no SQL Server')
    parser.add_argument('--sqlserver-user', required=True, help='Usuário do SQL Server')
    parser.add_argument('--sqlserver-password', required=True, help='Senha do SQL Server')
    
    # Argumentos para MySQL
    parser.add_argument('--mysql-host', required=True, help='Host do MySQL')
    parser.add_argument('--mysql-port', type=int, default=3306, help='Porta do MySQL (padrão: 3306)')
    parser.add_argument('--mysql-database', required=True, help='Nome do banco de dados no MySQL')
    parser.add_argument('--mysql-user', required=True, help='Usuário do MySQL')
    parser.add_argument('--mysql-password', required=True, help='Senha do MySQL')
    
    # Configurações do sincronizador
    parser.add_argument('--intervalo', type=int, default=15, 
                       help='Intervalo entre sincronizações em segundos (padrão: 30)')
    parser.add_argument('--mapeamento', default='mapeamento.json', 
                       help='Caminho do arquivo de mapeamento (padrão: mapeamento.json)')
    parser.add_argument('--log', default='sincronizador.log', 
                       help='Caminho do arquivo de log (padrão: sincronizador.log)')
    
    return parser.parse_args()

# Configuração do sistema de logs
def configurar_logging(log_file):
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
    log_handler.setFormatter(log_formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(log_handler)
    return logger

args = configurar_argumentos()
logger = configurar_logging(args.log)

def carregar_mapeamento(arquivo_mapeamento):
    try:
        with open(arquivo_mapeamento, "r", encoding="utf-8") as file:
            mapeamento = json.load(file)
            logger.info(f"Mapeamento carregado com sucesso de {arquivo_mapeamento}.")
            return mapeamento
    except Exception as e:
        logger.error(f"Erro ao carregar {arquivo_mapeamento}: {e}", exc_info=True)
        exit(1)

MAPEAMENTO = carregar_mapeamento(args.mapeamento)

def conectar_sql_server():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={args.sqlserver_host},{args.sqlserver_port};"
            f"DATABASE={args.sqlserver_database};"
            f"UID={args.sqlserver_user};"
            f"PWD={args.sqlserver_password};"
            f"Encrypt=no;TrustServerCertificate=yes;"
        )
        logger.info("Conexão com SQL Server estabelecida com sucesso.")
        return conn, conn.cursor()
    except Exception as e:
        logger.error(f"Erro ao conectar ao SQL Server: {e}", exc_info=True)
        exit(1)

def conectar_mysql():
    try:
        conn = mysql.connector.connect(
            host=args.mysql_host,
            port=args.mysql_port,
            database=args.mysql_database,
            user=args.mysql_user,
            password=args.mysql_password
        )
        logger.info("Conexão com MySQL estabelecida com sucesso.")
        return conn, conn.cursor()
    except Exception as e:
        logger.error(f"Erro ao conectar ao MySQL: {e}", exc_info=True)
        exit(1)

sql_conn, sql_cursor = conectar_sql_server()
mysql_conn, mysql_cursor = conectar_mysql()

def verificar_e_ajustar_coluna_sincronizacao(mysql_table):
    """
    Verifica se a coluna __$last_sync existe na tabela do MySQL.
    Se não existir, cria a coluna. Se existir, altera o tipo para BLOB.
    """
    try:
        mysql_cursor.execute(f"SHOW COLUMNS FROM {mysql_table} LIKE '__$last_sync'")
        coluna_existe = mysql_cursor.fetchone()

        if not coluna_existe:
            mysql_cursor.execute(f"ALTER TABLE {mysql_table} ADD COLUMN __$last_sync BLOB")
            mysql_conn.commit()
            logger.info(f"Coluna '__$last_sync' criada na tabela '{mysql_table}'.")
        else:
            mysql_cursor.execute(f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{mysql_table}' AND COLUMN_NAME = '__$last_sync'")
            tipo_coluna = mysql_cursor.fetchone()[0]

            if tipo_coluna.upper() != "BLOB":
                mysql_cursor.execute(f"ALTER TABLE {mysql_table} MODIFY COLUMN __$last_sync BLOB")
                mysql_conn.commit()
                logger.info(f"Coluna '__$last_sync' alterada para BLOB na tabela '{mysql_table}'.")
    except Exception as e:
        logger.error(f"Erro ao verificar/ajustar coluna '__$last_sync' na tabela '{mysql_table}': {e}", exc_info=True)

def obter_lsn_atual():
    """ Obtém o LSN atual do SQL Server """
    try:
        sql_cursor.execute("SELECT sys.fn_cdc_get_max_lsn()")
        lsn_atual = sql_cursor.fetchone()[0]
        logger.debug(f"LSN atual obtido: {lsn_atual}")
        return lsn_atual
    except Exception as e:
        logger.error(f"Erro ao obter LSN atual: {e}", exc_info=True)
        return None

def sincronizacao_inicial(mysql_table):
    """
    Realiza uma sincronização inicial completa da tabela de origem para a tabela de destino.
    """
    try:
        mapeamento = MAPEAMENTO.get(mysql_table, {})
        if not mapeamento:
            logger.warning(f"Nenhum mapeamento encontrado para {mysql_table}, pulando...")
            return
        
        tabela_origem = next(iter(mapeamento.values()))["tabelaOrigem"]
        logger.info(f"Iniciando sincronização inicial para tabela '{mysql_table}' (origem: '{tabela_origem}')")
        
        colunas_mysql = [col for col in mapeamento.keys() if col != "__$last_sync"]
        colunas_sql_mapeadas = [mapeamento[col]["campoOrigem"] for col in colunas_mysql]
        
        mysql_cursor.execute(f"DELETE FROM {mysql_table}")
        mysql_conn.commit()
        logger.debug(f"Tabela '{mysql_table}' limpa para sincronização inicial")

        sql_cursor.execute(f"SELECT {', '.join(colunas_sql_mapeadas)} FROM {tabela_origem}")
        dados = sql_cursor.fetchall()
        logger.info(f"Encontrados {len(dados)} registros para sincronização inicial")

        placeholders = ', '.join(['%s'] * len(colunas_mysql))
        query_insert = f"INSERT INTO {mysql_table} ({', '.join(colunas_mysql)}) VALUES ({placeholders})"

        for linha in dados:
            valores = tuple(linha)
            mysql_cursor.execute(query_insert, valores)

        lsn_inicial = obter_lsn_atual()
        if lsn_inicial:
            mysql_cursor.execute(f"UPDATE {mysql_table} SET __$last_sync = %s", (lsn_inicial,))
            mysql_conn.commit()
            logger.info(f"Sincronização inicial concluída para '{mysql_table}' com LSN: {lsn_inicial}")
    except Exception as e:
        logger.error(f"Erro durante a sincronização inicial de '{mysql_table}': {e}", exc_info=True)

def obter_tabela_cdc(tabela_origem):
    """ Obtém o nome correto da tabela de CDC associada a uma tabela de origem """
    try:
        sql_cursor.execute("SELECT capture_instance FROM cdc.change_tables WHERE capture_instance = ?", (f"dbo_{tabela_origem}",))
        result = sql_cursor.fetchone()
        if result:
            tabela_cdc = f"{result[0]}_CT"
            logger.debug(f"Tabela CDC encontrada para '{tabela_origem}': {tabela_cdc}")
            return tabela_cdc
        else:
            logger.warning(f"Nenhuma tabela de CDC encontrada para '{tabela_origem}'.")
            return None
    except Exception as e:
        logger.error(f"Erro ao buscar tabela de CDC para '{tabela_origem}': {e}", exc_info=True)
        return None

def obter_ultima_sincronizacao(mysql_table):
    """ Obtém o último LSN sincronizado do MySQL """
    try:
        mysql_cursor.execute(f"SELECT COALESCE(MAX(__$last_sync), 0x0) FROM {mysql_table}")
        ultima_sincronizacao = mysql_cursor.fetchone()[0]
        if isinstance(ultima_sincronizacao, bytearray):
            ultima_sincronizacao = bytes(ultima_sincronizacao)
        logger.debug(f"Última sincronização para '{mysql_table}': {ultima_sincronizacao}")
        return ultima_sincronizacao
    except Exception as e:
        logger.warning(f"Erro ao obter última sincronização para '{mysql_table}': {e}")
        return b"\x00" * 10

def sincronizar_tabela(mysql_table):
    """ Sincroniza uma tabela do SQL Server com o MySQL """
    try:
        mapeamento = MAPEAMENTO.get(mysql_table, {})
        if not mapeamento:
            logger.warning(f"Nenhum mapeamento encontrado para {mysql_table}, pulando...")
            return
        
        tabela_origem = next(iter(mapeamento.values()))["tabelaOrigem"]
        logger.info(f"Iniciando sincronização incremental para tabela '{mysql_table}' (origem: '{tabela_origem}')")
        
        tabela_cdc = obter_tabela_cdc(tabela_origem)
        if not tabela_cdc:
            return

        colunas_mysql = list(mapeamento.keys())
        colunas_sql_mapeadas = [mapeamento[col]["campoOrigem"] for col in colunas_mysql]
        
        ultima_sincronizacao = obter_ultima_sincronizacao(mysql_table)
        logger.debug(f"Última sincronização: {ultima_sincronizacao}")

        sql_cursor.execute(f"""
            SELECT __$operation, __$start_lsn, {', '.join(colunas_sql_mapeadas)}
            FROM cdc.{tabela_cdc}
            WHERE __$start_lsn > CONVERT(VARBINARY(10), ?)
        """, (ultima_sincronizacao,))
        alteracoes = sql_cursor.fetchall()

        if not alteracoes:
            logger.info(f"Nenhuma alteração encontrada para '{mysql_table}' desde a última sincronização.")
            return

        logger.info(f"Encontradas {len(alteracoes)} alterações para processar")
        ultimo_lsn = ultima_sincronizacao

        for alteracao in alteracoes:
            operacao = alteracao[0]
            lsn_atual = alteracao[1]
            dados = alteracao[2:]

            try:
                if operacao == 2:  # Inserção
                    query_insert = f"INSERT INTO {mysql_table} ({', '.join(colunas_mysql)}) VALUES ({', '.join(['%s'] * len(colunas_mysql))})"
                    mysql_cursor.execute(query_insert, dados)
                    logger.debug(f"Inserido registro em '{mysql_table}': {dados}")
                elif operacao == 4:  # Atualização
                    query_update = f"UPDATE {mysql_table} SET {', '.join([f'{col} = %s' for col in colunas_mysql])} WHERE {colunas_mysql[0]} = %s"
                    mysql_cursor.execute(query_update, (*dados, dados[0]))
                    logger.debug(f"Atualizado registro em '{mysql_table}': {dados}")
                elif operacao == 1:  # Deleção
                    query_delete = f"DELETE FROM {mysql_table} WHERE {colunas_mysql[0]} = %s"
                    mysql_cursor.execute(query_delete, (dados[0],))
                    logger.debug(f"Removido registro de '{mysql_table}': ID {dados[0]}")

                if lsn_atual > ultimo_lsn:
                    ultimo_lsn = lsn_atual
            except Exception as e:
                logger.error(f"Erro ao processar alteração para '{mysql_table}': {e}", exc_info=True)

        # Atualizar LSN
        mysql_cursor.execute(f"UPDATE {mysql_table} SET __$last_sync = %s", (ultimo_lsn,))
        mysql_conn.commit()
        logger.info(f"Sincronização concluída para '{mysql_table}'. Novo LSN: {ultimo_lsn}")

    except Exception as e:
        logger.error(f"Erro ao sincronizar '{mysql_table}': {e}", exc_info=True)

def main():
    try:
        # Preparar colunas de sincronização
        for mysql_table in MAPEAMENTO.keys():
            verificar_e_ajustar_coluna_sincronizacao(mysql_table)

        # Sincronização inicial
        for mysql_table in MAPEAMENTO.keys():
            sincronizacao_inicial(mysql_table)

        # Loop de sincronização contínua
        while True:
            logger.info("Iniciando ciclo de sincronização...")
            inicio_ciclo = datetime.now()
            
            for mysql_table in MAPEAMENTO.keys():
                sincronizar_tabela(mysql_table)
            
            tempo_ciclo = (datetime.now() - inicio_ciclo).total_seconds()
            logger.info(f"Ciclo de sincronização concluído em {tempo_ciclo:.2f} segundos. Aguardando próximo ciclo...")
            time.sleep(args.intervalo)

    except KeyboardInterrupt:
        logger.info("Programa interrompido pelo usuário")
    except Exception as e:
        logger.error(f"Erro fatal no programa principal: {e}", exc_info=True)
    finally:
        try:
            sql_cursor.close()
            sql_conn.close()
            mysql_cursor.close()
            mysql_conn.close()
            logger.info("Conexões com os bancos de dados fechadas com sucesso")
        except Exception as e:
            logger.error(f"Erro ao fechar conexões: {e}", exc_info=True)

if __name__ == "__main__":
    main()