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
    parser.add_argument('--tabelas', default='tabelas.json', 
                       help='Caminho do arquivo com lista de tabelas para sincronizar (padrão: tabelas.json)')
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

def carregar_tabelas(arquivo_tabelas):
    try:
        with open(arquivo_tabelas, "r", encoding="utf-8") as file:
            dados = json.load(file)
            tabelas = dados.get("tabelas", [])
            logger.info(f"Tabelas carregadas com sucesso: {tabelas}")
            return tabelas
    except Exception as e:
        logger.error(f"Erro ao carregar {arquivo_tabelas}: {e}", exc_info=True)
        exit(1)

TABELAS = carregar_tabelas(args.tabelas)

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

def verificar_e_criar_tabela(tabela):
    """
    Verifica se a tabela existe no MySQL. Se não existir, cria com a mesma estrutura do SQL Server.
    Também verifica/adiciona a coluna __$last_sync.
    """
    try:
        # Verificar se a tabela existe no MySQL
        mysql_cursor.execute(f"SHOW TABLES LIKE '{tabela}'")
        tabela_existe = mysql_cursor.fetchone()
        
        if not tabela_existe:
            # Obter estrutura da tabela no SQL Server
            sql_cursor.execute(f"""
                SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE 
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = '{tabela}'
                ORDER BY ORDINAL_POSITION
            """)
            colunas = sql_cursor.fetchall()
            
            if not colunas:
                logger.error(f"Tabela '{tabela}' não encontrada no SQL Server")
                return False
            
            # Construir query de criação da tabela
            colunas_def = []
            for col in colunas:
                nome = col[0]
                tipo = col[1]
                tamanho = col[2]
                nullable = "NULL" if col[3] == "YES" else "NOT NULL"
                
                # Mapear tipos de dados do SQL Server para MySQL
                if tipo in ["varchar", "nvarchar", "char", "nchar"]:
                    tipo_def = f"{tipo}({tamanho})" if tamanho else f"{tipo}(255)"
                elif tipo in ["decimal", "numeric"]:
                    tipo_def = tipo
                elif tipo == "datetime":
                    tipo_def = "DATETIME"
                elif tipo == "bit":
                    tipo_def = "TINYINT(1)"
                else:
                    tipo_def = tipo
                
                colunas_def.append(f"{nome} {tipo_def} {nullable}")
            
            # Adicionar coluna de sincronização
            colunas_def.append("__$last_sync BLOB")
            
            # Criar tabela
            create_query = f"CREATE TABLE {tabela} (\n  " + ",\n  ".join(colunas_def) + "\n)"
            mysql_cursor.execute(create_query)
            mysql_conn.commit()
            logger.info(f"Tabela '{tabela}' criada no MySQL com sucesso")
        else:
            # Verificar/adicionar coluna de sincronização
            mysql_cursor.execute(f"SHOW COLUMNS FROM {tabela} LIKE '__$last_sync'")
            coluna_existe = mysql_cursor.fetchone()

            if not coluna_existe:
                mysql_cursor.execute(f"ALTER TABLE {tabela} ADD COLUMN __$last_sync BLOB")
                mysql_conn.commit()
                logger.info(f"Coluna '__$last_sync' adicionada à tabela '{tabela}'")
            else:
                mysql_cursor.execute(f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabela}' AND COLUMN_NAME = '__$last_sync'")
                tipo_coluna = mysql_cursor.fetchone()[0]

                if tipo_coluna.upper() != "BLOB":
                    mysql_cursor.execute(f"ALTER TABLE {tabela} MODIFY COLUMN __$last_sync BLOB")
                    mysql_conn.commit()
                    logger.info(f"Coluna '__$last_sync' alterada para BLOB na tabela '{tabela}'")
        
        return True
        
    except Exception as e:
        logger.error(f"Erro ao verificar/criar tabela '{tabela}': {e}", exc_info=True)
        return False

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

def obter_colunas_tabela(tabela):
    """ Obtém a lista de colunas de uma tabela no SQL Server, exceto __$last_sync """
    try:
        sql_cursor.execute(f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{tabela}' 
            AND COLUMN_NAME != '__$last_sync'
            ORDER BY ORDINAL_POSITION
        """)
        colunas = [row[0] for row in sql_cursor.fetchall()]
        return colunas
    except Exception as e:
        logger.error(f"Erro ao obter colunas da tabela '{tabela}': {e}", exc_info=True)
        return []

def sincronizacao_inicial(tabela):
    """
    Realiza uma sincronização inicial completa da tabela de origem para a tabela de destino.
    """
    try:
        logger.info(f"Iniciando sincronização inicial para tabela '{tabela}'")
        
        colunas = obter_colunas_tabela(tabela)
        if not colunas:
            logger.error(f"Não foi possível obter colunas para a tabela '{tabela}'")
            return
        
        mysql_cursor.execute(f"DELETE FROM {tabela}")
        mysql_conn.commit()
        logger.debug(f"Tabela '{tabela}' limpa para sincronização inicial")

        sql_cursor.execute(f"SELECT {', '.join(colunas)} FROM {tabela}")
        dados = sql_cursor.fetchall()
        logger.info(f"Encontrados {len(dados)} registros para sincronização inicial")

        placeholders = ', '.join(['%s'] * len(colunas))
        query_insert = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({placeholders})"

        for linha in dados:
            valores = tuple(linha)
            mysql_cursor.execute(query_insert, valores)

        lsn_inicial = obter_lsn_atual()
        if lsn_inicial:
            mysql_cursor.execute(f"UPDATE {tabela} SET __$last_sync = %s", (lsn_inicial,))
            mysql_conn.commit()
            logger.info(f"Sincronização inicial concluída para '{tabela}' com LSN: {lsn_inicial}")
    except Exception as e:
        logger.error(f"Erro durante a sincronização inicial de '{tabela}': {e}", exc_info=True)

def obter_tabela_cdc(tabela):
    """ Obtém o nome correto da tabela de CDC associada a uma tabela de origem """
    try:
        sql_cursor.execute("SELECT capture_instance FROM cdc.change_tables WHERE capture_instance = ?", (f"dbo_{tabela}",))
        result = sql_cursor.fetchone()
        if result:
            tabela_cdc = f"{result[0]}_CT"
            logger.debug(f"Tabela CDC encontrada para '{tabela}': {tabela_cdc}")
            return tabela_cdc
        else:
            logger.warning(f"Nenhuma tabela de CDC encontrada para '{tabela}'.")
            return None
    except Exception as e:
        logger.error(f"Erro ao buscar tabela de CDC para '{tabela}': {e}", exc_info=True)
        return None

def obter_ultima_sincronizacao(tabela):
    """ Obtém o último LSN sincronizado do MySQL """
    try:
        mysql_cursor.execute(f"SELECT COALESCE(MAX(__$last_sync), 0x0) FROM {tabela}")
        ultima_sincronizacao = mysql_cursor.fetchone()[0]
        if isinstance(ultima_sincronizacao, bytearray):
            ultima_sincronizacao = bytes(ultima_sincronizacao)
        logger.debug(f"Última sincronização para '{tabela}': {ultima_sincronizacao}")
        return ultima_sincronizacao
    except Exception as e:
        logger.warning(f"Erro ao obter última sincronização para '{tabela}': {e}")
        return b"\x00" * 10

def sincronizar_tabela(tabela):
    """ Sincroniza uma tabela do SQL Server com o MySQL """
    try:
        logger.info(f"Iniciando sincronização incremental para tabela '{tabela}'")
        
        tabela_cdc = obter_tabela_cdc(tabela)
        if not tabela_cdc:
            return

        colunas = obter_colunas_tabela(tabela)
        if not colunas:
            logger.error(f"Não foi possível obter colunas para a tabela '{tabela}'")
            return
        
        ultima_sincronizacao = obter_ultima_sincronizacao(tabela)
        logger.debug(f"Última sincronização: {ultima_sincronizacao}")

        sql_cursor.execute(f"""
            SELECT __$operation, __$start_lsn, {', '.join(colunas)}
            FROM cdc.{tabela_cdc}
            WHERE __$start_lsn > CONVERT(VARBINARY(10), ?)
        """, (ultima_sincronizacao,))
        alteracoes = sql_cursor.fetchall()

        if not alteracoes:
            logger.info(f"Nenhuma alteração encontrada para '{tabela}' desde a última sincronização.")
            return

        logger.info(f"Encontradas {len(alteracoes)} alterações para processar")
        ultimo_lsn = ultima_sincronizacao

        for alteracao in alteracoes:
            operacao = alteracao[0]
            lsn_atual = alteracao[1]
            dados = alteracao[2:]

            try:
                if operacao == 2:  # Inserção
                    query_insert = f"INSERT INTO {tabela} ({', '.join(colunas)}) VALUES ({', '.join(['%s'] * len(colunas))})"
                    mysql_cursor.execute(query_insert, dados)
                    logger.debug(f"Inserido registro em '{tabela}': {dados}")
                elif operacao == 4:  # Atualização
                    query_update = f"UPDATE {tabela} SET {', '.join([f'{col} = %s' for col in colunas])} WHERE {colunas[0]} = %s"
                    mysql_cursor.execute(query_update, (*dados, dados[0]))
                    logger.debug(f"Atualizado registro em '{tabela}': {dados}")
                elif operacao == 1:  # Deleção
                    query_delete = f"DELETE FROM {tabela} WHERE {colunas[0]} = %s"
                    mysql_cursor.execute(query_delete, (dados[0],))
                    logger.debug(f"Removido registro de '{tabela}': ID {dados[0]}")

                if lsn_atual > ultimo_lsn:
                    ultimo_lsn = lsn_atual
            except Exception as e:
                logger.error(f"Erro ao processar alteração para '{tabela}': {e}", exc_info=True)

        # Atualizar LSN
        mysql_cursor.execute(f"UPDATE {tabela} SET __$last_sync = %s", (ultimo_lsn,))
        mysql_conn.commit()
        logger.info(f"Sincronização concluída para '{tabela}'. Novo LSN: {ultimo_lsn}")

    except Exception as e:
        logger.error(f"Erro ao sincronizar '{tabela}': {e}", exc_info=True)

def main():
    try:
        # Verificar e criar tabelas se necessário
        for tabela in TABELAS:
            if not verificar_e_criar_tabela(tabela):
                continue

        # Sincronização inicial
        for tabela in TABELAS:
            sincronizacao_inicial(tabela)

        # Loop de sincronização contínua
        while True:
            logger.info("Iniciando ciclo de sincronização...")
            inicio_ciclo = datetime.now()
            
            for tabela in TABELAS:
                sincronizar_tabela(tabela)
            
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