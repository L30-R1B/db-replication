import json
import logging
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import mysql.connector
import pyodbc

class ConfigOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # SQL Server Config
        parser.add_argument('--sqlserver_host', required=True, help='Host do SQL Server')
        parser.add_argument('--sqlserver_port', type=int, default=1433, help='Porta do SQL Server')
        parser.add_argument('--sqlserver_database', required=True, help='Nome do banco de dados no SQL Server')
        parser.add_argument('--sqlserver_user', required=True, help='Usuário do SQL Server')
        parser.add_argument('--sqlserver_password', required=True, help='Senha do SQL Server')
        
        # MySQL Config
        parser.add_argument('--mysql_host', required=True, help='Host do MySQL')
        parser.add_argument('--mysql_port', type=int, default=3306, help='Porta do MySQL')
        parser.add_argument('--mysql_database', required=True, help='Nome do banco de dados no MySQL')
        parser.add_argument('--mysql_user', required=True, help='Usuário do MySQL')
        parser.add_argument('--mysql_password', required=True, help='Senha do MySQL')
        
        # Sync Config
        parser.add_argument('--mapeamento', default='mapeamento.json', 
                          help='Caminho do arquivo de mapeamento (GCS path)')
        parser.add_argument('--temp_location', required=True, help='GCS temp location')
        parser.add_argument('--staging_location', required=True, help='GCS staging location')

class DatabaseConnectors:
    def __init__(self, options):
        self.options = options
    
    def get_sqlserver_conn(self):
        return pyodbc.connect(
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.options.sqlserver_host},{self.options.sqlserver_port};"
            f"DATABASE={self.options.sqlserver_database};"
            f"UID={self.options.sqlserver_user};"
            f"PWD={self.options.sqlserver_password};"
            f"Encrypt=no;TrustServerCertificate=yes;"
        )
    
    def get_mysql_conn(self):
        return mysql.connector.connect(
            host=self.options.mysql_host,
            port=self.options.mysql_port,
            database=self.options.mysql_database,
            user=self.options.mysql_user,
            password=self.options.mysql_password
        )

class LoadMapping(beam.DoFn):
    def __init__(self, mapeamento_path):
        self.mapeamento_path = mapeamento_path
    
    def process(self, element):
        from google.cloud import storage
        
        # Load mapping file from GCS
        storage_client = storage.Client()
        bucket_name, blob_name = self.mapeamento_path.replace("gs://", "").split("/", 1)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        content = blob.download_as_text()
        mapeamento = json.loads(content)
        
        yield mapeamento

class VerifySyncColumn(beam.DoFn):
    def __init__(self, options):
        self.options = options
    
    def process(self, element):
        mysql_table, mapeamento = element
        db_connectors = DatabaseConnectors(self.options)
        
        try:
            with db_connectors.get_mysql_conn() as mysql_conn:
                mysql_cursor = mysql_conn.cursor()
                
                mysql_cursor.execute(f"SHOW COLUMNS FROM {mysql_table} LIKE '__$last_sync'")
                coluna_existe = mysql_cursor.fetchone()

                if not coluna_existe:
                    mysql_cursor.execute(f"ALTER TABLE {mysql_table} ADD COLUMN __$last_sync BLOB")
                    mysql_conn.commit()
                    logging.info(f"Coluna '__$last_sync' criada na tabela '{mysql_table}'.")
                else:
                    mysql_cursor.execute(f"SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{mysql_table}' AND COLUMN_NAME = '__$last_sync'")
                    tipo_coluna = mysql_cursor.fetchone()[0]

                    if tipo_coluna.upper() != "BLOB":
                        mysql_cursor.execute(f"ALTER TABLE {mysql_table} MODIFY COLUMN __$last_sync BLOB")
                        mysql_conn.commit()
                        logging.info(f"Coluna '__$last_sync' alterada para BLOB na tabela '{mysql_table}'.")
                
                yield (mysql_table, mapeamento)
        except Exception as e:
            logging.error(f"Erro ao verificar/ajustar coluna '__$last_sync' na tabela '{mysql_table}': {e}")
            raise

class InitialSync(beam.DoFn):
    def __init__(self, options):
        self.options = options
    
    def process(self, element):
        mysql_table, mapeamento = element
        db_connectors = DatabaseConnectors(self.options)
        
        try:
            tabela_origem = next(iter(mapeamento.values()))["tabelaOrigem"]
            logging.info(f"Iniciando sincronização inicial para tabela '{mysql_table}' (origem: '{tabela_origem}')")
            
            colunas_mysql = [col for col in mapeamento.keys() if col != "__$last_sync"]
            colunas_sql_mapeadas = [mapeamento[col]["campoOrigem"] for col in colunas_mysql]
            
            with db_connectors.get_mysql_conn() as mysql_conn:
                mysql_cursor = mysql_conn.cursor()
                mysql_cursor.execute(f"DELETE FROM {mysql_table}")
                mysql_conn.commit()
                
                with db_connectors.get_sqlserver_conn() as sql_conn:
                    sql_cursor = sql_conn.cursor()
                    sql_cursor.execute(f"SELECT {', '.join(colunas_sql_mapeadas)} FROM {tabela_origem}")
                    dados = sql_cursor.fetchall()
                    
                    placeholders = ', '.join(['%s'] * len(colunas_mysql))
                    query_insert = f"INSERT INTO {mysql_table} ({', '.join(colunas_mysql)}) VALUES ({placeholders})"
                    
                    for linha in dados:
                        valores = tuple(linha)
                        mysql_cursor.execute(query_insert, valores)
                    
                    # Get current LSN
                    sql_cursor.execute("SELECT sys.fn_cdc_get_max_lsn()")
                    lsn_inicial = sql_cursor.fetchone()[0]
                    
                    mysql_cursor.execute(f"UPDATE {mysql_table} SET __$last_sync = %s", (lsn_inicial,))
                    mysql_conn.commit()
                    
                    logging.info(f"Sincronização inicial concluída para '{mysql_table}' com LSN: {lsn_inicial}")
            
            yield (mysql_table, mapeamento, lsn_inicial)
        except Exception as e:
            logging.error(f"Erro durante a sincronização inicial de '{mysql_table}': {e}")
            raise

class IncrementalSync(beam.DoFn):
    def __init__(self, options):
        self.options = options
    
    def process(self, element):
        mysql_table, mapeamento, ultimo_lsn = element
        db_connectors = DatabaseConnectors(self.options)
        
        try:
            tabela_origem = next(iter(mapeamento.values()))["tabelaOrigem"]
            logging.info(f"Iniciando sincronização incremental para tabela '{mysql_table}' (origem: '{tabela_origem}')")
            
            colunas_mysql = list(mapeamento.keys())
            colunas_sql_mapeadas = [mapeamento[col]["campoOrigem"] for col in colunas_mysql]
            
            with db_connectors.get_sqlserver_conn() as sql_conn:
                sql_cursor = sql_conn.cursor()
                
                # Get CDC table name
                sql_cursor.execute("SELECT capture_instance FROM cdc.change_tables WHERE capture_instance = ?", 
                                 (f"dbo_{tabela_origem}",))
                result = sql_cursor.fetchone()
                if not result:
                    logging.warning(f"Nenhuma tabela de CDC encontrada para '{tabela_origem}'.")
                    return
                
                tabela_cdc = f"{result[0]}_CT"
                
                # Get changes since last sync
                sql_cursor.execute(f"""
                    SELECT __$operation, __$start_lsn, {', '.join(colunas_sql_mapeadas)}
                    FROM cdc.{tabela_cdc}
                    WHERE __$start_lsn > CONVERT(VARBINARY(10), ?)
                """, (ultimo_lsn,))
                alteracoes = sql_cursor.fetchall()
                
                if not alteracoes:
                    logging.info(f"Nenhuma alteração encontrada para '{mysql_table}' desde a última sincronização.")
                    return
                
                logging.info(f"Encontradas {len(alteracoes)} alterações para processar")
                
                with db_connectors.get_mysql_conn() as mysql_conn:
                    mysql_cursor = mysql_conn.cursor()
                    
                    for alteracao in alteracoes:
                        operacao = alteracao[0]
                        lsn_atual = alteracao[1]
                        dados = alteracao[2:]
                        
                        try:
                            if operacao == 2:  # Inserção
                                query_insert = f"INSERT INTO {mysql_table} ({', '.join(colunas_mysql)}) VALUES ({', '.join(['%s'] * len(colunas_mysql))})"
                                mysql_cursor.execute(query_insert, dados)
                            elif operacao == 4:  # Atualização
                                query_update = f"UPDATE {mysql_table} SET {', '.join([f'{col} = %s' for col in colunas_mysql])} WHERE {colunas_mysql[0]} = %s"
                                mysql_cursor.execute(query_update, (*dados, dados[0]))
                            elif operacao == 1:  # Deleção
                                query_delete = f"DELETE FROM {mysql_table} WHERE {colunas_mysql[0]} = %s"
                                mysql_cursor.execute(query_delete, (dados[0],))
                            
                            if lsn_atual > ultimo_lsn:
                                ultimo_lsn = lsn_atual
                        except Exception as e:
                            logging.error(f"Erro ao processar alteração para '{mysql_table}': {e}")
                            continue
                    
                    # Update LSN
                    mysql_cursor.execute(f"UPDATE {mysql_table} SET __$last_sync = %s", (ultimo_lsn,))
                    mysql_conn.commit()
                    logging.info(f"Sincronização concluída para '{mysql_table}'. Novo LSN: {ultimo_lsn}")
            
            yield (mysql_table, mapeamento, ultimo_lsn)
        except Exception as e:
            logging.error(f"Erro ao sincronizar '{mysql_table}': {e}")
            raise

def run():
    pipeline_options = PipelineOptions()
    config_options = pipeline_options.view_as(ConfigOptions)
    
    with beam.Pipeline(options=pipeline_options) as p:
        # Load mapping
        mapeamento = (p 
                     | 'Start' >> beam.Create(['start'])
                     | 'LoadMapping' >> beam.ParDo(LoadMapping(config_options.mapeamento)))
        
        # Prepare tables and initial sync
        prepared_tables = (mapeamento 
                          | 'ExtractTables' >> beam.FlatMap(lambda m: [(k, m[k]) for k in m.keys()])
                          | 'VerifySyncColumns' >> beam.ParDo(VerifySyncColumn(config_options)))
        
        initial_sync = (prepared_tables 
                       | 'InitialSync' >> beam.ParDo(InitialSync(config_options)))
        
        # For production, you would add a loop here with windowing or triggers
        # This is a simplified version
        incremental_sync = (initial_sync 
                           | 'IncrementalSync' >> beam.ParDo(IncrementalSync(config_options)))
        
        # The pipeline could be extended with more complex scheduling logic

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()