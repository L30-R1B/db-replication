import pyodbc
import time
from tabulate import tabulate
from datetime import datetime

# Configurações de conexão
SQLSERVER_CONFIG = {
    'server': 'localhost,1433',
    'database': 'ExemploCDC',
    'user': 'SA',
    'password': 'SeuForte@Passw0rd123',
    'driver': '{ODBC Driver 17 for SQL Server}',
    'encrypt': 'no',
    'trust_server_certificate': 'yes'
}

def create_connection():
    """Cria conexão com o SQL Server"""
    conn_str = (
        f"DRIVER={SQLSERVER_CONFIG['driver']};"
        f"SERVER={SQLSERVER_CONFIG['server']};"
        f"DATABASE={SQLSERVER_CONFIG['database']};"
        f"UID={SQLSERVER_CONFIG['user']};"
        f"PWD={SQLSERVER_CONFIG['password']};"
        f"Encrypt={SQLSERVER_CONFIG['encrypt']};"
        f"TrustServerCertificate={SQLSERVER_CONFIG['trust_server_certificate']}"
    )

    try:
        conn = pyodbc.connect(conn_str)
        conn.autocommit = True  # Importante para comandos DDL
        return conn
    except pyodbc.Error as e:
        print(f"Erro ao conectar: {str(e)}")
        return None

def fix_cdc_configuration(conn, schema_name, table_name, columns_to_capture):
    """Corrige a configuração do CDC para a tabela especificada"""
    cursor = conn.cursor()
    try:
        # Verifica se a tabela tem CDC ativado
        cursor.execute("""
            SELECT capture_instance 
            FROM cdc.change_tables
            WHERE source_object_id = OBJECT_ID(? + '.' + ?)
        """, schema_name, table_name)
        
        capture_instance = cursor.fetchone()
        
        if capture_instance:
            # Se já existe, desativa primeiro
            capture_instance = capture_instance[0]
            print(f"\nDesativando CDC existente para {schema_name}.{table_name}...")
            cursor.execute(f"""
                EXEC sys.sp_cdc_disable_table 
                    @source_schema = ?,
                    @source_name = ?,
                    @capture_instance = ?
            """, schema_name, table_name, capture_instance)
            print("CDC desativado com sucesso.")
        
        # Ativa o CDC com as colunas especificadas
        print(f"\nAtivando CDC para {schema_name}.{table_name} com colunas: {columns_to_capture}")
        cursor.execute(f"""
            EXEC sys.sp_cdc_enable_table
                @source_schema = ?,
                @source_name = ?,
                @role_name = NULL,
                @supports_net_changes = 1,
                @captured_column_list = ?
        """, schema_name, table_name, columns_to_capture)
        
        print("CDC reconfigurado com sucesso!")
        return True
        
    except pyodbc.Error as e:
        print(f"\nERRO ao reconfigurar CDC: {str(e)}")
        return False
    finally:
        cursor.close()

def manual_cdc_capture(conn):
    """Executa o processo de captura manualmente"""
    cursor = conn.cursor()
    try:
        print("\nExecutando captura manual de mudanças...")
        cursor.execute("CHECKPOINT")
        cursor.execute("EXEC sys.sp_cdc_scan")
        print("Captura manual concluída.")
        return True
    except pyodbc.Error as e:
        print(f"\nERRO na captura manual: {str(e)}")
        return False
    finally:
        cursor.close()

def monitor_cdc_changes(conn, schema_name, table_name, interval=10):
    """Monitora continuamente as mudanças CDC"""
    print(f"\nIniciando monitoramento CDC para {schema_name}.{table_name}")
    print(f"Verificando a cada {interval} segundos...")
    print("Pressione Ctrl+C para parar\n")
    
    try:
        while True:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"\n=== [{now}] Ciclo de verificação ===")
            
            # Executa captura manual
            manual_cdc_capture(conn)
            
            # Consulta mudanças
            query_cdc_changes(conn, schema_name, table_name)
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\nMonitoramento interrompido pelo usuário")

def query_cdc_changes(conn, schema_name, table_name):
    """Consulta as mudanças capturadas pelo CDC"""
    cursor = conn.cursor()
    try:
        # Obtém o nome da instância de captura
        cursor.execute("""
            SELECT capture_instance 
            FROM cdc.change_tables
            WHERE source_object_id = OBJECT_ID(? + '.' + ?)
        """, schema_name, table_name)
        
        result = cursor.fetchone()
        if not result:
            print("CDC não está ativado para esta tabela.")
            return
        
        capture_instance = result[0]
        
        # Obtém os intervalos LSN
        cursor.execute(f"SELECT sys.fn_cdc_get_min_lsn('{capture_instance}') AS min_lsn")
        min_lsn = cursor.fetchone()[0]
        
        cursor.execute("SELECT sys.fn_cdc_get_max_lsn() AS max_lsn")
        max_lsn = cursor.fetchone()[0]
        
        if not min_lsn or not max_lsn:
            print("\nAVISO: Nenhuma mudança capturada ainda")
            return
        
        print(f"\nConsultando mudanças de LSN {min_lsn} a {max_lsn}")
        
        # Consulta as mudanças
        cursor.execute(f"""
            DECLARE @from_lsn binary(10) = ?
            DECLARE @to_lsn binary(10) = ?
            
            SELECT * 
            FROM cdc.fn_cdc_get_all_changes_{capture_instance}(@from_lsn, @to_lsn, 'all')
        """, min_lsn, max_lsn)
        
        columns = [column[0] for column in cursor.description]
        changes = cursor.fetchall()
        
        if changes:
            print(f"\n{len(changes)} mudanças capturadas:")
            print(tabulate(changes, headers=columns, tablefmt='grid'))
        else:
            print("\nNenhuma mudança capturada no intervalo.")
            
    except pyodbc.Error as e:
        print(f"\nErro ao consultar mudanças CDC: {str(e)}")
    finally:
        cursor.close()

def main():
    print("=== Solução CDC para SQL Server sem Agent ===")
    
    conn = create_connection()
    if not conn:
        return
    
    try:
        schema = "dbo"
        table = "teste"
        columns = "id,valor1,valor2,data_registro"  # Ajuste para suas colunas
        
        # 1. Corrigir a configuração do CDC
        if not fix_cdc_configuration(conn, schema, table, columns):
            return
        
        # 2. Iniciar monitoramento contínuo
        monitor_cdc_changes(conn, schema, table, interval=10)
        
    except KeyboardInterrupt:
        print("\nOperação interrompida pelo usuário")
    finally:
        conn.close()
        print("\nConexão encerrada.")

if __name__ == "__main__":
    main()