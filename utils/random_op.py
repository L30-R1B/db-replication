import pyodbc
import mysql.connector
from mysql.connector import Error
import random
from datetime import datetime, timedelta
import time

# Configurações de conexão
SQLSERVER_CONFIG = {
    'server': 'localhost,1433',
    'database': 'ExemploCDC',
    'user': 'SA',
    'password': 'SeuForte@Passw0rd123',
    'port': 1433,
    'driver': '{ODBC Driver 17 for SQL Server}',
    'encrypt': 'no',
    'trust_server_certificate': 'yes'
}

MYSQL_CONFIG = {
    'host': 'mysql-195100-0.cloudclusters.net',
    'database': 'Teste',
    'user': 'Leo',
    'password': '2025058089',
    'port': 19542
}

def create_sqlserver_connection():
    try:
        conn_str = f"DRIVER={SQLSERVER_CONFIG['driver']};SERVER={SQLSERVER_CONFIG['server']};DATABASE={SQLSERVER_CONFIG['database']};UID={SQLSERVER_CONFIG['user']};PWD={SQLSERVER_CONFIG['password']};PORT={SQLSERVER_CONFIG['port']};Encrypt={SQLSERVER_CONFIG['encrypt']};TrustServerCertificate={SQLSERVER_CONFIG['trust_server_certificate']}"
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        print(f"Erro ao conectar ao SQL Server: {e}")
        return None

def create_mysql_connection():
    try:
        conn = mysql.connector.connect(
            host=MYSQL_CONFIG['host'],
            database=MYSQL_CONFIG['database'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            port=MYSQL_CONFIG['port']
        )
        return conn
    except Error as e:
        print(f"Erro ao conectar ao MySQL: {e}")
        return None

def create_tables(conn, db_type):
    cursor = conn.cursor()
    
    try:
        if db_type == 'sqlserver':
            # Tabela PRODUTOS
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PRODUTOS')
            BEGIN
                CREATE TABLE PRODUTOS (
                    FILIAL INT,
                    CODIGO INT,
                    DESCRICAO VARCHAR(255),
                    TIPO CHAR(2),
                    COD_DIVISAO INT,
                    DESC_DIVISAO VARCHAR(255),
                    COD_CLASSE INT,
                    DESC_CLASSE VARCHAR(255),
                    COD_SUBCLASSE INT,
                    DESC_SUBCLASSE VARCHAR(255),
                    COD_APRESENTACAO INT,
                    DESC_APRESENTACAO VARCHAR(255),
                    COD_FORMATO INT,
                    DESC_FORMATO VARCHAR(255),
                    COD_LINHA INT,
                    DESC_LINHA VARCHAR(255),
                    RASTRO CHAR(1),
                    UM VARCHAR(10),
                    LOCAPAD INT,
                    QB INT,
                    METAL CHAR(1),
                    LOCALIZ CHAR(1),
                    REFUSAO CHAR(1),
                    COD_COMPONENTE INT,
                    DESC_COMPONENTE VARCHAR(255),
                    QTD_COMPONENTE FLOAT,
                    PRIMARY KEY (FILIAL, CODIGO)
                )
                PRINT 'Tabela PRODUTOS criada com sucesso.'
            END
            """)
            
            # Tabela MOVIMENTACAO
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'MOVIMENTACAO')
            BEGIN
                CREATE TABLE MOVIMENTACAO (
                    FILIAL INT,
                    OP BIGINT,
                    QUANT FLOAT,
                    PERDA FLOAT,
                    TM INT,
                    CODIGO INT,
                    DESCRICAO VARCHAR(255),
                    LOTE BIGINT,
                    SUB_LOTE BIGINT,
                    DT_EMISSAO DATETIME,
                    CF VARCHAR(10),
                    LOCAL INT,
                    DOC VARCHAR(50),
                    NUMSEQ VARCHAR(50),
                    QTD_PREVISTA FLOAT,
                    QTD_PRODUZIDA FLOAT,
                    DT_ENCERRAMENTO DATETIME,
                    QTD_PLANEJADA INT,
                    PRIMARY KEY (FILIAL, OP)
                )
                PRINT 'Tabela MOVIMENTACAO criada com sucesso.'
            END
            """)
            
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'FORNO_METRICS')
            BEGIN
                CREATE TABLE FORNO_METRICS (
                    hour TIME,
                    date DATE,
                    power_kg FLOAT,
                    temp_c FLOAT,
                    load_kg FLOAT,
                    PRIMARY KEY (hour, date)
                )
                PRINT 'Tabela FORNO_METRICS criada com sucesso.'
            END
            """)
            
            conn.commit()
            
            # Tabela ESTOQUE
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ESTOQUE')
            BEGIN
                CREATE TABLE ESTOQUE (
                    FILIAL INT,
                    CODIGO INT,
                    LOCAL INT,
                    DATA DATETIME,
                    SALDO INT,
                    EMPENHO INT,
                    LOTE BIGINT,
                    NUMLOTE BIGINT,
                    CLASSIFICACAO VARCHAR(50),
                    PRIMARY KEY (FILIAL, CODIGO, LOCAL, DATA)
                )
                PRINT 'Tabela ESTOQUE criada com sucesso.'
            END
            """)
            
            # Tabela VENDAS
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'VENDAS')
            BEGIN
                CREATE TABLE VENDAS (
                    Cliente VARCHAR(255),
                    Produto VARCHAR(255),
                    Pedido INT,
                    Expedicao DATETIME,
                    NF INT,
                    Qtd_Kg INT,
                    Premio FLOAT,
                    LME FLOAT,
                    PRIMARY KEY (Cliente, Produto, Pedido)
                )
                PRINT 'Tabela VENDAS criada com sucesso.'
            END
            """)
            
            # Tabela PRODUTIVIDADE
            cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PRODUTIVIDADE')
            BEGIN
                CREATE TABLE PRODUTIVIDADE (
                    Data DATETIME,
                    PRODUTO VARCHAR(255),
                    OP BIGINT,
                    LOTE BIGINT,
                    Kg INT,
                    Forno VARCHAR(50),
                    OperadorForno VARCHAR(100),
                    HoraInicio TIME,
                    HoraFim TIME,
                    Duracao TIME,
                    PRIMARY KEY (Data, PRODUTO, OP)
                )
                PRINT 'Tabela PRODUTIVIDADE criada com sucesso.'
            END
            """)
            
            conn.commit()
            print("Todas as tabelas foram criadas no SQL Server.")
            
        elif db_type == 'mysql':
            # Tabela PRODUTOS
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS PRODUTOS (
                FILIAL INT,
                CODIGO INT,
                DESCRICAO VARCHAR(255),
                TIPO CHAR(2),
                COD_DIVISAO INT,
                DESC_DIVISAO VARCHAR(255),
                COD_CLASSE INT,
                DESC_CLASSE VARCHAR(255),
                COD_SUBCLASSE INT,
                DESC_SUBCLASSE VARCHAR(255),
                COD_APRESENTACAO INT,
                DESC_APRESENTACAO VARCHAR(255),
                COD_FORMATO INT,
                DESC_FORMATO VARCHAR(255),
                COD_LINHA INT,
                DESC_LINHA VARCHAR(255),
                RASTRO CHAR(1),
                UM VARCHAR(10),
                LOCAPAD INT,
                QB INT,
                METAL CHAR(1),
                LOCALIZ CHAR(1),
                REFUSAO CHAR(1),
                COD_COMPONENTE INT,
                DESC_COMPONENTE VARCHAR(255),
                QTD_COMPONENTE FLOAT,
                PRIMARY KEY (FILIAL, CODIGO)
            )
            """)
            
            # Tabela MOVIMENTACAO
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS MOVIMENTACAO (
                FILIAL INT,
                OP BIGINT,
                QUANT FLOAT,
                PERDA FLOAT,
                TM INT,
                CODIGO INT,
                DESCRICAO VARCHAR(255),
                LOTE BIGINT,
                SUB_LOTE BIGINT,
                DT_EMISSAO DATETIME,
                CF VARCHAR(10),
                LOCAL INT,
                DOC VARCHAR(50),
                NUMSEQ VARCHAR(50),
                QTD_PREVISTA FLOAT,
                QTD_PRODUZIDA FLOAT,
                DT_ENCERRAMENTO DATETIME,
                QTD_PLANEJADA INT,
                PRIMARY KEY (FILIAL, OP)
            """)
            
            # Tabela ESTOQUE
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS ESTOQUE (
                FILIAL INT,
                CODIGO INT,
                LOCAL INT,
                DATA DATETIME,
                SALDO INT,
                EMPENHO INT,
                LOTE BIGINT,
                NUMLOTE BIGINT,
                CLASSIFICACAO VARCHAR(50),
                PRIMARY KEY (FILIAL, CODIGO, LOCAL, DATA)
            )
            """)
            
            # Tabela VENDAS
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS VENDAS (
                Cliente VARCHAR(255),
                Produto VARCHAR(255),
                Pedido INT,
                Expedicao DATETIME,
                NF INT,
                Qtd_Kg INT,
                Premio FLOAT,
                LME FLOAT,
                PRIMARY KEY (Cliente, Produto, Pedido)
            )
            """)
            
            # Tabela PRODUTIVIDADE
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS PRODUTIVIDADE (
                Data DATETIME,
                PRODUTO VARCHAR(255),
                OP BIGINT,
                LOTE BIGINT,
                Kg INT,
                Forno VARCHAR(50),
                OperadorForno VARCHAR(100),
                HoraInicio TIME,
                HoraFim TIME,
                Duracao TIME,
                PRIMARY KEY (Data, PRODUTO, OP)
            )
            """)
            
            conn.commit()
            print("Todas as tabelas foram criadas no MySQL.")
            
    except Exception as e:
        print(f"Erro ao criar tabelas no {db_type.upper()}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def generate_sample_data(table_name):
    """Gera dados de exemplo para as tabelas"""
    if table_name == 'PRODUTOS':
        return {
            'FILIAL': random.randint(1, 20000),
            'CODIGO': random.randint(100000, 999999),
            'DESCRICAO': random.choice(['TIBAL 5/1 ROD', 'TIBAL 5/1 BOBINA 200KG - PALLET']),
            'TIPO': random.choice(['PP', 'PA']),
            'COD_DIVISAO': random.randint(1, 3),
            'DESC_DIVISAO': 'ALUMINIO',
            'COD_CLASSE': random.randint(1, 3),
            'DESC_CLASSE': 'MASTER ALLOY',
            'COD_SUBCLASSE': random.randint(1, 3),
            'DESC_SUBCLASSE': 'TIBAL',
            'COD_APRESENTACAO': random.randint(1, 3),
            'DESC_APRESENTACAO': '5/1',
            'COD_FORMATO': random.randint(1, 3),
            'DESC_FORMATO': 'BOBINA',
            'COD_LINHA': random.randint(1, 20),
            'DESC_LINHA': 'GRAIN REFINERS',
            'RASTRO': random.choice(['L', 'S']),
            'UM': 'KG',
            'LOCAPAD': random.randint(1, 5),
            'QB': random.randint(1299, 1315),
            'METAL': random.choice(['S', 'N']),
            'LOCALIZ': random.choice(['S', 'N']),
            'REFUSAO': random.choice(['S', 'N']),
            'COD_COMPONENTE': random.randint(500000, 999999),
            'DESC_COMPONENTE': random.choice(["ALUMINIO PRIMARIO EM LINGOTES 99,7% - MI",
                "ONU 2923 SOLIDOS CORROSIVOS, TOXICO, N.E. (FLUORTITANATO DE POTASSIO) 8 II",
                "FLUORBORATO DE POTASSIO",
                "CRIOLITA BRUTA",
                "ALUMINIO FUNDIDO 01",
                "ONU 2862 PENTOXIDO DE VANADIO, NAO FUNDIDO 6.1 III",
                "TIBAL 5/1 ROD",
                "ONU 2923 SOLIDO CORROSIVO, TOXICO, N.E. (FLUORTITANATO DE POTASSIO) 8 6.1 II",
                "ALUMINIO PRIMARIO",
                "KBF4",
                "PENTOXIDO DE VANADIO",
                "CAIXA MADEIRA 1150X1150X765MM FECHADA P/ALUMINIO - FUMIGADO",
                "PALLET MADEIRA 770X770MM - FUMIGADO",
                "CAIXA MADEIRA 1010X1010X600MM (MED. EXT. 1100X1100X760MM) - FUMIGADO",
                "PALLET MADEIRA 560X560MM",
                "PALLET MADEIRA 900X450MM - FUMIGADO",
                "CAIXAS DE PAPELAO CAPACIDADE 500 KG CONFORME DESENHO AC-08-CX-00-005-07 -",
                "PALLET MADEIRA 1100X1100MM",
                "TAMBOR FIBRA 200L",
                "PALLET MADEIRA 1100X1100MM - FUMIGADO",
                "TAMPA/FUNDO DE PAPELAO PARA CAIXA DE 500 E 1.000 KG - MEDINDO 1,20 X 1,20 M",
                "TAMPA PARA 04 TAMBORES, MEDINDO 110 X 110CM - FUMIGADO",
                "TAMPA PARA 02 TAMBORES, MEDINDO 55 X 110CM - FUMIGADO",
                "CARRETEL DIN 8559 PSAI - PRETO, TIPO MIG DIAM EXT.300MMXDIAM INT.52MM",
                "CAIXA DE MADEIRA COM LATERAIS E TAMPA SEMI ABERTOS COM MEDIDAS INTERNAS 130 X 100 X 87 CM DE ALTURA",
                "CAIXA DE MADEIRA COM LATERAIS E TAMPA SEMIA BERTOS, COM MEDIDAS INTERNAS 130 X 100 X 45 CM DE ALTURA",
                "FLUXO ANTELIGAS",
                "CRIOLITA BRUTA",
                "ALUMINA CALCINADA - REF... ALCOA A1  - EMBALAGEM 01 PALLET COM 1380 KG SENDO 60 SACOS DE 23 KG",
                "TAMPA PARA 04 TAMBORES, MEDINDO 110 X 110CM",
                "TAMPA PARA 02 TAMBORES, MEDINDO 55 X 110CM",
                "TIBAL 5/1 ROD",
                "ALUMINIO PRIMARIO",
                "PALLET MADEIRA 560X560MM - FUMIGADO",
                "ALUMINIO PRIMARIO P1535"]),
            'QTD_COMPONENTE': round(random.uniform(0.1, 1000.0), 2)
        }
    elif table_name == 'MOVIMENTACAO':
        start_date = datetime.now() - timedelta(days=365)
        random_date = start_date + timedelta(days=random.randint(0, 365))
        
        return {
            'FILIAL': random.randint(1, 20000),
            'OP': random.randint(1_000_000_000, 9_999_999_999),
            'QUANT': round(random.uniform(1.0, 500.0), 2),
            'PERDA': round(random.uniform(0.0, 5.0), 2),
            'TM': random.randint(1, 1000),
            'CODIGO': random.randint(1000, 9999),
            'DESCRICAO': random.choice(['ONU 2923 SOLIDO CORROSIVO, TOXICO, N.E. (FLUORTITANATO DE POTASSIO) 8 6.1 II', 'CRIOLITA BRUTA']),
            'LOTE': random.randint(100000, 999999),
            'SUB_LOTE': random.randint(10000, 99999),
            'DT_EMISSAO': random_date,
            'CF': 'RE1',
            'LOCAL': random.randint(1, 3),
            'DOC': f"DOC{random.randint(1000000, 9999999)}",
            'NUMSEQ': f"SEQ{random.randint(100000, 999999)}",
            'QTD_PREVISTA': round(random.uniform(100.0, 5000.0), 2),
            'QTD_PRODUZIDA': round(random.uniform(80.0, 5000.0), 2),
            'DT_ENCERRAMENTO': random_date + timedelta(days=random.randint(1, 30)),
            'QTD_PLANEJADA': random.randint(100, 5000)
        }
    elif table_name == 'ESTOQUE':
        start_date = datetime.now() - timedelta(days=365)
        random_date = start_date + timedelta(days=random.randint(0, 365))
        
        return {
            'FILIAL': random.randint(1, 2000),
            'CODIGO': random.randint(1000000, 9999999),
            'LOCAL': random.randint(1, 10),
            'DATA': random_date,
            'SALDO': random.randint(100, 10000),
            'EMPENHO': random.randint(0, 5000),
            'LOTE': random.randint(1000000000, 9999999999),
            'NUMLOTE': random.randint(1, 999),
            'CLASSIFICACAO': random.choice(['A', 'B', 'C'])
        }
    elif table_name == 'VENDAS':
        start_date = datetime.now() - timedelta(days=365)
        random_date = start_date + timedelta(days=random.randint(0, 365))
        
        return {
            'Cliente': random.choice(['AIRGAS (MARINETTE)', 'ALMEXA ALUMINIO','NOVELIS KOREA','TRANSGULF','BOYNE SMELTERS LTD','VEDANTA LIMITED-SEZ UNIT','VEDANTA LIMITED','THAI METAL ALUMINIUM','STANCHEM SP. Z O. O.','NEMAK MEXICO'  ,'ALCOA POCOS'   ,'ALMEIDA METAIS','ALUAR ALUMINIO','ALUER ALUMINIO DO BRASIL','ALUFENIX','ALUMASA' ,'ALUMINIO 5 ESTRELAS','ALUMINIO ARARAS','ALUX DO BRASIL','AMG ALUMINUM CHINA''XINJIANG JOINWORLD''ZHONGFU INDUSTRY' ,'ALMEXA ALUMINIO','TRANSFORMACION PUEBLA' ,'FUNDIMET' ,'ALRO'  ,'VEDANTA LIMITED-SEZ UNIT','VEDANTA LIMITED','FRATELLI VEDANI','ARCELOR - MONLEVADE','CBA'   ,'CDPM - GO','COMETAIS' ,'CONDUMAX - ELETRO METALUR','DAGOFIX','DINAMICS' ,'GARBIT','GERDAU ACOMINAS','HUBNER','HYDRO - ITU'   ,'IBRAP' ,'IMPACTA' ,'IMPERI','INSIMBI','KADOSH','LAMINACAO CLEMENTE','LAMINACAO PAULISTA','MANGELS INDUSTRIAL','MAXION WHEELS' ,'MAXION WHEELS LIMEIRA' ,'MEGA ALUMINIO' ,'METAIS CAPIXABA','METAL 2','MODELACAO SOROCABANA','MOTO HONDA','NOVAMETAIS METALURGIA' ,'REBITOP','SAMOT - ALTREF','TERMOMECANICA SAO PAULO S','TRAMONTINA','VSB - UNIDADE JECEABA' ,'WETZEL','PINO LAR INDUSTRIA','ARMANDO MENDIGUREN E HIJO','APERAM SOUTH AMERICA','CORDEIRO FILIAL 01','CBA ITAPISSUMA LTDA','TEX','FILIAL CACHOEIRINHA','MDM CHEMICAL GROUP SRL','MINAS CONDUTORES ELETRICO','STABLE LUXURY TECHNOLOGY','PROLIND ALUMINIO LTDA','METALFLEX','BRALUMINIO']),
            'Produto': random.choice(['TIBAL 5/1 FIO 3MM CARRETEL 6KG - CX. MADEIRA','TIBAL 5/1 BOBINA 200KG - PALLET','CAAL 6% BARRA 20CM - CAIXA DE MADEIRA','CAAL 6% BOBINA 185KG - PALLET','LIGA 1350 BARRA 1M - AA F - PALLET','BAL 10% BARRA 3KG - PALLET','BAL 10% WP 7,5KG - PALLET','BAL 10% WP 7,5KG - PALLET','BAL 10% WP 7,5KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','SRAL 10% VARETA 0,5M - CAIXA DE MADEIRA','SRAL 10% VARETA 1M - CX. MADEIRA','SRAL 10% QUICKSOL 250G - CX. MADEIRA','TIBAL 3/1 BOBINA 200KG - PALLET','SRAL 10% VARETA 1M - PALLET','TIAL 10% WP 7,5KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','CUAL 33% WP 7,5KG - PALLET','SIAL 20% WP 7,5KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 VARETA 1M - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIAL 10% WP 7,5KG - PALLET','TIBAL 5/1 VARETA 1M - PALLET','CAAL 10% WP 7,5KG - PALLET','BAL 8% BARRA 3KG - PALLET','TIAL 10% WP 7,5KG - PALLET','TIBAL 5/02 BOBINA 200KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIAL 10% BARRA 200G - CX. MADEIRA','TIBAL 5/1 BOBINA 200KG - PALLET','BAL 8% WP 7,5KG - PALLET','BAL 10% WP 7,5KG - PALLET','BAL 10% WP 7,5KG - PALLET','SRAL 10% VARETA 1M - CX. MADEIRA','DESOXIDANTE 12 MM BOBINA 2000 KG VERTICAL - PALLET','FESIAL 15/7,5% WP 7,5KG - PALLET','SRAL 15% BARRA 3KG - PALLET','TIAL 10% WP 7,5KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 BOBINA 200KG F. FINAS- PALLET','TIBAL 5/1 VARETA 1M - PALLET','SRAL 10% VARETA 1M - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 VARETA 1M - PALLET','TIAL 10% WP 7,5KG - PALLET','LIGA 6201 9,5 MM BOBINA 2000KG T4 - PALLET','LIGA 5005 9,5 MM BOBINA 1000KG "F" - PALLET','LIGA 4043 9,5 MM BOBINA 1000KG "O" - PALLET','LIGA 5005 9,5 MM BOBINA 1000KG "F" - PALLET','DESOXIDANTE 12 MM JUMBO 2000KG - PALLET','SRAL 10% VARETA 1M - CAIXA DE PAPELAO 240KG','SRAL 10% VARETA 1M - PALLET','TIBAL 5/1 VARETA 1M - CAIXA DE PAPELAO 500KG','TIBAL 5/1 VARETA 1M - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','CUAL 33% WP 7,5KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','LIGA 6201 9,5 MM BOBINA 2000KG T1 - PALLET','SRAL 15% QUICKSOL 1/4LB (113G) - CX. MADEIRA','TIAL 10% BARRA 400G - CX. MADEIRA','TIBAL 5/1 VARETA 1M - CX. MADEIRA','LIGA 4043 9,5 MM BOBINA 1000KG "O" - PALLET','LIGA 6351 9,5 MM BOBINA 2000KG "F" -  PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','SRAL 10% BARRA 220G - BIG BAG','TIBAL 5/1 VARETA 1M - CX. MADEIRA','SRAL 10% BARRA 200G - TAMBOR DE FIBRA','TIBAL 5/1 VARETA 1M - PALLET','SRAL 10% BARRA 200G - TAMBOR DE FIBRA','TIBAL 5/1 VARETA 1M - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIAL 10% WP 7,5KG - PALLET','SRAL 10% VARETA 1M - PALLET','TIAL 10% BARRA 200G - TAMBOR DE FIBRA','SIAL 50% WP 7,5KG - PALLET','SRAL 10% VARETA 1M - PALLET','TIBAL 5/1 VARETA 1M - PALLET','SRAL 10% VARETA 1M - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','LIGA 5005 9,5 MM BOBINA 2000KG "O" - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIAL 10% BARRA 2 KG - TAMBOR DE FIBRA','DESOXIDANTE 9,5 MM JUMBO 2000KG - PALLET','SRAL 10% VARETA 1M - PALLET','LIGA 6351 9,5 MM BOBINA 1000KG "F" - PALLET','SRAL 10% VARETA 1M - CX. MADEIRA','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 VARETA 1M - CX. MADEIRA','DESOXIDANTE 9,5 MM BOBINA 1000KG - PALLET','LIGA 6201 9,5 MM BOBINA 2000 KG T4 - BITOLA 3,45 MM - PALLET','TIAL 10% WP 7,5KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 BOBINA 200KG FOLHAS - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','SRAL 10% VARETA 1M - PALLET','TIBAL 5/1 VARETA 1M - PALLET','SRAL 10% BOBINA 400LB (181KG) - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','BAL 4% WP 7,5KG - PALLET','TIAL 10% WP 7,5KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 VARETA 1M - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET','TIBAL 5/1 BOBINA 200KG - PALLET']),
            'Pedido': random.randint(100000, 999999),
            'Expedicao': random_date,
            'NF': random.randint(1000000, 9999999),
            'Qtd_Kg': random.randint(100, 50000),
            'Premio': round(random.uniform(1.0, 10.0), 4),
            'LME': round(random.uniform(1.0, 10.0), 5)
        }
    elif table_name == 'PRODUTIVIDADE':
        start_date = datetime.now() - timedelta(days=30)
        random_date = start_date + timedelta(days=random.randint(0, 30))
        
        # Gerar horas aleatórias no formato compatível com SQL Server
        hora_inicio = f"{random.randint(6, 18):02d}:{random.randint(0, 59):02d}:00"
        horas_duracao = random.randint(1, 8)
        hora_fim = (datetime.strptime(hora_inicio, "%H:%M:%S") + timedelta(hours=horas_duracao)).strftime("%H:%M:%S")
        duracao = f"{horas_duracao:02d}:00:00"  # Simplificando a duração
        
        return {
            'Data': random_date.strftime("%Y-%m-%d"),  # Formato YYYY-MM-DD
            'PRODUTO': random.choice(['BAl 8% WP','BAl 10% WP','BAl 10% Barra','FeSiAl 15/7,5% WP','Desoxidante 9,5mm','Desoxidante 12mm','Liga 5005 9,5mm (F)','Liga 6351 9,5mm - (F)','Altab Fe 75% - 6 pastilhas','Altab Mn 75% - 6 pastilhas','Altab Mn 75% - 6 past','Altab Mn 75% - 5 pastilhas','Altab Mn 75% - 5 past','Liga 6201 9,5mm - BT 3,45','Liga 5052 modificada','MgAl 50% WP','MgAl 90% (A) WP','AlBe 1%','VAl 10% WP','SrAl 10% Barra','SrAl 15% Barra','TiAl 10% WP','TiAl 10% Barra','CAAL 7,5% BARRA','TIBAL 5/1 ROD','TIBAL 5/1 BARRAS']),
            'OP': random.randint(1000000000, 9999999999),
            'LOTE': random.randint(1000000, 9999999),
            'Kg': random.randint(100, 5000),
            'Forno': random.choice(['1', '2', '3', '4', 'Prensa', '5/6', '6/5']),
            'OperadorForno': random.choice(['Lucas Ferreira', 'Mariana Oliveira', 'Gabriel Souza', 'Isabela Lima', 'Pedro Almeida', 'Ana Júlia Rocha', 'Felipe Barbosa', 'Camila Martins', 'Rafael Moreira', 'Larissa Teixeira', 'Vinícius Gomes', 'Beatriz Costa', 'Henrique Cardoso', 'Juliana Pinto', 'Thiago Mendes', 'Letícia Ribeiro', 'Bruno Carvalho', 'Yasmin Duarte', 'André Nunes', 'Natália Silveira']),
            'HoraInicio': hora_inicio,
            'HoraFim': hora_fim,
            'Duracao': duracao
        }

def insert_sample_data(conn, db_type, table_name, count=1):
    cursor = conn.cursor()
    inserted = 0
    
    try:
        for _ in range(count):
            data = generate_sample_data(table_name)
            
            if db_type == 'sqlserver':
                placeholders = ', '.join(['?'] * len(data))
                columns = ', '.join(data.keys())
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                values = tuple(data.values())
            elif db_type == 'mysql':
                placeholders = ', '.join(['%s'] * len(data))
                columns = ', '.join(data.keys())
                query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                values = tuple(data.values())
            
            try:
                cursor.execute(query, values)
                inserted += 1
            except Exception as e:
                print(f"Erro ao inserir registro na tabela {table_name}: {e}")
                conn.rollback()
                continue
        
        conn.commit()
        print(f"Inseridos {inserted} registros na tabela {table_name} no {db_type.upper()}.")
        
    except Exception as e:
        print(f"Erro geral ao inserir dados na tabela {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()
        
def get_random_record(conn, db_type, table_name):
    """Obtém um registro aleatório da tabela especificada"""
    cursor = conn.cursor()
    try:
        if db_type == 'sqlserver':
            if table_name == 'PRODUTOS':
                cursor.execute("SELECT TOP 1 FILIAL, CODIGO FROM PRODUTOS ORDER BY NEWID()")
            elif table_name == 'MOVIMENTACAO':
                cursor.execute("SELECT TOP 1 FILIAL, OP FROM MOVIMENTACAO ORDER BY NEWID()")
            elif table_name == 'ESTOQUE':
                cursor.execute("SELECT TOP 1 FILIAL, CODIGO, LOCAL, DATA FROM ESTOQUE ORDER BY NEWID()")
            elif table_name == 'VENDAS':
                cursor.execute("SELECT TOP 1 Cliente, Produto, Pedido FROM VENDAS ORDER BY NEWID()")
            elif table_name == 'PRODUTIVIDADE':
                cursor.execute("SELECT TOP 1 Data, PRODUTO, OP FROM PRODUTIVIDADE ORDER BY NEWID()")
        elif db_type == 'mysql':
            if table_name == 'PRODUTOS':
                cursor.execute("SELECT FILIAL, CODIGO FROM PRODUTOS ORDER BY RAND() LIMIT 1")
            elif table_name == 'MOVIMENTACAO':
                cursor.execute("SELECT FILIAL, OP FROM MOVIMENTACAO ORDER BY RAND() LIMIT 1")
            elif table_name == 'ESTOQUE':
                cursor.execute("SELECT FILIAL, CODIGO, LOCAL, DATA FROM ESTOQUE ORDER BY RAND() LIMIT 1")
            elif table_name == 'VENDAS':
                cursor.execute("SELECT Cliente, Produto, Pedido FROM VENDAS ORDER BY RAND() LIMIT 1")
            elif table_name == 'PRODUTIVIDADE':
                cursor.execute("SELECT Data, PRODUTO, OP FROM PRODUTIVIDADE ORDER BY RAND() LIMIT 1")
        
        return cursor.fetchone()
    except Exception as e:
        print(f"Erro ao obter registro aleatório: {e}")
        return None
    finally:
        cursor.close()

def remove_random_record(conn, db_type, table_name):
    """Remove um registro aleatório da tabela especificada"""
    cursor = conn.cursor()
    try:
        # Obter chave primária de um registro aleatório
        primary_key = get_random_record(conn, db_type, table_name)
        
        if not primary_key:
            print(f"Nenhum registro encontrado na tabela {table_name} para remover.")
            return
        
        # Construir a condição WHERE baseada na tabela
        if table_name == 'PRODUTOS':
            condition = f"FILIAL = {primary_key[0]} AND CODIGO = {primary_key[1]}"
        elif table_name == 'MOVIMENTACAO':
            condition = f"FILIAL = {primary_key[0]} AND OP = {primary_key[1]}"
        elif table_name == 'ESTOQUE':
            condition = f"FILIAL = {primary_key[0]} AND CODIGO = {primary_key[1]} AND LOCAL = {primary_key[2]} AND DATA = '{primary_key[3]}'"
        elif table_name == 'VENDAS':
            condition = f"Cliente = '{primary_key[0]}' AND Produto = '{primary_key[1]}' AND Pedido = {primary_key[2]}"
        elif table_name == 'PRODUTIVIDADE':
            condition = f"Data = '{primary_key[0]}' AND PRODUTO = '{primary_key[1]}' AND OP = {primary_key[2]}"
        
        # Executar a remoção
        delete_query = f"DELETE FROM {table_name} WHERE {condition}"
        cursor.execute(delete_query)
        conn.commit()
        print(f"Registro removido com sucesso da tabela {table_name}!")
        
    except Exception as e:
        print(f"Erro ao remover registro da tabela {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def delete_all_records(conn, db_type, table_name):
    """Remove todos os registros da tabela especificada"""
    cursor = conn.cursor()
    try:
        confirm = input(f"Tem certeza que deseja apagar TODOS os registros da tabela {table_name}? (s/n): ")
        if confirm.lower() == 's':
            cursor.execute(f"DELETE FROM {table_name}")
            conn.commit()
            print(f"Todos os registros foram removidos com sucesso da tabela {table_name}!")
    except Exception as e:
        print(f"Erro ao remover registros da tabela {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def update_random_field(conn, db_type, table_name):
    """Atualiza um campo aleatório de um registro aleatório"""
    cursor = conn.cursor()
    try:
        # Obter chave primária de um registro aleatório
        primary_key = get_random_record(conn, db_type, table_name)
        
        if not primary_key:
            print(f"Nenhum registro encontrado na tabela {table_name} para atualizar.")
            return
        
        # Construir a condição WHERE baseada na tabela
        if table_name == 'PRODUTOS':
            condition = f"FILIAL = {primary_key[0]} AND CODIGO = {primary_key[1]}"
            fields = ['DESCRICAO', 'TIPO', 'COD_DIVISAO', 'QB', 'QTD_COMPONENTE']
        elif table_name == 'MOVIMENTACAO':
            condition = f"FILIAL = {primary_key[0]} AND OP = {primary_key[1]}"
            fields = ['QUANT', 'PERDA', 'TM', 'QTD_PRODUZIDA', 'DT_ENCERRAMENTO']
        elif table_name == 'ESTOQUE':
            condition = f"FILIAL = {primary_key[0]} AND CODIGO = {primary_key[1]} AND LOCAL = {primary_key[2]} AND DATA = '{primary_key[3]}'"
            fields = ['SALDO', 'EMPENHO', 'CLASSIFICACAO']
        elif table_name == 'VENDAS':
            condition = f"Cliente = '{primary_key[0]}' AND Produto = '{primary_key[1]}' AND Pedido = {primary_key[2]}"
            fields = ['Qtd_Kg', 'Premio', 'LME']
        elif table_name == 'PRODUTIVIDADE':
            condition = f"Data = '{primary_key[0]}' AND PRODUTO = '{primary_key[1]}' AND OP = {primary_key[2]}"
            fields = ['Kg', 'Forno', 'OperadorForno', 'Duracao']
        
        # Selecionar um campo aleatório para atualizar
        field_to_update = random.choice(fields)
        new_value = None
        
        # Gerar novo valor baseado no tipo do campo
        if field_to_update in ['DESCRICAO', 'TIPO', 'CLASSIFICACAO', 'Forno', 'OperadorForno']:
            new_value = f"'Novo Valor {random.randint(1, 100)}'"
        elif field_to_update in ['QB', 'TM']:
            new_value = random.randint(1, 10)
        elif field_to_update in ['QUANT', 'PERDA', 'QTD_COMPONENTE', 'QTD_PRODUZIDA', 'Premio', 'LME']:
            new_value = round(random.uniform(0.1, 100.0), 2)
        elif field_to_update in ['SALDO', 'EMPENHO', 'Qtd_Kg', 'Kg']:
            new_value = random.randint(1, 10000)
        elif field_to_update == 'DT_ENCERRAMENTO':
            new_value = f"'{datetime.now().strftime('%Y-%m-%d')}'"
        elif field_to_update == 'Duracao':
            new_value = f"'{random.randint(1, 8):02d}:00:00'"
        
        # Executar a atualização
        update_query = f"UPDATE {table_name} SET {field_to_update} = {new_value} WHERE {condition}"
        cursor.execute(update_query)
        conn.commit()
        print(f"Campo {field_to_update} atualizado com sucesso no registro selecionado da tabela {table_name}!")
        
    except Exception as e:
        print(f"Erro ao atualizar registro na tabela {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()

def main():
    sqlserver_conn = create_sqlserver_connection()
    mysql_conn = create_mysql_connection()
    
    if sqlserver_conn:
        create_tables(sqlserver_conn, 'sqlserver')
    
    while True:
        print("\nMenu Principal")
        print("1. SQL Server")
        print("2. MySQL")
        print("3. Sair")
        
        db_choice = input("Escolha o banco de dados (1-3): ")
        
        if db_choice == '3':
            break
        elif db_choice not in ['1', '2']:
            print("Opção inválida. Tente novamente.")
            continue
        
        db_type = 'sqlserver' if db_choice == '1' else 'mysql'
        conn = sqlserver_conn if db_choice == '1' else mysql_conn
        
        if not conn:
            print(f"Não foi possível conectar ao {db_type.upper()}. Verifique as credenciais.")
            continue
        
        while True:
            print(f"\nOperações no {db_type.upper()}")
            print("1. Inserir dados em PRODUTOS")
            print("2. Inserir dados em MOVIMENTACAO")
            print("3. Inserir dados em ESTOQUE")
            print("4. Inserir dados em VENDAS")
            print("5. Inserir dados em PRODUTIVIDADE")
            print("6. Remover registro aleatório")
            print("7. Remover todos os registros")
            print("8. Atualizar campo aleatório")
            print("9. Voltar")
            
            operation = input("Escolha a operação (1-9): ")
            
            if operation == '9':
                break
            elif operation in ['1', '2', '3', '4', '5']:
                tables = {
                    '1': 'PRODUTOS',
                    '2': 'MOVIMENTACAO',
                    '3': 'ESTOQUE',
                    '4': 'VENDAS',
                    '5': 'PRODUTIVIDADE'
                }
                table_name = tables[operation]
                
                try:
                    count = int(input(f"Quantos registros deseja inserir em {table_name}? (1-1000) ") or "1")
                    count = max(1, min(1000, count))
                    insert_sample_data(conn, db_type, table_name, count)
                except ValueError:
                    print("Por favor, insira um número válido.")
            elif operation == '6':
                table_name = input("Digite o nome da tabela (PRODUTOS, MOVIMENTACAO, ESTOQUE, VENDAS, PRODUTIVIDADE): ").upper()
                if table_name in ['PRODUTOS', 'MOVIMENTACAO', 'ESTOQUE', 'VENDAS', 'PRODUTIVIDADE']:
                    remove_random_record(conn, db_type, table_name)
                else:
                    print("Tabela inválida.")
            elif operation == '7':
                table_name = input("Digite o nome da tabela (PRODUTOS, MOVIMENTACAO, ESTOQUE, VENDAS, PRODUTIVIDADE): ").upper()
                if table_name in ['PRODUTOS', 'MOVIMENTACAO', 'ESTOQUE', 'VENDAS', 'PRODUTIVIDADE']:
                    delete_all_records(conn, db_type, table_name)
                else:
                    print("Tabela inválida.")
            elif operation == '8':
                table_name = input("Digite o nome da tabela (PRODUTOS, MOVIMENTACAO, ESTOQUE, VENDAS, PRODUTIVIDADE): ").upper()
                if table_name in ['PRODUTOS', 'MOVIMENTACAO', 'ESTOQUE', 'VENDAS', 'PRODUTIVIDADE']:
                    update_random_field(conn, db_type, table_name)
                else:
                    print("Tabela inválida.")
            else:
                print("Opção inválida. Tente novamente.")
            
            time.sleep(1)
    
    if sqlserver_conn:
        sqlserver_conn.close()
    if mysql_conn:
        mysql_conn.close()
    print("Conexões encerradas. Aplicação finalizada.")

if __name__ == "__main__":
    main()