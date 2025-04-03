# Sincronizador de Bancos de Dados SQL Server para MySQL

## Visão Geral

Este script Python realiza a sincronização contínua de dados entre bancos de dados SQL Server e MySQL, utilizando o Change Data Capture (CDC) do SQL Server para identificar e replicar apenas as alterações ocorridas nos dados. O sistema é projetado para operar em tempo real com um intervalo configurável entre sincronizações.

## Funcionalidades Principais

- **Sincronização inicial completa**: Cópia integral dos dados das tabelas configuradas
- **Sincronização incremental**: Replicação contínua apenas das alterações detectadas
- **Mapeamento flexível**: Configuração personalizada de tabelas e colunas entre os bancos
- **Controle de sincronização**: Rastreamento do ponto de sincronização usando LSN (Log Sequence Number)
- **Resiliência**: Tratamento robusto de erros e reconexão automática
- **Log detalhado**: Registro completo de operações para auditoria e troubleshooting

## Pré-requisitos

### Dependências de Software

- Python 3.7 ou superior
- Bibliotecas Python:
  - `mysql-connector-python`
  - `pyodbc`
  - `argparse`
  - `logging`

Instale as dependências com:
```bash
pip install mysql-connector-python pyodbc
```

### Configuração do SQL Server

1. Habilitar CDC no banco de dados de origem:
```sql
-- Habilitar CDC no nível do banco de dados
EXEC sys.sp_cdc_enable_db;

-- Habilitar CDC para cada tabela a ser sincronizada
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'nome_da_tabela',
    @role_name = NULL;
```

2. Certificar-se que o usuário especificado tem permissões:
   - SELECT nas tabelas de origem
   - Acesso às tabelas CDC (`cdc.*`)

### Configuração do MySQL

1. Criar o banco de dados de destino e conceder permissões ao usuário especificado
2. As tabelas de destino serão criadas automaticamente durante a sincronização inicial

## Configuração

### Arquivo de Mapeamento (JSON)

O arquivo de mapeamento (`mapeamento.json` por padrão) define como as tabelas e colunas serão sincronizadas entre os bancos. Exemplo:

```json
{
    "tabela_mysql": {
        "coluna1_mysql": {
            "campoOrigem": "coluna1_sqlserver",
            "tabelaOrigem": "tabela_sqlserver"
        },
        "coluna2_mysql": {
            "campoOrigem": "coluna2_sqlserver",
            "tabelaOrigem": "tabela_sqlserver"
        },
        "__$last_sync": {}
    }
}
```

### Parâmetros de Execução

O script aceita os seguintes argumentos de linha de comando:

#### Conexão SQL Server
- `--sqlserver-host`: Host do SQL Server (obrigatório)
- `--sqlserver-port`: Porta do SQL Server (padrão: 1433)
- `--sqlserver-database`: Nome do banco de dados (obrigatório)
- `--sqlserver-user`: Usuário (obrigatório)
- `--sqlserver-password`: Senha (obrigatório)

#### Conexão MySQL
- `--mysql-host`: Host do MySQL (obrigatório)
- `--mysql-port`: Porta do MySQL (padrão: 3306)
- `--mysql-database`: Nome do banco de dados (obrigatório)
- `--mysql-user`: Usuário (obrigatório)
- `--mysql-password`: Senha (obrigatório)

#### Configuração do Sincronizador
- `--intervalo`: Intervalo entre sincronizações em segundos (padrão: 15)
- `--mapeamento`: Caminho do arquivo de mapeamento (padrão: mapeamento.json)
- `--log`: Caminho do arquivo de log (padrão: sincronizador.log)

## Fluxo de Execução

1. **Inicialização**:
   - Parse dos argumentos de linha de comando
   - Configuração do sistema de logging
   - Carregamento do arquivo de mapeamento
   - Conexão com ambos os bancos de dados

2. **Preparação**:
   - Verificação e criação da coluna `__$last_sync` em cada tabela de destino no MySQL
   - Sincronização inicial completa para todas as tabelas mapeadas

3. **Loop Principal**:
   - Para cada tabela configurada:
     - Identifica alterações desde o último LSN registrado
     - Aplica as alterações (INSERT/UPDATE/DELETE) no MySQL
     - Atualiza o LSN de sincronização
   - Aguarda o intervalo configurado antes do próximo ciclo

## Estrutura do Código

### Funções Principais

1. **`configurar_argumentos()`**:
   - Define e parseia os argumentos de linha de comando
   - Configura valores padrão quando aplicável

2. **`configurar_logging(log_file)`**:
   - Configura sistema de logging com rotação de arquivos
   - Limite de 10MB por arquivo de log, mantendo até 5 backups

3. **`carregar_mapeamento(arquivo_mapeamento)`**:
   - Carrega e valida o arquivo JSON de mapeamento
   - Encerra o programa em caso de erro

4. **`conectar_sql_server()`** / **`conectar_mysql()`**:
   - Estabelecem conexões com os respectivos bancos
   - Utilizam parâmetros fornecidos via linha de comando

5. **`verificar_e_ajustar_coluna_sincronizacao(mysql_table)`**:
   - Garante a existência da coluna especial `__$last_sync` no MySQL
   - Cria ou ajusta o tipo da coluna conforme necessário

6. **`obter_lsn_atual()`**:
   - Obtém o LSN máximo atual do SQL Server usando `sys.fn_cdc_get_max_lsn()`

7. **`sincronizacao_inicial(mysql_table)`**:
   - Realiza cópia completa dos dados da tabela de origem para destino
   - Limpa a tabela de destino antes da sincronização
   - Registra o LSN inicial após conclusão

8. **`obter_tabela_cdc(tabela_origem)`**:
   - Consulta as tabelas CDC para encontrar a tabela de captura correspondente

9. **`obter_ultima_sincronizacao(mysql_table)`**:
   - Recupera o último LSN sincronizado da tabela de destino

10. **`sincronizar_tabela(mysql_table)`**:
    - Identifica alterações desde a última sincronização
    - Aplica as operações (insert/update/delete) no MySQL
    - Atualiza o LSN após processamento bem-sucedido

11. **`main()`**:
    - Orquestra todo o fluxo de execução
    - Gerencia o loop principal de sincronização
    - Trata interrupções e encerramento adequado

## Tratamento de Erros

O sistema implementa múltiplas camadas de tratamento de erros:

1. **Log detalhado**: Todas as operações são registradas com timestamp
   - Níveis: INFO (operações normais), WARNING (problemas recuperáveis), ERROR (falhas graves)
   - Stack traces completos para exceções

2. **Encerramento controlado**: Conexões são sempre fechadas adequadamente
   - Mesmo em caso de interrupção pelo usuário (Ctrl+C)
   - Ou em falhas não tratadas

3. **Validações**:
   - Verificação de mapeamentos existentes
   - Confirmação de tabelas CDC habilitadas
   - Checagem de tipos de dados

## Considerações de Desempenho

1. **Intervalo de sincronização**: Ajustável conforme necessidade
   - Valores menores para maior atualidade dos dados
   - Valores maiores para reduzir carga nos servidores

2. **Processamento em lote**: Todas as alterações são processadas em cada ciclo
   - Minimiza round-trips entre os bancos
   - Atualiza LSN apenas após processamento completo

3. **Logging assíncrono**: Não bloqueia o fluxo principal

## Limitações

1. **Tipos de dados**: Nem todos os tipos do SQL Server têm correspondência direta no MySQL
   - Pode requerer ajustes no mapeamento

2. **Esquema dinâmico**: Alterações na estrutura das tabelas requerem:
   - Atualização do arquivo de mapeamento
   - Reinício do sincronizador

3. **Dependência do CDC**: Requer configuração prévia no SQL Server

## Exemplo de Uso

```bash
python sincronizador.py \
    --sqlserver-host servidor-sql \
    --sqlserver-database banco_origem \
    --sqlserver-user usuario \
    --sqlserver-password senha123 \
    --mysql-host servidor-mysql \
    --mysql-database banco_destino \
    --mysql-user usuario \
    --mysql-password senha456 \
    --intervalo 30 \
    --mapeamento /caminho/mapeamento.json \
    --log /caminho/logs/sincronizador.log
```

## Monitoramento

O arquivo de log contém todas as informações necessárias para monitoramento:

- Tempo de cada ciclo de sincronização
- Quantidade de registros processados
- Erros ocorridos
- Pontos de sincronização (LSN)

Exemplo de entrada de log:
```
2023-05-15 14:30:45 - INFO - Iniciando ciclo de sincronização...
2023-05-15 14:30:47 - INFO - Sincronização concluída para 'tabela_clientes'. Novo LSN: 0x0000002A000000960003
2023-05-15 14:30:47 - INFO - Ciclo de sincronização concluído em 2.34 segundos. Aguardando próximo ciclo...
```

## Considerações Finais

Este sincronizador é ideal para cenários onde:
- É necessária replicação próxima do tempo real entre SQL Server e MySQL
- A infraestrutura do SQL Server suporta CDC
- As tabelas de destino no MySQL podem ser dedicadas para a sincronização

Para ambientes de produção, recomenda-se:
1. Implementar monitoramento adicional do processo
2. Considerar alta disponibilidade para o sincronizador
3. Realizar testes de carga com volumes de dados similares ao produção