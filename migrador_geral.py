import psycopg2
from psycopg2 import sql, pool
import oracledb
from oracledb import create_pool
import re
from flask import Flask, request, render_template_string, redirect, url_for
import threading
import time
import os
import sys
import logging
from typing import Dict, List, Tuple, Optional

oracledb.init_oracle_client(lib_dir='C:\\instantclient_23_8')

app = Flask(__name__)

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variáveis globais para armazenar o estado da migração
migration_status = {
    'in_progress': False,
    'completed': False,
    'logs': [],
    'tables_created': 0,
    'tables_failed': 0,
    'tables_data_migrated': 0,
    'tables_data_failed': 0,
    'total_tables': 0
}

# Pools de conexão
connection_pools = {
    'postgresql_source': None,
    'postgresql_dest': None,
    'oracle_source': None
}

# HTML templates (mantidos intactos)
INDEX_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Migração de Banco de Dados</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        .form-group { margin-bottom: 15px; }
        label { display: block; margin-bottom: 5px; font-weight: bold; }
        input[type="text"], input[type="password"], select { 
            width: 300px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; 
        }
        .db-section { 
            background-color: #f5f5f5; padding: 15px; margin-bottom: 20px; border-radius: 5px; 
        }
        .db-title { 
            font-size: 18px; color: #555; margin-bottom: 15px; 
        }
        button { 
            background-color: #4CAF50; color: white; padding: 10px 15px; 
            border: none; border-radius: 4px; cursor: pointer; 
        }
        button:hover { background-color: #45a049; }
        .migration-type { margin-bottom: 20px; }
    </style>
</head>
<body>
    <h1>Migração de Banco de Dados</h1>
    
    <form method="POST" action="/list_tables">
        <div class="migration-type">
            <div class="form-group">
                <label for="migration_type">Tipo de Migração:</label>
                <select id="migration_type" name="migration_type" required onchange="toggleOracleFields()">
                    <option value="postgres_to_postgres">PostgreSQL → PostgreSQL</option>
                    <option value="oracle_to_postgres">Oracle → PostgreSQL</option>
                </select>
            </div>
        </div>
        
        <div class="db-section" id="source-db-section">
            <div class="db-title" id="source-db-title">Banco de Dados de Origem (PostgreSQL)</div>
            <div class="form-group">
                <label for="source_host">Host:</label>
                <input type="text" id="source_host" name="source_host" required>
            </div>
            <div class="form-group">
                <label for="source_dbname">Nome do Banco:</label>
                <input type="text" id="source_dbname" name="source_dbname" required>
            </div>
            <div class="form-group">
                <label for="source_user">Usuário:</label>
                <input type="text" id="source_user" name="source_user" required>
            </div>
            <div class="form-group">
                <label for="source_password">Senha:</label>
                <input type="password" id="source_password" name="source_password" required>
            </div>
            <div class="form-group">
                <label for="source_schema">Schema:</label>
                <input type="text" id="source_schema" name="source_schema" required>
            </div>
            <div class="form-group" id="oracle_tns_group" style="display: none;">
                <label for="oracle_tns">TNS/DSN (Oracle):</label>
                <input type="text" id="oracle_tns" name="oracle_tns" placeholder="Ex: localhost/XEPDB1">
            </div>
            <div class="form-group" id="oracle_lib_dir_group" style="display: none;">
                <label for="oracle_lib_dir">Caminho Instant Client (Oracle - opcional):</label>
                <input type="text" id="oracle_lib_dir" name="oracle_lib_dir" placeholder="Ex: C:\instantclient_23_8">
            </div>
        </div>
        
        <div class="db-section">
            <div class="db-title">Banco de Dados de Destino (PostgreSQL)</div>
            <div class="form-group">
                <label for="dest_host">Host:</label>
                <input type="text" id="dest_host" name="dest_host" required>
            </div>
            <div class="form-group">
                <label for="dest_dbname">Nome do Banco:</label>
                <input type="text" id="dest_dbname" name="dest_dbname" required>
            </div>
            <div class="form-group">
                <label for="dest_user">Usuário:</label>
                <input type="text" id="dest_user" name="dest_user" required>
            </div>
            <div class="form-group">
                <label for="dest_password">Senha:</label>
                <input type="password" id="dest_password" name="dest_password" required>
            </div>
            <div class="form-group">
                <label for="dest_schema">Schema:</label>
                <input type="text" id="dest_schema" name="dest_schema" required>
            </div>
        </div>
        
        <button type="submit">Listar Tabelas</button>
    </form>
    
    <script>
        function toggleOracleFields() {
            const migrationType = document.getElementById('migration_type').value;
            const oracleTnsGroup = document.getElementById('oracle_tns_group');
            const oracleLibDirGroup = document.getElementById('oracle_lib_dir_group');
            const sourceDbTitle = document.getElementById('source-db-title');
            
            if (migrationType === 'oracle_to_postgres') {
                oracleTnsGroup.style.display = 'block';
                oracleLibDirGroup.style.display = 'block';
                sourceDbTitle.textContent = 'Banco de Dados de Origem (Oracle)';
                document.getElementById('source_dbname').required = false;
                document.getElementById('oracle_tns').required = true;
            } else {
                oracleTnsGroup.style.display = 'none';
                oracleLibDirGroup.style.display = 'none';
                sourceDbTitle.textContent = 'Banco de Dados de Origem (PostgreSQL)';
                document.getElementById('source_dbname').required = true;
                document.getElementById('oracle_tns').required = false;
            }
        }
    </script>
</body>
</html>
'''

TABLES_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Selecionar Tabelas para Migração</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        .table-list { margin: 20px 0; }
        .table-item { margin: 5px 0; }
        button { 
            background-color: #4CAF50; color: white; padding: 10px 15px; 
            border: none; border-radius: 4px; cursor: pointer; margin-top: 20px;
        }
        button:hover { background-color: #45a049; }
        .select-all { margin-bottom: 10px; }
    </style>
</head>
<body>
    <h1>Selecionar Tabelas para Migração</h1>
    <form method="POST" action="/migrate">
        <input type="hidden" name="migration_type" value="{{ migration_type }}">
        <input type="hidden" name="source_host" value="{{ source_host }}">
        <input type="hidden" name="source_dbname" value="{{ source_dbname }}">
        <input type="hidden" name="source_user" value="{{ source_user }}">
        <input type="hidden" name="source_password" value="{{ source_password }}">
        <input type="hidden" name="source_schema" value="{{ source_schema }}">
        <input type="hidden" name="oracle_tns" value="{{ oracle_tns }}">
        <input type="hidden" name="oracle_lib_dir" value="{{ oracle_lib_dir }}">
        <input type="hidden" name="dest_host" value="{{ dest_host }}">
        <input type="hidden" name="dest_dbname" value="{{ dest_dbname }}">
        <input type="hidden" name="dest_user" value="{{ dest_user }}">
        <input type="hidden" name="dest_password" value="{{ dest_password }}">
        <input type="hidden" name="dest_schema" value="{{ dest_schema }}">
        
        <div class="select-all">
            <input type="checkbox" id="select_all" onclick="toggleSelectAll()">
            <label for="select_all">Selecionar Todas</label>
        </div>
        
        <div class="table-list">
            {% for table in tables %}
            <div class="table-item">
                <input type="checkbox" id="table_{{ loop.index }}" name="selected_tables" value="{{ table[0] }}.{{ table[1] }}">
                <label for="table_{{ loop.index }}">{{ table[0] }}.{{ table[1] }}</label>
            </div>
            {% endfor %}
        </div>
        
        <button type="submit">Iniciar Migração</button>
    </form>
    
    <script>
        function toggleSelectAll() {
            const selectAll = document.getElementById('select_all');
            const checkboxes = document.querySelectorAll('input[name="selected_tables"]');
            checkboxes.forEach(checkbox => {
                checkbox.checked = selectAll.checked;
            });
        }
    </script>
</body>
</html>
'''

RESULTS_HTML = '''
<!DOCTYPE html>
<html>
<head>
    <title>Resultado da Migração</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        .log-container { 
            background-color: #f5f5f5; padding: 15px; border-radius: 5px; 
            max-height: 400px; overflow-y: auto; margin: 20px 0;
        }
        .log-entry { margin: 5px 0; }
        .success { color: green; }
        .error { color: red; }
        .info { color: blue; }
        .stats { 
            background-color: #e9e9e9; padding: 10px; border-radius: 5px; 
            margin: 20px 0;
        }
    </style>
    <script>
        function refreshLogs() {
            fetch('/migration_logs')
                .then(response => response.json())
                .then(data => {
                    const logContainer = document.getElementById('log-container');
                    logContainer.innerHTML = '';
                    
                    data.logs.forEach(log => {
                        const logEntry = document.createElement('div');
                        logEntry.className = 'log-entry ' + log.type;
                        logEntry.textContent = log.message;
                        logContainer.appendChild(logEntry);
                    });
                    
                    document.getElementById('tables-created').textContent = data.tables_created;
                    document.getElementById('tables-failed').textContent = data.tables_failed;
                    document.getElementById('tables-data-migrated').textContent = data.tables_data_migrated;
                    document.getElementById('tables-data-failed').textContent = data.tables_data_failed;
                    document.getElementById('total-tables').textContent = data.total_tables;
                    
                    if (data.in_progress) {
                        setTimeout(refreshLogs, 2000);
                    }
                });
        }
        
        window.onload = function() {
            refreshLogs();
        };
    </script>
</head>
<body>
    <h1>Resultado da Migração</h1>
    
    <div class="stats">
        <h3>Estatísticas:</h3>
        <p>Tabelas criadas com sucesso: <span id="tables-created">0</span></p>
        <p>Tabelas com erro na criação: <span id="tables-failed">0</span></p>
        <p>Tabelas com dados migrados: <span id="tables-data-migrated">0</span></p>
        <p>Tabelas com erro na migração de dados: <span id="tables-data-failed">0</span></p>
        <p>Total processado: <span id="total-tables">0</span></p>
    </div>
    
    <h3>Logs:</h3>
    <div class="log-container" id="log-container">
        {% for log in logs %}
        <div class="log-entry {{ log.type }}">{{ log.message }}</div>
        {% endfor %}
    </div>
    
    <a href="/">Voltar ao início</a>
</body>
</html>
'''

# Mapeamento completo de tipos Oracle para PostgreSQL
COMPREHENSIVE_TYPE_MAPPING = {
    # Tipos caractere
    'VARCHAR2': 'VARCHAR',
    'NVARCHAR2': 'VARCHAR', 
    'CHAR': 'CHAR',
    'NCHAR': 'CHAR',
    'VARCHAR': 'VARCHAR',
    
    # Tipos numéricos
    'NUMBER': 'NUMERIC',
    'NUMERIC': 'NUMERIC',
    'FLOAT': 'DOUBLE PRECISION',
    'BINARY_FLOAT': 'REAL',
    'BINARY_DOUBLE': 'DOUBLE PRECISION',
    'DECIMAL': 'NUMERIC',
    'INTEGER': 'INTEGER',
    'INT': 'INTEGER',
    'SMALLINT': 'SMALLINT',
    'REAL': 'REAL',
    
    # Tipos data/hora
    'DATE': 'TIMESTAMP',
    'TIMESTAMP': 'TIMESTAMP',
    'TIMESTAMP WITH TIME ZONE': 'TIMESTAMPTZ',
    'TIMESTAMP WITH LOCAL TIME ZONE': 'TIMESTAMPTZ',
    'TIMESTAMPTZ': 'TIMESTAMPTZ',
    'TIMESTAMPLTZ': 'TIMESTAMPTZ',
    
    # Tipos LOB
    'CLOB': 'TEXT',
    'BLOB': 'BYTEA',
    'LONG': 'TEXT',
    'LONG RAW': 'BYTEA',
    'RAW': 'BYTEA',
    'BFILE': 'TEXT',  # Mapeado para TEXT pois PostgreSQL não tem equivalente direto
    
    # Tipos especiais
    'ROWID': 'TEXT',
    'UROWID': 'TEXT',
    'XMLTYPE': 'XML',
    'JSON': 'JSON',
    'JSONB': 'JSONB'
}

class DatabaseManager:
    """Gerenciador robusto de conexões de banco de dados"""
    
    @staticmethod
    def create_postgresql_connection_pool(host: str, dbname: str, user: str, password: str, 
                                        port: int = 5432, min_conn: int = 1, max_conn: int = 10) -> Optional[pool.SimpleConnectionPool]:
        """Cria pool de conexões PostgreSQL"""
        try:
            return pool.SimpleConnectionPool(
                min_conn, max_conn,
                host=host, database=dbname, user=user, password=password,
                port=port, connect_timeout=10, sslmode='prefer'
            )
        except Exception as e:
            add_log(f"❌ Erro ao criar pool PostgreSQL: {e}", 'error')
            return None
    
    @staticmethod
    def create_oracle_connection_pool(user: str, password: str, dsn: str, lib_dir: str = None,
                                    min_conn: int = 1, max_conn: int = 10) -> Optional[any]:
        """Cria pool de conexões Oracle"""
        try:
            # Configurar cliente Oracle se lib_dir for fornecido
            if lib_dir and os.path.exists(lib_dir):
                try:
                    oracledb.init_oracle_client(lib_dir=lib_dir)
                    add_log(f"✅ Cliente Oracle inicializado: {lib_dir}")
                except Exception as e:
                    add_log(f"⚠️  Não foi possível inicializar cliente Oracle: {e}")
            
            return create_pool(
                user=user, password=password, dsn=dsn,
                min=min_conn, max=max_conn, increment=1
            )
        except Exception as e:
            add_log(f"❌ Erro ao criar pool Oracle: {e}", 'error')
            return None
    
    @staticmethod
    def get_postgresql_connection(connection_pool: pool.SimpleConnectionPool, timeout: int = 30):
        """Obtém conexão do pool PostgreSQL com timeout"""
        try:
            return connection_pool.getconn(timeout=timeout)
        except Exception as e:
            add_log(f"❌ Erro ao obter conexão PostgreSQL: {e}", 'error')
            return None
    
    @staticmethod
    def get_oracle_connection(connection_pool, timeout: int = 30):
        """Obtém conexão do pool Oracle com timeout"""
        try:
            return connection_pool.acquire(timeout=timeout)
        except Exception as e:
            add_log(f"❌ Erro ao obter conexão Oracle: {e}", 'error')
            return None
    
    @staticmethod
    def release_postgresql_connection(connection_pool: pool.SimpleConnectionPool, connection):
        """Libera conexão PostgreSQL de volta para o pool"""
        try:
            if connection and not connection.closed:
                connection_pool.putconn(connection)
        except Exception as e:
            add_log(f"⚠️  Erro ao liberar conexão PostgreSQL: {e}")
    
    @staticmethod
    def release_oracle_connection(connection_pool, connection):
        """Libera conexão Oracle de volta para o pool"""
        try:
            if connection:
                connection_pool.release(connection)
        except Exception as e:
            add_log(f"⚠️  Erro ao liberar conexão Oracle: {e}")

def add_log(message: str, type: str = 'info'):
    """Adiciona uma mensagem de log"""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {'message': f"[{timestamp}] {message}", 'type': type}
    migration_status['logs'].append(log_entry)
    logger.log(
        logging.INFO if type == 'info' else logging.ERROR if type == 'error' else logging.WARNING,
        message
    )

def testar_conexoes(source_config: Dict, dest_config: Dict, migration_type: str) -> bool:
    """Testa todas as conexões antes de iniciar a migração"""
    add_log("🔍 Testando conexões com os bancos de dados...")
    
    success = True
    
    # Testar PostgreSQL destino
    try:
        test_conn = psycopg2.connect(
            host=dest_config['host'], database=dest_config['dbname'],
            user=dest_config['user'], password=dest_config['password'],
            port=dest_config.get('port', 5432), connect_timeout=10
        )
        test_conn.close()
        add_log("✅ Conexão PostgreSQL destino: OK")
    except Exception as e:
        add_log(f"❌ Conexão PostgreSQL destino falhou: {e}", 'error')
        success = False
    
    # Testar origem baseado no tipo de migração
    if migration_type == 'postgres_to_postgres':
        try:
            test_conn = psycopg2.connect(
                host=source_config['host'], database=source_config['dbname'],
                user=source_config['user'], password=source_config['password'],
                port=source_config.get('port', 5432), connect_timeout=10
            )
            test_conn.close()
            add_log("✅ Conexão PostgreSQL origem: OK")
        except Exception as e:
            add_log(f"❌ Conexão PostgreSQL origem falhou: {e}", 'error')
            success = False
    else:  # oracle_to_postgres
        try:
            # Tentar modo thin primeiro
            if source_config.get('lib_dir') and os.path.exists(source_config['lib_dir']):
                try:
                    oracledb.init_oracle_client(lib_dir=source_config['lib_dir'])
                except:
                    pass
            
            dsn = source_config['tns']
            if ':' not in dsn and '/' in dsn:
                parts = dsn.split('/')
                dsn = f"{parts[0]}:1521/{parts[1]}"
            
            test_conn = oracledb.connect(
                user=source_config['user'], password=source_config['password'], dsn=dsn
            )
            test_conn.close()
            add_log("✅ Conexão Oracle origem: OK")
        except Exception as e:
            add_log(f"❌ Conexão Oracle origem falhou: {e}", 'error')
            success = False
    
    return success

def listar_tabelas_postgres(host: str, dbname: str, user: str, password: str, schema: str = None) -> List[Tuple]:
    """Lista todas as tabelas disponíveis no PostgreSQL com conexão robusta"""
    conn = None
    try:
        conn = psycopg2.connect(
            host=host, dbname=dbname, user=user, password=password,
            port=5432, connect_timeout=10, sslmode='prefer'
        )
        cursor = conn.cursor()
        
        if schema:
            cursor.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema = %s 
                ORDER BY table_schema, table_name
            """, (schema,))
        else:
            cursor.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name
            """)
        
        tabelas = cursor.fetchall()
        cursor.close()
        return tabelas
        
    except Exception as e:
        add_log(f"❌ Erro ao listar tabelas PostgreSQL: {e}", 'error')
        return []
    finally:
        if conn:
            conn.close()

def listar_tabelas_oracle(user: str, password: str, tns: str, lib_dir: str = None, schema: str = None) -> List[Tuple]:
    """Lista todas as tabelas disponíveis no Oracle com conexão robusta"""
    conn = None
    try:
        # Configurar cliente Oracle se necessário
        if lib_dir and os.path.exists(lib_dir):
            try:
                oracledb.init_oracle_client(lib_dir=lib_dir)
            except Exception as e:
                add_log(f"⚠️  Não foi possível inicializar cliente Oracle: {e}")
        
        # Formatar DSN se necessário
        dsn = tns
        if ':' not in dsn and '/' in dsn:
            parts = dsn.split('/')
            dsn = f"{parts[0]}:1521/{parts[1]}"
        
        conn = oracledb.connect(user=user, password=password, dsn=dsn)
        cursor = conn.cursor()
        
        if schema:
            cursor.execute("""
                SELECT owner, table_name 
                FROM all_tables 
                WHERE owner = UPPER(:schema) 
                ORDER BY owner, table_name
            """, schema=schema)
        else:
            cursor.execute("""
                SELECT owner, table_name 
                FROM all_tables 
                WHERE owner NOT IN ('SYS', 'SYSTEM')
                ORDER BY owner, table_name
            """)
        
        tabelas = cursor.fetchall()
        cursor.close()
        return tabelas
        
    except Exception as e:
        add_log(f"❌ Erro ao listar tabelas Oracle: {str(e)}", 'error')
        return []
    finally:
        if conn:
            conn.close()

def listar_tabelas_banco(migration_type: str, host: str, dbname: str, user: str, password: str, 
                        schema: str = None, oracle_tns: str = None, oracle_lib_dir: str = None) -> List[Tuple]:
    """Lista tabelas baseado no tipo de migração"""
    if migration_type == 'postgres_to_postgres':
        return listar_tabelas_postgres(host, dbname, user, password, schema)
    elif migration_type == 'oracle_to_postgres':
        return listar_tabelas_oracle(user, password, oracle_tns, oracle_lib_dir, schema)
    else:
        add_log(f"❌ Tipo de migração inválido: {migration_type}", 'error')
        return []

def criar_sequencias_necessarias(tabela: str, schema_origem: str, schema_destino: str, 
                               source_conn, dest_conn, migration_type: str) -> bool:
    """Cria sequências necessárias para a tabela no schema de destino"""
    try:
        if migration_type == 'postgres_to_postgres':
            source_cursor = source_conn.cursor()
            dest_cursor = dest_conn.cursor()
            
            # Buscar colunas com valores padrão que usam sequências
            source_cursor.execute("""
                SELECT column_name, column_default, data_type
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = %s 
                AND column_default LIKE 'nextval(%'
            """, (schema_origem, tabela))
            
            sequencias = source_cursor.fetchall()
            
            for coluna, default_value, data_type in sequencias:
                match = re.search(r"nextval\('([^']+)'::regclass\)", str(default_value))
                if match:
                    sequencia_nome = match.group(1)
                    sequencia_nome_sem_schema = sequencia_nome.split('.')[-1]
                    
                    add_log(f"    🔍 Detectada sequência: {sequencia_nome}")
                    
                    # Verificar se a sequência já existe no destino
                    dest_cursor.execute("""
                        SELECT COUNT(*) 
                        FROM pg_sequences 
                        WHERE schemaname = %s AND sequencename = %s
                    """, (schema_destino, sequencia_nome_sem_schema))
                    
                    if dest_cursor.fetchone()[0] == 0:
                        add_log(f"    ⚙️  Criando sequência: {schema_destino}.{sequencia_nome_sem_schema}")
                        
                        try:
                            source_cursor.execute("SELECT * FROM {} LIMIT 1".format(sequencia_nome))
                            seq_info = source_cursor.fetchone()
                            
                            if seq_info:
                                create_seq_sql = sql.SQL("CREATE SEQUENCE {}.{}").format(
                                    sql.Identifier(schema_destino),
                                    sql.Identifier(sequencia_nome_sem_schema)
                                )
                                dest_cursor.execute(create_seq_sql)
                                dest_conn.commit()
                                add_log(f"    ✅ Sequência criada com sucesso")
                                
                        except Exception as seq_error:
                            add_log(f"    ⚠️  Criando sequência com valores padrão: {seq_error}")
                            try:
                                create_seq_sql = sql.SQL("CREATE SEQUENCE IF NOT EXISTS {}.{}").format(
                                    sql.Identifier(schema_destino),
                                    sql.Identifier(sequencia_nome_sem_schema)
                                )
                                dest_cursor.execute(create_seq_sql)
                                dest_conn.commit()
                                add_log(f"    ✅ Sequência criada com IF NOT EXISTS")
                            except Exception as create_error:
                                add_log(f"    ❌ Erro ao criar sequência: {create_error}", 'error')
                    else:
                        add_log(f"    ✅ Sequência {schema_destino}.{sequencia_nome_sem_schema} já existe")
            
            source_cursor.close()
            dest_cursor.close()
        
        return True
        
    except Exception as e:
        add_log(f"❌ Erro ao criar sequências para {tabela}: {e}", 'error')
        return False

def obter_chaves_primarias(tabela: str, schema_origem: str, source_conn, migration_type: str) -> List[str]:
    """Obtém as chaves primárias da tabela"""
    try:
        if migration_type == 'postgres_to_postgres':
            cursor = source_conn.cursor()
            cursor.execute("""
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = %s::regclass AND i.indisprimary
            """, (f"{schema_origem}.{tabela}",))
            
            chaves_primarias = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return chaves_primarias
            
        elif migration_type == 'oracle_to_postgres':
            cursor = source_conn.cursor()
            cursor.execute("""
                SELECT cols.column_name
                FROM all_constraints cons
                JOIN all_cons_columns cols ON cons.constraint_name = cols.constraint_name
                WHERE cons.owner = UPPER(:owner) 
                AND cols.table_name = UPPER(:table_name)
                AND cons.constraint_type = 'P'
            """, owner=schema_origem, table_name=tabela)
            
            chaves_primarias = [row[0].lower() for row in cursor.fetchall()]
            cursor.close()
            return chaves_primarias
            
    except Exception as e:
        add_log(f"⚠️  Erro ao obter chaves primárias: {e}", 'error')
        return []

def migrar_dados_postgres_para_postgres(tabela: str, schema_origem: str, schema_destino: str, 
                                      source_conn, dest_conn) -> bool:
    """Migra dados de PostgreSQL para PostgreSQL de forma transacional"""
    source_cursor = None
    dest_cursor = None
    
    try:
        add_log(f"    📦 Migrando dados da tabela: {schema_origem}.{tabela}")
        
        # Iniciar transação
        dest_conn.autocommit = False
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        # Verificar se a tabela existe no destino
        dest_cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema_destino, tabela))
        
        if not dest_cursor.fetchone()[0]:
            add_log(f"    ⏭️  Tabela {schema_destino}.{tabela} não existe no destino", 'error')
            return False
        
        # Obter colunas
        source_cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s 
            ORDER BY ordinal_position
        """, (schema_origem, tabela))
        
        colunas = [row[0] for row in source_cursor.fetchall()]
        colunas_str = ', '.join([f'"{col}"' for col in colunas])
        
        # Contar registros
        source_cursor.execute(f"SELECT COUNT(*) FROM {schema_origem}.{tabela}")
        total_registros = source_cursor.fetchone()[0]
        
        if total_registros == 0:
            add_log(f"    ℹ️  Nenhum registro para migrar")
            dest_conn.commit()
            return True
        
        add_log(f"    📊 Total de registros: {total_registros}")
        
        # Limpar tabela de destino
        dest_cursor.execute(f"TRUNCATE TABLE {schema_destino}.{tabela}")
        
        # Migrar dados em lotes
        placeholders = ', '.join(['%s'] * len(colunas))
        insert_query = f"INSERT INTO {schema_destino}.{tabela} ({colunas_str}) VALUES ({placeholders})"
        
        source_cursor.execute(f"SELECT {colunas_str} FROM {schema_origem}.{tabela}")
        
        lote_size = 1000
        registros_migrados = 0
        registros = source_cursor.fetchmany(lote_size)
        
        while registros:
            try:
                dest_cursor.executemany(insert_query, registros)
                registros_migrados += len(registros)
                
                if registros_migrados % 5000 == 0:
                    add_log(f"    ✅ {registros_migrados}/{total_registros} registros migrados")
                
                registros = source_cursor.fetchmany(lote_size)
            except Exception as insert_error:
                add_log(f"    ❌ Erro ao inserir lote: {insert_error}", 'error')
                dest_conn.rollback()
                return False
        
        dest_conn.commit()
        add_log(f"    ✅ Todos os {registros_migrados} registros migrados com sucesso", 'success')
        return True
        
    except Exception as e:
        add_log(f"❌ Erro ao migrar dados da tabela {tabela}: {e}", 'error')
        if dest_conn:
            dest_conn.rollback()
        return False
    finally:
        if source_cursor:
            source_cursor.close()
        if dest_cursor:
            dest_cursor.close()

def migrar_dados_oracle_para_postgres(tabela: str, schema_origem: str, schema_destino: str, 
                                    source_conn, dest_conn) -> bool:
    """Migra dados de Oracle para PostgreSQL de forma transacional"""
    source_cursor = None
    dest_cursor = None
    
    try:
        add_log(f"    📦 Migrando dados da tabela Oracle: {schema_origem}.{tabela}")
        
        # Iniciar transação
        dest_conn.autocommit = False
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        # Verificar se a tabela existe no destino
        dest_cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema_destino, tabela.lower()))
        
        if not dest_cursor.fetchone()[0]:
            add_log(f"    ⏭️  Tabela {schema_destino}.{tabela} não existe no destino", 'error')
            return False
        
        # Obter colunas
        source_cursor.execute("""
            SELECT column_name 
            FROM all_tab_columns 
            WHERE owner = UPPER(:owner) AND table_name = UPPER(:table_name) 
            ORDER BY column_id
        """, owner=schema_origem, table_name=tabela)
        
        colunas = [row[0] for row in source_cursor.fetchall()]
        colunas_str = ', '.join([f'"{col.lower()}"' for col in colunas])
        colunas_oracle_str = ', '.join([f'"{col}"' for col in colunas])
        
        # Contar registros
        source_cursor.execute(f"SELECT COUNT(*) FROM {schema_origem}.{tabela}")
        total_registros = source_cursor.fetchone()[0]
        
        if total_registros == 0:
            add_log(f"    ℹ️  Nenhum registro para migrar")
            dest_conn.commit()
            return True
        
        add_log(f"    📊 Total de registros: {total_registros}")
        
        # Limpar tabela de destino
        dest_cursor.execute(f"TRUNCATE TABLE {schema_destino}.{tabela.lower()}")
        
        # Migrar dados em lotes
        placeholders = ', '.join(['%s'] * len(colunas))
        insert_query = f"INSERT INTO {schema_destino}.{tabela.lower()} ({colunas_str}) VALUES ({placeholders})"
        
        source_cursor.execute(f"SELECT {colunas_oracle_str} FROM {schema_origem}.{tabela}")
        
        lote_size = 1000
        registros_migrados = 0
        registros = source_cursor.fetchmany(lote_size)
        
        while registros:
            try:
                # Converter tipos Oracle
                registros_convertidos = []
                for registro in registros:
                    registro_convertido = []
                    for valor in registro:
                        if isinstance(valor, oracledb.LOB):
                            try:
                                registro_convertido.append(valor.read())
                            except:
                                registro_convertido.append(None)
                        elif valor is None:
                            registro_convertido.append(None)
                        else:
                            registro_convertido.append(valor)
                    registros_convertidos.append(tuple(registro_convertido))
                
                dest_cursor.executemany(insert_query, registros_convertidos)
                registros_migrados += len(registros_convertidos)
                
                if registros_migrados % 5000 == 0:
                    add_log(f"    ✅ {registros_migrados}/{total_registros} registros migrados")
                
                registros = source_cursor.fetchmany(lote_size)
            except Exception as insert_error:
                add_log(f"    ❌ Erro ao inserir lote: {insert_error}", 'error')
                dest_conn.rollback()
                return False
        
        dest_conn.commit()
        add_log(f"    ✅ Todos os {registros_migrados} registros migrados com sucesso", 'success')
        return True
        
    except Exception as e:
        add_log(f"❌ Erro ao migrar dados da tabela Oracle {tabela}: {str(e)}", 'error')
        if dest_conn:
            dest_conn.rollback()
        return False
    finally:
        if source_cursor:
            source_cursor.close()
        if dest_cursor:
            dest_cursor.close()

def criar_tabela_postgres_para_postgres(tabela: str, schema_origem: str, schema_destino: str, 
                                      source_conn, dest_conn) -> bool:
    """Cria tabela PostgreSQL para PostgreSQL"""
    source_cursor = None
    dest_cursor = None
    
    try:
        add_log(f"\n🔄 Processando tabela: {schema_origem}.{tabela}")
        
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        # Verificar se a tabela existe na origem
        source_cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema_origem, tabela))
        
        if not source_cursor.fetchone()[0]:
            add_log(f"⏭️  Tabela {schema_origem}.{tabela} não existe na origem")
            return False
        
        # Obter informações das colunas
        source_cursor.execute("""
            SELECT column_name, data_type, is_nullable, column_default,
                   character_maximum_length, numeric_precision, numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s 
            ORDER BY ordinal_position
        """, (schema_origem, tabela))
        
        colunas_info = source_cursor.fetchall()
        colunas_def = []
        
        for coluna in colunas_info:
            nome, tipo, nullable, default_value, char_max_length, num_precision, num_scale = coluna
            
            # Construir tipo PostgreSQL
            if tipo == 'character varying':
                pg_tipo = f'VARCHAR({char_max_length})' if char_max_length else 'VARCHAR'
            elif tipo == 'character':
                pg_tipo = f'CHAR({char_max_length})' if char_max_length else 'CHAR'
            elif tipo == 'numeric':
                if num_precision and num_scale:
                    pg_tipo = f'NUMERIC({num_precision}, {num_scale})'
                elif num_precision:
                    pg_tipo = f'NUMERIC({num_precision})'
                else:
                    pg_tipo = 'NUMERIC'
            else:
                pg_tipo = tipo.upper()
            
            col_def = f'"{nome}" {pg_tipo}'
            
            if nullable == 'NO':
                col_def += ' NOT NULL'
            
            if default_value:
                col_def += f' DEFAULT {default_value}'
            
            colunas_def.append(col_def)
        
        # Criar tabela
        create_sql = f'CREATE TABLE IF NOT EXISTS {schema_destino}.{tabela} ({", ".join(colunas_def)})'
        
        try:
            dest_cursor.execute(create_sql)
            dest_conn.commit()
            add_log(f"✅ Tabela {schema_destino}.{tabela} criada com sucesso!", 'success')
            return True
        except Exception as create_error:
            add_log(f"❌ Erro ao criar tabela: {create_error}", 'error')
            return False
        
    except Exception as e:
        add_log(f"❌ Erro ao processar tabela {tabela}: {e}", 'error')
        return False
    finally:
        if source_cursor:
            source_cursor.close()
        if dest_cursor:
            dest_cursor.close()

def criar_tabela_oracle_para_postgres(tabela: str, schema_origem: str, schema_destino: str, 
                                    source_conn, dest_conn) -> bool:
    """Cria tabela Oracle para PostgreSQL"""
    source_cursor = None
    dest_cursor = None
    
    try:
        add_log(f"\n🔄 Processando tabela Oracle: {schema_origem}.{tabela}")
        
        source_cursor = source_conn.cursor()
        dest_cursor = dest_conn.cursor()
        
        # Obter informações das colunas
        source_cursor.execute("""
            SELECT column_name, data_type, data_length, data_precision,
                   data_scale, nullable, data_default
            FROM all_tab_columns 
            WHERE owner = UPPER(:owner) AND table_name = UPPER(:table_name) 
            ORDER BY column_id
        """, owner=schema_origem, table_name=tabela)
        
        colunas_info = source_cursor.fetchall()
        
        if not colunas_info:
            add_log(f"❌ Nenhuma coluna encontrada para {tabela}", 'error')
            return False
        
        colunas_def = []
        
        for coluna in colunas_info:
            nome, tipo_oracle, data_length, data_precision, data_scale, nullable, data_default = coluna
            
            nome = nome.lower()
            tipo_base = tipo_oracle.split('(')[0] if '(' in tipo_oracle else tipo_oracle
            pg_tipo = COMPREHENSIVE_TYPE_MAPPING.get(tipo_base.upper(), 'TEXT')
            
            # Ajustar tipo com precisão
            if pg_tipo in ['VARCHAR', 'CHAR'] and data_length:
                pg_tipo = f"{pg_tipo}({data_length})"
            elif pg_tipo == 'NUMERIC' and data_precision is not None:
                if data_scale is not None and data_scale > 0:
                    pg_tipo = f"NUMERIC({data_precision}, {data_scale})"
                elif data_precision is not None:
                    pg_tipo = f"NUMERIC({data_precision})"
            
            col_def = f'"{nome}" {pg_tipo}'
            
            if nullable == 'N':
                col_def += ' NOT NULL'
            
            if data_default is not None:
                default_value = str(data_default).strip()
                if default_value.upper() not in ['NULL', '']:
                    col_def += f' DEFAULT {default_value}'
            
            colunas_def.append(col_def)
        
        # Criar tabela
        tabela_destino = tabela.lower()
        colunas_sql = ', '.join(colunas_def)
        create_sql = f'CREATE TABLE IF NOT EXISTS {schema_destino}.{tabela_destino} ({colunas_sql})'
        
        try:
            dest_cursor.execute(create_sql)
            dest_conn.commit()
            add_log(f"✅ Tabela {schema_destino}.{tabela_destino} criada com sucesso!", 'success')
            return True
        except Exception as create_error:
            add_log(f"❌ Erro ao criar tabela: {create_error}", 'error')
            return False
        
    except Exception as e:
        add_log(f"❌ Erro ao processar tabela Oracle {tabela}: {str(e)}", 'error')
        return False
    finally:
        if source_cursor:
            source_cursor.close()
        if dest_cursor:
            dest_cursor.close()

def migrar_tabela_segura(tabela: str, schema_origem: str, schema_destino: str,
                        source_conn, dest_conn, migration_type: str) -> bool:
    """Migra uma tabela de forma segura com transação"""
    try:
        # 1. Criar tabela
        if migration_type == 'postgres_to_postgres':
            sucesso_criacao = criar_tabela_postgres_para_postgres(tabela, schema_origem, schema_destino, source_conn, dest_conn)
        else:
            sucesso_criacao = criar_tabela_oracle_para_postgres(tabela, schema_origem, schema_destino, source_conn, dest_conn)
        
        if not sucesso_criacao:
            return False
        
        # 2. Migrar dados
        if migration_type == 'postgres_to_postgres':
            sucesso_dados = migrar_dados_postgres_para_postgres(tabela, schema_origem, schema_destino, source_conn, dest_conn)
        else:
            sucesso_dados = migrar_dados_oracle_para_postgres(tabela, schema_origem, schema_destino, source_conn, dest_conn)
        
        return sucesso_dados
        
    except Exception as e:
        add_log(f"💥 Erro crítico na migração de {tabela}: {e}", 'error')
        return False

def run_migration(migration_type: str, source_params: Dict, dest_params: Dict, selected_tables: List[str]):
    """Executa a migração em uma thread separada"""
    global migration_status
    
    migration_status['in_progress'] = True
    migration_status['completed'] = False
    migration_status['logs'] = []
    migration_status['tables_created'] = 0
    migration_status['tables_failed'] = 0
    migration_status['tables_data_migrated'] = 0
    migration_status['tables_data_failed'] = 0
    migration_status['total_tables'] = len(selected_tables)
    
    add_log("🚀 Iniciando processo de migração")
    add_log(f"📋 Tipo: {'PostgreSQL → PostgreSQL' if migration_type == 'postgres_to_postgres' else 'Oracle → PostgreSQL'}")
    
    # Testar conexões antes de iniciar
    if not testar_conexoes(source_params, dest_params, migration_type):
        add_log("❌ Teste de conexões falhou. Migração cancelada.", 'error')
        migration_status['in_progress'] = False
        return
    
    source_conn = None
    dest_conn = None
    
    try:
        # Estabelecer conexões
        if migration_type == 'postgres_to_postgres':
            source_conn = psycopg2.connect(
                host=source_params['host'], database=source_params['dbname'],
                user=source_params['user'], password=source_params['password'],
                port=5432, connect_timeout=10
            )
        else:
            dsn = source_params['tns']
            if ':' not in dsn and '/' in dsn:
                parts = dsn.split('/')
                dsn = f"{parts[0]}:1521/{parts[1]}"
            
            if source_params.get('lib_dir') and os.path.exists(source_params['lib_dir']):
                try:
                    oracledb.init_oracle_client(lib_dir=source_params['lib_dir'])
                except:
                    pass
            
            source_conn = oracledb.connect(
                user=source_params['user'], password=source_params['password'], dsn=dsn
            )
        
        dest_conn = psycopg2.connect(
            host=dest_params['host'], database=dest_params['dbname'],
            user=dest_params['user'], password=dest_params['password'],
            port=5432, connect_timeout=10
        )
        
        add_log("✅ Conexões estabelecidas com sucesso")
        
        # Processar tabelas
        for table_full_name in selected_tables:
            schema_origem, tabela = table_full_name.split('.', 1)
            
            sucesso = migrar_tabela_segura(
                tabela, schema_origem, dest_params['schema'],
                source_conn, dest_conn, migration_type
            )
            
            if sucesso:
                migration_status['tables_created'] += 1
                migration_status['tables_data_migrated'] += 1
            else:
                migration_status['tables_failed'] += 1
                migration_status['tables_data_failed'] += 1
            
            add_log("─" * 40)
        
    except Exception as e:
        add_log(f"💥 Erro crítico durante a migração: {e}", 'error')
    finally:
        # Fechar conexões
        if source_conn:
            try:
                source_conn.close()
            except:
                pass
        if dest_conn:
            try:
                dest_conn.close()
            except:
                pass
        
        migration_status['in_progress'] = False
        migration_status['completed'] = True
        add_log("🎉 Processo de migração concluído!", 'success')

# Rotas Flask (mantidas intactas)
@app.route('/')
def index():
    return render_template_string(INDEX_HTML)

@app.route('/list_tables', methods=['POST'])
def list_tables():
    migration_type = request.form['migration_type']
    
    source_params = {
        'host': request.form['source_host'],
        'dbname': request.form['source_dbname'],
        'user': request.form['source_user'],
        'password': request.form['source_password'],
        'schema': request.form['source_schema'],
        'oracle_tns': request.form.get('oracle_tns', ''),
        'oracle_lib_dir': request.form.get('oracle_lib_dir', '')
    }
    
    dest_params = {
        'host': request.form['dest_host'],
        'dbname': request.form['dest_dbname'],
        'user': request.form['dest_user'],
        'password': request.form['dest_password'],
        'schema': request.form['dest_schema']
    }
    
    tabelas = listar_tabelas_banco(
        migration_type,
        source_params['host'], source_params['dbname'], source_params['user'], 
        source_params['password'], source_params['schema'],
        source_params['oracle_tns'], source_params['oracle_lib_dir']
    )
    
    if not tabelas:
        add_log("❌ Nenhuma tabela encontrada!", 'error')
        return redirect('/')
    
    return render_template_string(
        TABLES_HTML, 
        tables=tabelas,
        migration_type=migration_type,
        source_host=source_params['host'],
        source_dbname=source_params['dbname'],
        source_user=source_params['user'],
        source_password=source_params['password'],
        source_schema=source_params['schema'],
        oracle_tns=source_params['oracle_tns'],
        oracle_lib_dir=source_params['oracle_lib_dir'],
        dest_host=dest_params['host'],
        dest_dbname=dest_params['dbname'],
        dest_user=dest_params['user'],
        dest_password=dest_params['password'],
        dest_schema=dest_params['schema']
    )

@app.route('/migrate', methods=['POST'])
def migrate():
    migration_type = request.form['migration_type']
    
    source_params = {
        'host': request.form['source_host'],
        'dbname': request.form['source_dbname'],
        'user': request.form['source_user'],
        'password': request.form['source_password'],
        'schema': request.form['source_schema'],
        'oracle_tns': request.form.get('oracle_tns', ''),
        'oracle_lib_dir': request.form.get('oracle_lib_dir', '')
    }
    
    dest_params = {
        'host': request.form['dest_host'],
        'dbname': request.form['dest_dbname'],
        'user': request.form['dest_user'],
        'password': request.form['dest_password'],
        'schema': request.form['dest_schema']
    }
    
    selected_tables = request.form.getlist('selected_tables')
    
    if not selected_tables:
        add_log("❌ Nenhuma tabela selecionada!", 'error')
        return redirect('/')
    
    thread = threading.Thread(
        target=run_migration, 
        args=(migration_type, source_params, dest_params, selected_tables)
    )
    thread.daemon = True
    thread.start()
    
    return render_template_string(RESULTS_HTML, logs=migration_status['logs'])

@app.route('/migration_logs')
def migration_logs():
    return {
        'logs': migration_status['logs'],
        'in_progress': migration_status['in_progress'],
        'completed': migration_status['completed'],
        'tables_created': migration_status['tables_created'],
        'tables_failed': migration_status['tables_failed'],
        'tables_data_migrated': migration_status['tables_data_migrated'],
        'tables_data_failed': migration_status['tables_data_failed'],
        'total_tables': migration_status['total_tables']
    }

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=5000)