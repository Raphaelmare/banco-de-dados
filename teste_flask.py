# app.py
from flask import Flask, render_template_string, request, session, redirect, url_for
import psycopg2
from psycopg2 import sql
import re
import traceback
from datetime import datetime

app = Flask(__name__)
app.config['SECRET_KEY'] = 'sua-chave-secreta-aqui-mude-em-producao'
app.config['PERMANENT_SESSION_LIFETIME'] = 86400  # 24 horas

# Funções de gerenciamento de conexões
def salvar_conexao(nome, dados):
    if 'conexoes' not in session:
        session['conexoes'] = {}
    session['conexoes'][nome] = {
        'host': dados['host'],
        'port': dados.get('port', '5432'),
        'dbname': dados['dbname'],
        'user': dados['user'],
        'password': dados.get('password', ''),
        'schema': dados.get('schema', 'public'),
        'data_criacao': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    session.modified = True

def carregar_conexoes():
    return session.get('conexoes', {})

def obter_conexao(nome):
    return session.get('conexoes', {}).get(nome, None)

# Funções de banco de dados
def conectar(conn_params):
    try:
        return psycopg2.connect(
            host=conn_params['host'],
            database=conn_params['dbname'],
            user=conn_params['user'],
            password=conn_params.get('password', ''),
            port=conn_params.get('port', 5432)
        )
    except Exception as e:
        raise Exception(f"Erro ao conectar: {e}")

def testar_conexao(conn_params):
    try:
        conn = conectar(conn_params)
        conn.close()
        return True, "Conexão bem-sucedida!"
    except Exception as e:
        return False, f"Falha na conexão: {str(e)}"

def listar_tabelas_banco(conn_params, schema=None):
    try:
        conn = conectar(conn_params)
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
        conn.close()
        
        return tabelas
    except Exception as e:
        raise Exception(f"Erro ao listar tabelas: {e}")

def criar_tabela_postgres(tabela, origin_params, dest_params, schema_origem='public', schema_destino='public'):
    try:
        # Conexões - origem e destino PostgreSQL
        pg_origem_conn = conectar(origin_params)
        pg_destino_conn = conectar(dest_params)
        
        pg_origem_cursor = pg_origem_conn.cursor()
        pg_destino_cursor = pg_destino_conn.cursor()
        
        # Verificar se a tabela existe no PostgreSQL de origem
        pg_origem_cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema_origem, tabela))
        
        if not pg_origem_cursor.fetchone()[0]:
            return False, f"Tabela {schema_origem}.{tabela} não existe na origem"
        
        # Obter informações das colunas do PostgreSQL
        pg_origem_cursor.execute("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s 
            ORDER BY ordinal_position
        """, (schema_origem, tabela))
        
        colunas_info = pg_origem_cursor.fetchall()
        
        # Construir definições das colunas
        colunas_def = []
        for coluna in colunas_info:
            nome, tipo, nullable, default_val, char_max_length, num_precision, num_scale = coluna
            
            # Construir o tipo de dados apropriado
            if tipo == 'character varying':
                if char_max_length:
                    pg_tipo = f'VARCHAR({char_max_length})'
                else:
                    pg_tipo = 'VARCHAR'
            elif tipo == 'character':
                if char_max_length:
                    pg_tipo = f'CHAR({char_max_length})'
                else:
                    pg_tipo = 'CHAR'
            elif tipo == 'numeric':
                if num_precision and num_scale:
                    pg_tipo = f'NUMERIC({num_precision}, {num_scale})'
                elif num_precision:
                    pg_tipo = f'NUMERIC({num_precision})'
                else:
                    pg_tipo = 'NUMERIC'
            else:
                pg_tipo = tipo.upper()
            
            # Adicionar constraints
            col_def = f'"{nome}" {pg_tipo}'
            if nullable == 'NO':
                col_def += ' NOT NULL'
            if default_val:
                # Corrigir referências de sequência se necessário
                if 'nextval' in str(default_val):
                    match = re.search(r"nextval\('([^']+)'::regclass\)", str(default_val))
                    if match:
                        sequencia_nome = match.group(1)
                        sequencia_nome_sem_schema = sequencia_nome.split('.')[-1]
                        default_val = f"nextval('{schema_destino}.{sequencia_nome_sem_schema}'::regclass)"
                col_def += f' DEFAULT {default_val}'
            
            colunas_def.append(col_def)
        
        # Criar schema de destino se não existir
        pg_destino_cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_destino)))
        pg_destino_conn.commit()
        
        # Criar tabela no PostgreSQL de destino  
        create_sql = f'CREATE TABLE IF NOT EXISTS {schema_destino}.{tabela} ({", ".join(colunas_def)})'
        pg_destino_cursor.execute(create_sql)
        pg_destino_conn.commit()
        
        return True, f"Tabela {schema_destino}.{tabela} criada com sucesso!"
    except Exception as e:
        return False, f"Erro ao criar tabela {tabela}: {str(e)}"
    finally:
        if 'pg_origem_cursor' in locals(): pg_origem_cursor.close()
        if 'pg_destino_cursor' in locals(): pg_destino_cursor.close()
        if 'pg_origem_conn' in locals(): pg_origem_conn.close()
        if 'pg_destino_conn' in locals(): pg_destino_conn.close()

def criar_sequencias_necessarias(tabela, schema_origem, schema_destino, origin_params, dest_params):
    """Cria sequências necessárias para a tabela no schema de destino"""
    try:
        pg_origem_conn = conectar(origin_params)
        pg_destino_conn = conectar(dest_params)
        pg_origem_cursor = pg_origem_conn.cursor()
        pg_destino_cursor = pg_destino_conn.cursor()
        # Buscar colunas com valores padrão que usam sequências
        pg_origem_cursor.execute("""
            SELECT column_name, column_default, data_type
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s 
            AND column_default LIKE 'nextval(%'
        """, (schema_origem, tabela))
        sequencias = pg_origem_cursor.fetchall()
        for coluna, default_value, data_type in sequencias:
            match = re.search(r"nextval\('([^']+)'::regclass\)", str(default_value))
            if match:
                sequencia_nome = match.group(1)
                sequencia_nome_sem_schema = sequencia_nome.split('.')[-1]
                # Verificar se a sequência já existe no destino
                pg_destino_cursor.execute("""
                    SELECT COUNT(*) 
                    FROM pg_sequences 
                    WHERE schemaname = %s AND sequencename = %s
                """, (schema_destino, sequencia_nome_sem_schema))
                if pg_destino_cursor.fetchone()[0] == 0:
                    try:
                        pg_origem_cursor.execute(f"""
                            SELECT 
                                increment_by,
                                min_value,
                                max_value,
                                start_value,
                                cache_size
                            FROM {sequencia_nome}
                        """)
                        seq_info = pg_origem_cursor.fetchone()
                        if seq_info:
                            increment, min_val, max_val, start_val, cache = seq_info
                            create_seq_sql = sql.SQL("""
                                CREATE SEQUENCE {}.{} 
                                INCREMENT %s 
                                MINVALUE %s 
                                MAXVALUE %s 
                                START %s 
                                CACHE %s
                            """).format(
                                sql.Identifier(schema_destino),
                                sql.Identifier(sequencia_nome_sem_schema)
                            )
                            pg_destino_cursor.execute(create_seq_sql, (increment, min_val, max_val, start_val, cache))
                            pg_destino_conn.commit()
                    except Exception:
                        try:
                            create_seq_default_sql = sql.SQL("CREATE SEQUENCE {}.{} ").format(
                                sql.Identifier(schema_destino),
                                sql.Identifier(sequencia_nome_sem_schema)
                            )
                            pg_destino_cursor.execute(create_seq_default_sql)
                            pg_destino_conn.commit()
                        except Exception:
                            try:
                                create_seq_if_not_exists = sql.SQL("CREATE SEQUENCE IF NOT EXISTS {}.{} ").format(
                                    sql.Identifier(schema_destino),
                                    sql.Identifier(sequencia_nome_sem_schema)
                                )
                                pg_destino_cursor.execute(create_seq_if_not_exists)
                                pg_destino_conn.commit()
                            except Exception:
                                pass
        return True
    except Exception:
        return False
    finally:
        if 'pg_origem_cursor' in locals(): pg_origem_cursor.close()
        if 'pg_destino_cursor' in locals(): pg_destino_cursor.close()
        if 'pg_origem_conn' in locals(): pg_origem_conn.close()
        if 'pg_destino_conn' in locals(): pg_destino_conn.close()

def obter_chaves_primarias(tabela, schema_origem, origin_params):
    """Obtém as chaves primárias da tabela"""
    try:
        conn = conectar(origin_params)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """, (f"{schema_origem}.{tabela}",))
        chaves_primarias = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()
        return chaves_primarias
    except Exception:
        return []

def criar_tabela_com_serial(tabela, schema_origem, schema_destino, origin_params, dest_params):
    """Cria a tabela no PostgreSQL de destino usando tipos SERIAL quando apropriado"""
    try:
        criar_sequencias_necessarias(tabela, schema_origem, schema_destino, origin_params, dest_params)
        chaves_primarias = obter_chaves_primarias(tabela, schema_origem, origin_params)
        pg_origem_conn = conectar(origin_params)
        pg_destino_conn = conectar(dest_params)
        pg_origem_cursor = pg_origem_conn.cursor()
        pg_destino_cursor = pg_destino_conn.cursor()
        pg_origem_cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            )
        """, (schema_origem, tabela))
        if not pg_origem_cursor.fetchone()[0]:
            return False, f"Tabela {schema_origem}.{tabela} não existe na origem"
        pg_origem_cursor.execute("""
            SELECT 
                column_name, 
                data_type, 
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s 
            ORDER BY ordinal_position
        """, (schema_origem, tabela))
        colunas_info = pg_origem_cursor.fetchall()
        colunas_def = []
        for coluna in colunas_info:
            nome, tipo, nullable, default_value, char_max_length, num_precision, num_scale = coluna
            is_serial = False
            serial_type = None
            if default_value and 'nextval' in str(default_value):
                if 'int' in tipo.lower() or 'serial' in tipo.lower():
                    if 'big' in tipo.lower():
                        serial_type = 'BIGSERIAL'
                    elif 'small' in tipo.lower():
                        serial_type = 'SMALLSERIAL'
                    else:
                        serial_type = 'SERIAL'
                    is_serial = True
            if nome in chaves_primarias and nome.lower().endswith(('id', 'key')) and not is_serial:
                if 'int' in tipo.lower():
                    serial_type = 'SERIAL'
                    is_serial = True
            if is_serial:
                pg_tipo = serial_type
            elif tipo == 'character varying':
                if char_max_length:
                    pg_tipo = f'VARCHAR({char_max_length})'
                else:
                    pg_tipo = 'VARCHAR'
            elif tipo == 'character':
                if char_max_length:
                    pg_tipo = f'CHAR({char_max_length})'
                else:
                    pg_tipo = 'CHAR'
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
            if nullable == 'NO' and not is_serial:
                col_def += ' NOT NULL'
            if not is_serial and default_value:
                if 'nextval' in str(default_value):
                    match = re.search(r"nextval\('([^']+)'::regclass\)", str(default_value))
                    if match:
                        sequencia_nome = match.group(1)
                        sequencia_nome_sem_schema = sequencia_nome.split('.')[-1]
                        default_value = f"nextval('{schema_destino}.{sequencia_nome_sem_schema}'::regclass)"
                col_def += f' DEFAULT {default_value}'
            if nome in chaves_primarias:
                col_def += ' PRIMARY KEY'
            colunas_def.append(col_def)
        pg_destino_cursor.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {} ").format(sql.Identifier(schema_destino)))
        pg_destino_conn.commit()
        create_sql = f'CREATE TABLE IF NOT EXISTS {schema_destino}.{tabela} ({", ".join(colunas_def)})'
        try:
            pg_destino_cursor.execute(create_sql)
            pg_destino_conn.commit()
            return True, f"Tabela {schema_destino}.{tabela} criada com sucesso!"
        except Exception as create_error:
            return False, f"Erro ao executar CREATE TABLE: {create_error}"
    except Exception as e:
        return False, f"Erro ao processar tabela {tabela}: {e}"
    finally:
        if 'pg_origem_cursor' in locals(): pg_origem_cursor.close()
        if 'pg_destino_cursor' in locals(): pg_destino_cursor.close()
        if 'pg_origem_conn' in locals(): pg_origem_conn.close()
        if 'pg_destino_conn' in locals(): pg_destino_conn.close()

# Templates HTML
HTML_BASE = """
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Sistema de Migração PostgreSQL</title>
    <style>
        :root {
            --primary: #4361ee;
            --secondary: #3f37c9;
            --success: #4cc9f0;
            --danger: #f72585;
            --warning: #f8961e;
            --info: #4895ef;
            --light: #f8f9fa;
            --dark: #212529;
            --gray: #6c757d;
        }
        
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        }
        
        body {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            color: var(--dark);
        }
        
        .container {
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.2);
            overflow: hidden;
        }
        
        header {
            background: var(--primary);
            color: white;
            padding: 20px;
            text-align: center;
        }
        
        header h1 {
            margin-bottom: 10px;
            font-size: 2.2rem;
        }
        
        header p {
            opacity: 0.9;
        }
        
        .content {
            display: flex;
            min-height: 600px;
        }
        
        .sidebar {
            width: 250px;
            background: var(--light);
            padding: 20px;
            border-right: 1px solid #dee2e6;
        }
        
        .main-content {
            flex: 1;
            padding: 20px;
            overflow-y: auto;
        }
        
        .nav-item {
            display: block;
            padding: 12px 15px;
            margin-bottom: 8px;
            border-radius: 8px;
            color: var(--dark);
            text-decoration: none;
            font-weight: 500;
            transition: all 0.3s;
        }
        
        .nav-item:hover, .nav-item.active {
            background: var(--primary);
            color: white;
        }
        
        .nav-item i {
            margin-right: 10px;
            width: 20px;
            text-align: center;
        }
        
        .section {
            display: none;
        }
        
        .section.active {
            display: block;
        }
        
        .card {
            background: white;
            border-radius: 10px;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
            padding: 20px;
            margin-bottom: 20px;
        }
        
        .card-title {
            font-size: 1.4rem;
            margin-bottom: 15px;
            color: var(--primary);
            border-bottom: 2px solid var(--light);
            padding-bottom: 10px;
        }
        
        .form-group {
            margin-bottom: 15px;
        }
        
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: 500;
        }
        
        input, select {
            width: 100%;
            padding: 12px;
            border: 1px solid #ced4da;
            border-radius: 6px;
            font-size: 16px;
            transition: border-color 0.3s;
        }
        
        input:focus, select:focus {
            border-color: var(--primary);
            outline: none;
            box-shadow: 0 0 0 3px rgba(67, 97, 238, 0.2);
        }
        
        .btn {
            display: inline-block;
            padding: 12px 20px;
            background: var(--primary);
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 16px;
            font-weight: 500;
            transition: all 0.3s;
            text-align: center;
        }
        
        .btn:hover {
            background: var(--secondary);
            transform: translateY(-2px);
            box-shadow: 0 4px 10px rgba(0, 0, 0, 0.15);
        }
        
        .btn-success {
            background: var(--success);
        }
        
        .btn-danger {
            background: var(--danger);
        }
        
        .btn-warning {
            background: var(--warning);
        }
        
        .connection-list {
            list-style: none;
        }
        
        .connection-item {
            padding: 15px;
            border: 1px solid #dee2e6;
            border-radius: 8px;
            margin-bottom: 10px;
            background: white;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .connection-info {
            flex: 1;
        }
        
        .connection-actions {
            display: flex;
            gap: 10px;
        }
        
        .tables-container {
            max-height: 400px;
            overflow-y: auto;
            border: 1px solid #dee2e6;
            border-radius: 8px;
            padding: 15px;
            margin-top: 15px;
        }
        
        .table-item {
            padding: 10px;
            border-bottom: 1px solid #eee;
            display: flex;
            align-items: center;
        }
        
        .table-item:last-child {
            border-bottom: none;
        }
        
        .results {
            margin-top: 20px;
        }
        
        .result-item {
            padding: 10px;
            border-radius: 6px;
            margin-bottom: 8px;
            background: #f8f9fa;
        }
        
        .success {
            border-left: 4px solid #28a745;
        }
        
        .error {
            border-left: 4px solid #dc3545;
        }
        
        .alert {
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 20px;
        }
        
        .alert-success {
            background: #d4edda;
            color: #155724;
            border: 1px solid #c3e6cb;
        }
        
        .alert-danger {
            background: #f8d7da;
            color: #721c24;
            border: 1px solid #f5c6cb;
        }
        
        .action-buttons {
            display: flex;
            gap: 10px;
            margin: 15px 0;
        }
        
        .hidden {
            display: none;
        }
        
        .connection-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
        }
        
        @media (max-width: 768px) {
            .content {
                flex-direction: column;
            }
            
            .sidebar {
                width: 100%;
                border-right: none;
                border-bottom: 1px solid #dee2e6;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔄 Sistema de Migração PostgreSQL</h1>
            <p>Crie e gerencie migrações de tabelas entre bancos PostgreSQL</p>
        </header>
        
        <div class="content">
            <div class="sidebar">
                <a href="#" class="nav-item active" onclick="showSection('gerenciar-conexoes')">
                    <i>🔌</i> Gerenciar Conexões
                </a>
                <a href="#" class="nav-item" onclick="showSection('nova-conexao')">
                    <i>➕</i> Nova Conexão
                </a>
                <a href="#" class="nav-item" onclick="showSection('migrar-tabelas')">
                    <i>🚀</i> Migrar Tabelas
                </a>
                <a href="#" class="nav-item" onclick="showSection('conexoes-salvas')">
                    <i>💾</i> Conexões Salvas
                </a>
            </div>
            
            <div class="main-content">
                <!-- Gerenciar Conexões -->
                <div id="gerenciar-conexoes" class="section active">
                    <div class="card">
                        <h2 class="card-title">🔌 Gerenciar Conexões</h2>
                        <p>Use as conexões salvas para acessar rapidamente seus bancos de dados.</p>
                        
                        <div class="connection-header">
                            <h3>Conexões Disponíveis</h3>
                            <button class="btn" onclick="showSection('nova-conexao')">➕ Nova Conexão</button>
                        </div>
                        
                        <ul class="connection-list">
                            {% for nome, dados in conexoes_salvas.items() %}
                            <li class="connection-item">
                                <div class="connection-info">
                                    <strong>{{ nome }}</strong><br>
                                    <small>{{ dados.user }}@{{ dados.host }}:{{ dados.port }}/{{ dados.dbname }}</small>
                                </div>
                                <div class="connection-actions">
                                    <button class="btn btn-success" onclick="usarComoOrigem('{{ nome }}')">Origem</button>
                                    <button class="btn btn-warning" onclick="usarComoDestino('{{ nome }}')">Destino</button>
                                    <form method="post" action="/deletar_conexao" style="display:inline;">
                                        <input type="hidden" name="nome_conexao" value="{{ nome }}">
                                        <button type="submit" class="btn btn-danger">🗑️</button>
                                    </form>
                                </div>
                            </li>
                            {% endfor %}
                        </ul>
                    </div>
                </div>
                
                <!-- Nova Conexão -->
                <div id="nova-conexao" class="section">
                    <div class="card">
                        <h2 class="card-title">➕ Nova Conexão</h2>
                        <p>Adicione uma nova conexão de banco de dados para usar posteriormente.</p>
                        
                        <form method="post" action="/salvar_conexao">
                            <div class="form-group">
                                <label for="nome_conexao">Nome da Conexão:</label>
                                <input type="text" id="nome_conexao" name="nome_conexao" required placeholder="ex: Produção, Homologação">
                            </div>
                            
                            <div class="form-group">
                                <label for="host">Host:</label>
                                <input type="text" id="host" name="host" required placeholder="ex: 192.168.1.100">
                            </div>
                            
                            <div class="form-group">
                                <label for="port">Porta:</label>
                                <input type="number" id="port" name="port" value="5432">
                            </div>
                            
                            <div class="form-group">
                                <label for="dbname">Database:</label>
                                <input type="text" id="dbname" name="dbname" required placeholder="ex: meubanco">
                            </div>
                            
                            <div class="form-group">
                                <label for="user">Usuário:</label>
                                <input type="text" id="user" name="user" required placeholder="ex: postgres">
                            </div>
                            
                            <div class="form-group">
                                <label for="password">Senha:</label>
                                <input type="password" id="password" name="password" placeholder="Senha do banco">
                            </div>
                            
                            <div class="form-group">
                                <label for="schema">Schema Padrão:</label>
                                <input type="text" id="schema" name="schema" value="public" placeholder="ex: public">
                            </div>
                            
                            <button type="submit" class="btn">💾 Salvar Conexão</button>
                            <button type="button" class="btn btn-warning" onclick="testarConexao()">🔍 Testar Conexão</button>
                        </form>
                    </div>
                </div>
                
                <!-- Migrar Tabelas -->
                <div id="migrar-tabelas" class="section">
                    <div class="card">
                        <h2 class="card-title">🚀 Migrar Tabelas</h2>
                        <p>Migre tabelas entre bancos de dados usando conexões salvas.</p>
                        
                        <form method="post" action="/migrar_tabelas">
                            <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 20px;">
                                <!-- Origem -->
                                <div>
                                    <h3>📋 Banco de Origem</h3>
                                    
                                    <div class="form-group">
                                        <label for="origem_select">Conexão Salva:</label>
                                        <select id="origem_select" onchange="carregarConexao('origem', this.value)">
                                            <option value="">-- Selecione --</option>
                                            {% for nome, dados in conexoes_salvas.items() %}
                                            <option value="{{ nome }}">{{ nome }}</option>
                                            {% endfor %}
                                        </select>
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="o_host">Host:</label>
                                        <input type="text" id="o_host" name="o_host" required>
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="o_port">Porta:</label>
                                        <input type="number" id="o_port" name="o_port" value="5432">
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="o_db">Database:</label>
                                        <input type="text" id="o_db" name="o_db" required>
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="o_user">Usuário:</label>
                                        <input type="text" id="o_user" name="o_user" required>
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="o_pass">Senha:</label>
                                        <input type="password" id="o_pass" name="o_pass">
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="o_schema">Schema:</label>
                                        <input type="text" id="o_schema" name="o_schema" value="public">
                                    </div>
                                </div>
                                
                                <!-- Destino -->
                                <div>
                                    <h3>🎯 Banco de Destino</h3>
                                    
                                    <div class="form-group">
                                        <label for="destino_select">Conexão Salva:</label>
                                        <select id="destino_select" onchange="carregarConexao('destino', this.value)">
                                            <option value="">-- Selecione --</option>
                                            {% for nome, dados in conexoes_salvas.items() %}
                                            <option value="{{ nome }}">{{ nome }}</option>
                                            {% endfor %}
                                        </select>
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="d_host">Host:</label>
                                        <input type="text" id="d_host" name="d_host" required>
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="d_port">Porta:</label>
                                        <input type="number" id="d_port" name="d_port" value="5432">
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="d_db">Database:</label>
                                        <input type="text" id="d_db" name="d_db" required>
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="d_user">Usuário:</label>
                                        <input type="text" id="d_user" name="d_user" required>
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="d_pass">Senha:</label>
                                        <input type="password" id="d_pass" name="d_pass">
                                    </div>
                                    
                                    <div class="form-group">
                                        <label for="d_schema">Schema Destino:</label>
                                        <input type="text" id="d_schema" name="d_schema" value="public">
                                    </div>
                                </div>
                            </div>
                            
                            <div class="action-buttons">
                                <button type="submit" name="action" value="listar" class="btn">📋 Listar Tabelas</button>
                                <button type="button" class="btn btn-success" onclick="testarConexaoOrigem()">🔍 Testar Origem</button>
                                <button type="button" class="btn btn-success" onclick="testarConexaoDestino()">🔍 Testar Destino</button>
                            </div>
                            
                            {% if tabelas_listadas %}
                            <div class="tables-container">
                                <h3>📋 Tabelas Disponíveis no Schema {{ o_schema }}:</h3>
                                
                                <div class="action-buttons">
                                    <button type="button" class="btn" onclick="selecionarTodasTabelas()">✓ Selecionar Todas</button>
                                    <button type="button" class="btn" onclick="desselecionarTodasTabelas()">✗ Desselecionar Todas</button>
                                </div>
                                
                                {% for tabela in tabelas_listadas %}
                                <div class="table-item">
                                    <input type="checkbox" name="tabelas_selecionadas" value="{{ tabela[0] }}|{{ tabela[1] }}" id="tabela{{ loop.index }}">
                                    <label for="tabela{{ loop.index }}" style="margin: 0; margin-left: 10px;">
                                        <strong>{{ tabela[0] }}.{{ tabela[1] }}</strong>
                                    </label>
                                </div>
                                {% endfor %}
                            </div>
                            
                            <div class="action-buttons">
                                <button type="submit" name="action" value="criar" class="btn btn-success">🚀 Migrar Tabelas Selecionadas</button>
                            </div>
                            {% endif %}
                        </form>
                        
                        {% if resultados %}
                        <div class="results">
                            <h3>📊 Resultados da Migração:</h3>
                            {% for resultado in resultados %}
                            <div class="result-item {% if '✅' in resultado %}success{% else %}error{% endif %}">
                                {{ resultado }}
                            </div>
                            {% endfor %}
                        </div>
                        {% endif %}
                    </div>
                </div>
                
                <!-- Conexões Salvas -->
                <div id="conexoes-salvas" class="section">
                    <div class="card">
                        <h2 class="card-title">💾 Conexões Salvas</h2>
                        <p>Visualize e gerencie todas as suas conexões salvas.</p>
                        
                        {% if conexoes_salvas %}
                        <ul class="connection-list">
                            {% for nome, dados in conexoes_salvas.items() %}
                            <li class="connection-item">
                                <div class="connection-info">
                                    <strong>{{ nome }}</strong><br>
                                    <small>Host: {{ dados.host }}:{{ dados.port }}</small><br>
                                    <small>Database: {{ dados.dbname }}</small><br>
                                    <small>Usuário: {{ dados.user }}</small><br>
                                    <small>Schema: {{ dados.schema }}</small><br>
                                    <small>Criada em: {{ dados.data_criacao }}</small>
                                </div>
                                <div class="connection-actions">
                                    <button class="btn" onclick="usarComoOrigem('{{ nome }}')">Origem</button>
                                    <button class="btn" onclick="usarComoDestino('{{ nome }}')">Destino</button>
                                    <form method="post" action="/deletar_conexao" style="display:inline;">
                                        <input type="hidden" name="nome_conexao" value="{{ nome }}">
                                        <button type="submit" class="btn btn-danger">🗑️ Excluir</button>
                                    </form>
                                </div>
                            </li>
                            {% endfor %}
                        </ul>
                        {% else %}
                        <div class="alert alert-info">
                            Nenhuma conexão salva. <a href="#" onclick="showSection('nova-conexao')">Clique aqui</a> para adicionar uma.
                        </div>
                        {% endif %}
                    </div>
                </div>
            </div>
        </div>
    </div>
    
    <script>
        // Funções para gerenciar a exibição das seções
        function showSection(sectionId) {
            document.querySelectorAll('.section').forEach(section => {
                section.classList.remove('active');
            });
            document.querySelectorAll('.nav-item').forEach(item => {
                item.classList.remove('active');
            });
            
            document.getElementById(sectionId).classList.add('active');
            event.target.classList.add('active');
        }
        
        // Funções para gerenciar conexões
        function carregarConexao(tipo, nome) {
            const conexoes = {{ conexoes_salvas|tojson }};
            if (conexoes[nome]) {
                const conn = conexoes[nome];
                document.getElementById(`${tipo}_host`).value = conn.host || '';
                document.getElementById(`${tipo}_port`).value = conn.port || '5432';
                document.getElementById(`${tipo}_db`).value = conn.dbname || '';
                document.getElementById(`${tipo}_user`).value = conn.user || '';
                document.getElementById(`${tipo}_pass`).value = conn.password || '';
                document.getElementById(`${tipo}_schema`).value = conn.schema || 'public';
            }
        }
        
        function usarComoOrigem(nome) {
            carregarConexao('origem', nome);
            showSection('migrar-tabelas');
        }
        
        function usarComoDestino(nome) {
            carregarConexao('destino', nome);
            showSection('migrar-tabelas');
        }
        
        // Funções para gerenciar tabelas
        function selecionarTodasTabelas() {
            const checkboxes = document.querySelectorAll('input[name="tabelas_selecionadas"]');
            checkboxes.forEach(cb => cb.checked = true);
        }
        
        function desselecionarTodasTabelas() {
            const checkboxes = document.querySelectorAll('input[name="tabelas_selecionadas"]');
            checkboxes.forEach(cb => cb.checked = false);
        }
        
        // Funções para testar conexões
        async function testarConexao() {
            const formData = new FormData();
            formData.append('host', document.getElementById('host').value);
            formData.append('port', document.getElementById('port').value);
            formData.append('dbname', document.getElementById('dbname').value);
            formData.append('user', document.getElementById('user').value);
            formData.append('password', document.getElementById('password').value);
            
            try {
                const response = await fetch('/testar_conexao', {
                    method: 'POST',
                    body: formData
                });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ ' + result.message);
                } else {
                    alert('❌ ' + result.message);
                }
            } catch (error) {
                alert('❌ Erro ao testar conexão: ' + error);
            }
        }
        
        async function testarConexaoOrigem() {
            const formData = new FormData();
            formData.append('host', document.getElementById('o_host').value);
            formData.append('port', document.getElementById('o_port').value);
            formData.append('dbname', document.getElementById('o_db').value);
            formData.append('user', document.getElementById('o_user').value);
            formData.append('password', document.getElementById('o_pass').value);
            
            try {
                const response = await fetch('/testar_conexao', {
                    method: 'POST',
                    body: formData
                });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ Conexão de origem: ' + result.message);
                } else {
                    alert('❌ Conexão de origem: ' + result.message);
                }
            } catch (error) {
                alert('❌ Erro ao testar conexão de origem: ' + error);
            }
        }
        
        async function testarConexaoDestino() {
            const formData = new FormData();
            formData.append('host', document.getElementById('d_host').value);
            formData.append('port', document.getElementById('d_port').value);
            formData.append('dbname', document.getElementById('d_db').value);
            formData.append('user', document.getElementById('d_user').value);
            formData.append('password', document.getElementById('d_pass').value);
            
            try {
                const response = await fetch('/testar_conexao', {
                    method: 'POST',
                    body: formData
                });
                const result = await response.json();
                
                if (result.success) {
                    alert('✅ Conexão de destino: ' + result.message);
                } else {
                    alert('❌ Conexão de destino: ' + result.message);
                }
            } catch (error) {
                alert('❌ Erro ao testar conexão de destino: ' + error);
            }
        }
        
        // Mostrar a seção de gerenciar conexões por padrão
        showSection('gerenciar-conexoes');
    </script>
</body>
</html>
"""

@app.route('/', methods=['GET'])
def index():
    conexoes_salvas = carregar_conexoes()
    return render_template_string(HTML_BASE, conexoes_salvas=conexoes_salvas)

@app.route('/salvar_conexao', methods=['POST'])
def salvar_conexao():
    nome = request.form['nome_conexao']
    dados = {
        'host': request.form['host'],
        'port': request.form.get('port', '5432'),
        'dbname': request.form['dbname'],
        'user': request.form['user'],
        'password': request.form.get('password', ''),
        'schema': request.form.get('schema', 'public')
    }
    
    salvar_conexao(nome, dados)
    return redirect(url_for('index'))

@app.route('/deletar_conexao', methods=['POST'])
def deletar_conexao():
    nome = request.form['nome_conexao']
    conexoes = carregar_conexoes()
    if nome in conexoes:
        del conexoes[nome]
        session['conexoes'] = conexoes
        session.modified = True
    
    return redirect(url_for('index'))

@app.route('/testar_conexao', methods=['POST'])
def testar_conexao_route():
    try:
        conn_params = {
            'host': request.form['host'],
            'port': request.form.get('port', '5432'),
            'dbname': request.form['dbname'],
            'user': request.form['user'],
            'password': request.form.get('password', '')
        }
        
        success, message = testar_conexao(conn_params)
        return {'success': success, 'message': message}
    except Exception as e:
        return {'success': False, 'message': str(e)}

@app.route('/migrar_tabelas', methods=['POST'])
def migrar_tabelas():
    # Obter dados do formulário
    origin_params = {
        'host': request.form['o_host'],
        'port': request.form.get('o_port', '5432'),
        'dbname': request.form['o_db'],
        'user': request.form['o_user'],
        'password': request.form.get('o_pass', '')
    }
    
    dest_params = {
        'host': request.form['d_host'],
        'port': request.form.get('d_port', '5432'),
        'dbname': request.form['d_db'],
        'user': request.form['d_user'],
        'password': request.form.get('d_pass', '')
    }
    
    schema_origem = request.form.get('o_schema', 'public')
    schema_destino = request.form.get('d_schema', 'public')
    action = request.form.get('action', '')

    resultados = []
    tabelas_listadas = []
    mensagem = ""

    try:
        if action == 'listar':
            # Listar tabelas disponíveis
            tabelas_listadas = listar_tabelas_banco(origin_params, schema_origem)
            if not tabelas_listadas:
                mensagem = "Nenhuma tabela encontrada no schema especificado."
            else:
                mensagem = f"Encontradas {len(tabelas_listadas)} tabelas no schema {schema_origem}"

        elif action == 'criar':
            # Criar tabelas selecionadas
            tabelas_selecionadas = request.form.getlist('tabelas_selecionadas')
            if not tabelas_selecionadas:
                mensagem = "Por favor, selecione pelo menos uma tabela"
            else:
                sucessos = 0
                falhas = 0
                for item in tabelas_selecionadas:
                    schema, tabela = item.split('|')
                    success, msg = criar_tabela_com_serial(
                        tabela, schema_origem, schema_destino, origin_params, dest_params
                    )
                    if success:
                        sucessos += 1
                        resultados.append(f"✅ {msg}")
                    else:
                        falhas += 1
                        resultados.append(f"❌ {msg}")
                mensagem = f"Migração concluída! ✅ {sucessos} sucesso(s), ❌ {falhas} falha(s)"

    except Exception as e:
        mensagem = f"Erro: {str(e)}"

    conexoes_salvas = carregar_conexoes()
    return render_template_string(HTML_BASE, 
                                mensagem=mensagem,
                                tabelas_listadas=tabelas_listadas,
                                resultados=resultados,
                                o_schema=schema_origem,
                                conexoes_salvas=conexoes_salvas)

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)