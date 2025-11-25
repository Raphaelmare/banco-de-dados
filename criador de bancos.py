import psycopg2
from psycopg2 import sql
import re

def listar_tabelas_banco(host, dbname, user, password, schema=None):
    """Lista todas as tabelas disponíveis no banco"""
    try:
        conn = psycopg2.connect(
            host=host, dbname=dbname,
            user=user, password=password
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
        conn.close()
        
        return tabelas
        
    except Exception as e:
        print(f"❌ Erro ao listar tabelas: {e}")
        return []

def selecionar_tabelas_para_migrar(tabelas):
    """Permite ao usuário selecionar quais tabelas migrar"""
    if not tabelas:
        print("❌ Nenhuma tabela encontrada!")
        return []
    
    print("\n📋 Tabelas disponíveis:")
    print("-" * 60)
    for i, (schema, tabela) in enumerate(tabelas, 1):
        print(f"{i:3d}. {schema}.{tabela}")
    print("-" * 60)
    
    while True:
        try:
            selecao = input("\n🔹 Selecione as tabelas (ex: 1,3,5-10, all): ").strip().lower()
            
            if selecao == 'all':
                return [(schema, tabela) for schema, tabela in tabelas]
            
            tabelas_selecionadas = []
            partes = selecao.split(',')
            
            for parte in partes:
                parte = parte.strip()
                if '-' in parte:
                    inicio, fim = map(int, parte.split('-'))
                    tabelas_selecionadas.extend(range(inicio, fim + 1))
                else:
                    tabelas_selecionadas.append(int(parte))
            
            # Converter números para índices e validar
            indices_validos = []
            for num in tabelas_selecionadas:
                if 1 <= num <= len(tabelas):
                    indices_validos.append(num - 1)
                else:
                    print(f"⚠️  Número {num} fora do range (1-{len(tabelas)})")
            
            if not indices_validos:
                print("❌ Nenhuma seleção válida. Tente novamente.")
                continue
                
            return [tabelas[i] for i in indices_validos]
            
        except ValueError:
            print("❌ Entrada inválida. Use números separados por vírgulas ou 'all'.")
        except KeyboardInterrupt:
            print("\n⏹️  Operação cancelada pelo usuário")
            return []

def criar_sequencias_necessarias(tabela, schema_origem, schema_destino):
    """Cria sequências necessárias para a tabela no schema de destino"""
    try:
        pg_origem_conn = psycopg2.connect(
            host='', dbname='',
            user='', password=''
        )
        pg_destino_conn = psycopg2.connect(
            host='', dbname='',
            user='', password=''
        )
        
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
            # Extrair o nome da sequência do valor padrão
            match = re.search(r"nextval\('([^']+)'::regclass\)", str(default_value))
            if match:
                sequencia_nome = match.group(1)
                sequencia_nome_sem_schema = sequencia_nome.split('.')[-1]
                
                print(f"   🔍 Detectada sequência: {sequencia_nome}")
                
                # Verificar se a sequência já existe no destino
                pg_destino_cursor.execute("""
                    SELECT COUNT(*) 
                    FROM pg_sequences 
                    WHERE schemaname = %s AND sequencename = %s
                """, (schema_destino, sequencia_nome_sem_schema))
                
                if pg_destino_cursor.fetchone()[0] == 0:
                    print(f"   ⚙️  Criando sequência: {schema_destino}.{sequencia_nome_sem_schema}")
                    
                    try:
                        # Tentar obter informações detalhadas da sequência
                        pg_origem_cursor.execute("""
                            SELECT 
                                increment_by,
                                min_value,
                                max_value,
                                start_value,
                                cache_size
                            FROM {}
                        """.format(sequencia_nome))
                        
                        seq_info = pg_origem_cursor.fetchone()
                        
                        if seq_info:
                            increment, min_val, max_val, start_val, cache = seq_info
                            
                            # Criar a sequência no destino com os parâmetros corretos
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
                            print(f"   ✅ Sequência criada com parâmetros específicos")
                            
                    except Exception as seq_error:
                        print(f"   ⚠️  Não foi possível obter parâmetros da sequência: {seq_error}")
                        # Criar sequência com valores padrão
                        try:
                            create_seq_default_sql = sql.SQL("CREATE SEQUENCE {}.{}").format(
                                sql.Identifier(schema_destino),
                                sql.Identifier(sequencia_nome_sem_schema)
                            )
                            pg_destino_cursor.execute(create_seq_default_sql)
                            pg_destino_conn.commit()
                            print(f"   ✅ Sequência criada com valores padrão")
                        except Exception as create_error:
                            print(f"   ❌ Erro ao criar sequência: {create_error}")
                            # Tentar criar com IF NOT EXISTS
                            try:
                                create_seq_if_not_exists = sql.SQL("CREATE SEQUENCE IF NOT EXISTS {}.{}").format(
                                    sql.Identifier(schema_destino),
                                    sql.Identifier(sequencia_nome_sem_schema)
                                )
                                pg_destino_cursor.execute(create_seq_if_not_exists)
                                pg_destino_conn.commit()
                                print(f"   ✅ Sequência criada com IF NOT EXISTS")
                            except Exception as final_error:
                                print(f"   💥 Falha crítica ao criar sequência: {final_error}")
                else:
                    print(f"   ✅ Sequência {schema_destino}.{sequencia_nome_sem_schema} já existe")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro ao criar sequências para {tabela}: {e}")
        return False
    finally:
        if 'pg_origem_cursor' in locals(): pg_origem_cursor.close()
        if 'pg_destino_cursor' in locals(): pg_destino_cursor.close()
        if 'pg_origem_conn' in locals(): pg_origem_conn.close()
        if 'pg_destino_conn' in locals(): pg_destino_conn.close()

def obter_chaves_primarias(tabela, schema_origem):
    """Obtém as chaves primárias da tabela"""
    try:
        conn = psycopg2.connect(
            host='', dbname='',
            user='', password=''
        )
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
        
    except Exception as e:
        print(f"⚠️  Erro ao obter chaves primárias: {e}")
        return []

def criar_tabela_com_serial(tabela, schema_origem, schema_destino):
    """Cria a tabela no PostgreSQL de destino usando tipos SERIAL quando apropriado"""
    try:
        print(f"\n🔄 Processando tabela: {schema_origem}.{tabela}")
        
        # Primeiro criar sequências necessárias (se houver)
        criar_sequencias_necessarias(tabela, schema_origem, schema_destino)
        
        # Obter chaves primárias para identificar possíveis colunas ID
        chaves_primarias = obter_chaves_primarias(tabela, schema_origem)
        
        # Conexões - origem e destino PostgreSQL
        pg_origem_conn = psycopg2.connect(
            host='', dbname='',
            user='', password=''
        )
        pg_destino_conn = psycopg2.connect(
            host='', dbname='',
            user='', password=''
        )
        
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
            print(f"⏭️  Tabela {schema_origem}.{tabela} não existe no PostgreSQL de origem, pulando...")
            return False
        
        # Obter informações das colunas de forma mais robusta
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
        colunas_serial = []
        
        for coluna in colunas_info:
            nome, tipo, nullable, default_value, char_max_length, num_precision, num_scale = coluna
            
            # Verificar se é uma coluna SERIAL (com nextval)
            is_serial = False
            serial_type = None
            
            if default_value and 'nextval' in str(default_value):
                # Determinar o tipo de SERIAL baseado no tipo de dados
                if 'int' in tipo.lower() or 'serial' in tipo.lower():
                    if 'big' in tipo.lower():
                        serial_type = 'BIGSERIAL'
                    elif 'small' in tipo.lower():
                        serial_type = 'SMALLSERIAL'
                    else:
                        serial_type = 'SERIAL'
                    is_serial = True
            
            # Também verificar se é uma chave primária que parece ser um ID
            if nome in chaves_primarias and nome.lower().endswith(('id', 'key')) and not is_serial:
                if 'int' in tipo.lower():
                    serial_type = 'SERIAL'
                    is_serial = True
                    print(f"   🔍 Coluna {nome} identificada como possível SERIAL (chave primária)")
            
            # Construir o tipo de dados apropriado
            if is_serial:
                pg_tipo = serial_type
                colunas_serial.append(nome)
                print(f"   🔹 Coluna {nome}: {tipo} → {serial_type}")
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
            
            # Adicionar constraints
            col_def = f'"{nome}" {pg_tipo}'
            
            # Para colunas SERIAL, não adicionar NOT NULL (já é implícito)
            if nullable == 'NO' and not is_serial:
                col_def += ' NOT NULL'
            
            # Para colunas não SERIAL, adicionar DEFAULT se existir
            if not is_serial and default_value:
                # Corrigir referências de sequência para o schema correto
                if 'nextval' in str(default_value):
                    match = re.search(r"nextval\('([^']+)'::regclass\)", str(default_value))
                    if match:
                        sequencia_nome = match.group(1)
                        sequencia_nome_sem_schema = sequencia_nome.split('.')[-1]
                        default_value = f"nextval('{schema_destino}.{sequencia_nome_sem_schema}'::regclass)"
                col_def += f' DEFAULT {default_value}'
            
            # Adicionar chave primária se for o caso
            if nome in chaves_primarias:
                col_def += ' PRIMARY KEY'
            
            colunas_def.append(col_def)
        
        if colunas_serial:
            print(f"   📋 Colunas convertidas para SERIAL: {', '.join(colunas_serial)}")
        else:
            print(f"   ℹ️  Nenhuma coluna SERIAL detectada")
        
        # Criar tabela no PostgreSQL de destino  
        create_sql = f'CREATE TABLE IF NOT EXISTS {schema_destino}.{tabela} ({", ".join(colunas_def)})'
        
        try:
            pg_destino_cursor.execute(create_sql)
            pg_destino_conn.commit()
            print(f"✅ Tabela {schema_destino}.{tabela} criada com sucesso!")
            return True
        except Exception as create_error:
            print(f"❌ Erro ao executar CREATE TABLE: {create_error}")
            print(f"   SQL: {create_sql}")
            return False
        
    except Exception as e:
        print(f"❌ Erro ao processar tabela {tabela}: {e}")
        return False
    finally:
        if 'pg_origem_cursor' in locals(): pg_origem_cursor.close()
        if 'pg_destino_cursor' in locals(): pg_destino_cursor.close()
        if 'pg_origem_conn' in locals(): pg_origem_conn.close()
        if 'pg_destino_conn' in locals(): pg_destino_conn.close()

def main():
    """Função principal do sistema de criação de tabelas"""
    print("🚀 Sistema de Criação de Tabelas PostgreSQL")
    print("📋 Convertendo auto-incrementos para SERIAL")
    print("=" * 60)
    
    # Configurações de conexão
    host_origem = ''
    dbname_origem = ''
    user_origem = ''
    password_origem = ''
    schema_origem = ''
    
    host_destino = ''
    dbname_destino = ''
    user_destino = ''
    password_destino = ''
    schema_destino = ''
    
    # Listar tabelas disponíveis
    print(f"\n🔍 Listando tabelas do schema {schema_origem}...")
    tabelas = listar_tabelas_banco(host_origem, dbname_origem, user_origem, password_origem, schema_origem)
    
    if not tabelas:
        print("❌ Nenhuma tabela encontrada!")
        return
    
    # Selecionar tabelas para criar
    tabelas_selecionadas = selecionar_tabelas_para_migrar(tabelas)
    
    if not tabelas_selecionadas:
        print("❌ Nenhuma tabela selecionada!")
        return
    
    print(f"\n📋 Tabelas selecionadas para criação:")
    for schema, tabela in tabelas_selecionadas:
        print(f"   - {schema}.{tabela} → {schema_destino}.{tabela}")
    
    confirmar = input("\n🔹 Confirmar criação das tabelas? (s/N): ").strip().lower()
    
    if confirmar not in ['s', 'sim', 'y', 'yes']:
        print("⏹️  Operação cancelada pelo usuário")
        return
    
    # Processar criação das tabelas
    print(f"\n🔄 Iniciando criação das tabelas...")
    print("─" * 60)
    
    tabelas_criadas = 0
    tabelas_falhas = 0
    
    for schema, tabela in tabelas_selecionadas:
        sucesso = criar_tabela_com_serial(tabela, schema_origem, schema_destino)
        if sucesso:
            tabelas_criadas += 1
        else:
            tabelas_falhas += 1
        print("─" * 40)
    
    print("─" * 60)
    print(f"🎉 Processo concluído!")
    print(f"✅ Tabelas criadas com sucesso: {tabelas_criadas}")
    print(f"❌ Tabelas com erro: {tabelas_falhas}")
    print(f"📊 Total processado: {tabelas_criadas + tabelas_falhas}")

if __name__ == "__main__":
    main()