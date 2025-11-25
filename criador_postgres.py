import psycopg2
from psycopg2 import sql

def criar_tabela_postgres_para_postgres(tabela, schema_origem='chatbot'):
    """Cria a tabela no PostgreSQL de destino baseada na estrutura do PostgreSQL de origem"""
    try:
        # Conexões - origem e destino PostgreSQL
        pg_origem_conn = psycopg2.connect(
            host='', dbname='',  # Banco de origem
            user='', password=''
        )
        pg_destino_conn = psycopg2.connect(
            host='', dbname='',  # Banco de destino
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
            print(f"⏭️  Tabela {tabela} não existe no PostgreSQL de origem, pulando...")
            return
        
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
            nome, tipo, nullable, default, char_max_length, num_precision, num_scale = coluna
            
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
            if default:
                col_def += f' DEFAULT {default}'
            
            colunas_def.append(col_def)
        
        # Criar tabela no PostgreSQL de destino  
        create_sql = f'CREATE TABLE IF NOT EXISTS leis.{tabela} ({", ".join(colunas_def)})'
        pg_destino_cursor.execute(create_sql)
        pg_destino_conn.commit()
        
        print(f"✅ Tabela {tabela} criada com sucesso no schema!")
        
    except Exception as e:
        print(f"❌ Erro ao criar tabela {tabela}: {e}")
    finally:
        if 'pg_origem_cursor' in locals(): pg_origem_cursor.close()
        if 'pg_destino_cursor' in locals(): pg_destino_cursor.close()
        if 'pg_origem_conn' in locals(): pg_origem_conn.close()
        if 'pg_destino_conn' in locals(): pg_destino_conn.close()

# Lista de tabelas para criar
TABELAS = [
    'glpi_logs'
]

if __name__ == "__main__":
    print("🚀 Iniciando criação de tabelas no PostgreSQL (PostgreSQL para PostgreSQL)")
    print("📋 Apenas estrutura, sem dados")
    print("─" * 50)
    
    for tabela in TABELAS:
        criar_tabela_postgres_para_postgres(tabela)
    
    print("─" * 50)
    print("🎉 Criação de tabelas concluída!")