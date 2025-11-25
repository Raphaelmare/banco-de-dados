import oracledb
import psycopg2
import re
import os
import csv

oracledb.init_oracle_client(lib_dir=os.environ.get('ORACLE_CLIENT_LIB', 'C:\\instantclient_23_8'))

def normalizar_caminho(caminho):
    """Normaliza caminhos de arquivo resolvendo .. e . corretamente."""
    if not caminho or caminho == 'None':
        return None
    partes = []
    for parte in str(caminho).replace('\\', '/').split('/'):
        if parte == '..':
            if partes:
                partes.pop()
        elif parte and parte != '.':
            partes.append(parte)
    return '/'.join(partes) if partes else None

def escapar_e_formatar(valor, tipo_pg, tipo_oracle, coluna_pg):
    # STATUS padronizado
    if coluna_pg.lower() == 'status':
        if valor is None:
            return 'NULL'
        valor_str = str(valor).strip().upper()
        if valor_str in ('A', 'S', 'TRUE', '1'):
            return '1'
        elif valor_str in ('I', 'N', 'FALSE', '0'):
            return '0'
        else:
            return 'NULL'
    # Inteiros
    if tipo_pg in ('integer', 'bigint', 'smallint'):
        if valor is None:
            return 'NULL'
        valor_str = str(valor).strip()
        if valor_str.replace('-', '').isdigit():
            return valor_str
        return 'NULL'
    # Decimais
    if tipo_pg in ('numeric', 'decimal', 'real', 'double precision'):
        if valor is None:
            return 'NULL'
        try:
            return str(float(str(valor).replace(',', '.')))
        except:
            return 'NULL'
    # Texto
    if tipo_pg in ('text', 'varchar', 'character varying'):
        if valor is None:
            return 'NULL'
        return f"'{str(valor).replace("'", "''")}'"
    # Caminhos
    if any(x in coluna_pg.lower() for x in ['arquivo', 'arq', 'path', 'caminho']):
        if valor is None:
            return 'NULL'
        caminho_normalizado = normalizar_caminho(valor)
        if caminho_normalizado:
            return f"'{caminho_normalizado.replace("'", "''")}'"
        return 'NULL'
    # BLOB
    if tipo_oracle == 'BLOB':
        if isinstance(valor, bytes):
            return f"'\\x{valor.hex()}'"
        return 'NULL'
    # CLOB/LONG
    if tipo_oracle in ('CLOB', 'LONG'):
        try:
            if hasattr(valor, 'read'):
                content = valor.read()
                if content:
                    return f"'{str(content).replace("'", "''")}'"
                return 'NULL'
            else:
                return f"'{str(valor).replace("'", "''")}'"
        except:
            return 'NULL'
    # Datas
    if tipo_oracle in ('DATE', 'TIMESTAMP', 'TIMESTAMP(6)'):
        if valor:
            try:
                return f"'{valor.strftime('%Y-%m-%d %H:%M:%S')}'"
            except:
                return 'NULL'
        return 'NULL'
    # Genérico
    if valor:
        return f"'{str(valor).replace("'", "''")}'"
    return 'NULL'

def migrar_tabela(tabela, schema_oracle='', schema_pg='', export_csv=False):
    """Migra uma tabela do Oracle para PostgreSQL e exporta log para CSV se desejado."""
    oracle_conn = None
    pg_conn = None
    oracle_cursor = None
    pg_cursor = None
    log_linhas = []
    try:
        # Conexão Oracle
        oracle_conn = oracledb.connect(
            user=os.environ.get('ORACLE_USER'),
            password=os.environ.get('ORACLE_PASSWORD'),
            dsn=os.environ.get('ORACLE_DSN')
        )
        # Conexão PostgreSQL
        pg_conn = psycopg2.connect(
            host=os.environ.get('PG_HOST'),
            dbname=os.environ.get('PG_DBNAME'),
            user=os.environ.get('PG_USER'),
            password=os.environ.get('PG_PASSWORD')
        )
        oracle_cursor = oracle_conn.cursor()
        pg_cursor = pg_conn.cursor()
        # 1. Obter colunas do PostgreSQL
        pg_cursor.execute(f"""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_pg, tabela.lower()))
        colunas_pg_info = pg_cursor.fetchall()
        colunas_pg = [col[0] for col in colunas_pg_info]
        tipos_pg = {col[0]: col[1] for col in colunas_pg_info}
        if not colunas_pg:
            print(f"❌ Tabela {schema_pg}.{tabela.lower()} não encontrada no PostgreSQL")
            return
        # 2. Obter colunas do Oracle
        oracle_cursor.execute(f"""
            SELECT column_name, data_type, data_length, data_precision, data_scale 
            FROM all_tab_columns 
            WHERE table_name = UPPER(:tab) AND owner = UPPER(:own)
            ORDER BY column_id
        """, {'tab': tabela, 'own': schema_oracle})
        colunas_info = oracle_cursor.fetchall()
        if not colunas_info:
            print(f"❌ Tabela {tabela} não encontrada no schema Oracle {schema_oracle}")
            return
        mapeamento_colunas = {}
        colunas_oracle = [col[0].lower() for col in colunas_info]
        tipos_oracle = {col[0].lower(): col[1].upper() for col in colunas_info}
        colunas_para_migrar = []
        for coluna_pg in colunas_pg:
            if coluna_pg in colunas_oracle:
                mapeamento_colunas[coluna_pg] = coluna_pg
                colunas_para_migrar.append(coluna_pg)
            else:
                encontrou = False
                for coluna_oracle in colunas_oracle:
                    if (coluna_pg.replace('_', '') == coluna_oracle.replace('_', '') or
                        coluna_pg in coluna_oracle or 
                        coluna_oracle in coluna_pg):
                        mapeamento_colunas[coluna_pg] = coluna_oracle
                        colunas_para_migrar.append(coluna_pg)
                        encontrou = True
                        break
                if not encontrou:
                    print(f"⚠️  Coluna {coluna_pg} não encontrada no Oracle")
        if not colunas_para_migrar:
            print("❌ Nenhuma coluna correspondente encontrada para migração")
            return
        # 4. Buscar dados do Oracle
        colunas_oracle_select = [mapeamento_colunas[col_pg] for col_pg in colunas_para_migrar]
        select_columns = ', '.join(colunas_oracle_select)
        oracle_cursor.execute(f"SELECT {select_columns} FROM {schema_oracle}.{tabela}")
        dados = oracle_cursor.fetchall()
        print(f"📋 Migrando {tabela}: {len(dados)} registros, {len(colunas_para_migrar)} colunas")
        registros_processados = 0
        registros_com_erro = 0
        for reg in dados:
            try:
                valores = []
                for i, valor in enumerate(reg):
                    coluna_pg = colunas_para_migrar[i]
                    coluna_oracle = mapeamento_colunas[coluna_pg]
                    tipo_pg = tipos_pg.get(coluna_pg, 'text')
                    tipo_oracle = tipos_oracle.get(coluna_oracle, 'VARCHAR2')
                    valores.append(escapar_e_formatar(valor, tipo_pg, tipo_oracle, coluna_pg))
                colunas_str = ', '.join(colunas_para_migrar)
                tabela_pg = tabela.lower()
                query = f"INSERT INTO {schema_pg}.{tabela_pg} ({colunas_str}) VALUES ({', '.join(valores)})"
                if query and len(query) < 100000:
                    pg_cursor.execute(query)
                    registros_processados += 1
                    log_linhas.append([tabela_pg, registros_processados, 'OK', ''])
                else:
                    print(f"⚠️ Query muito longa ou inválida no registro {registros_processados + 1}")
                    registros_com_erro += 1
                    log_linhas.append([tabela_pg, registros_processados, 'ERRO', 'Query longa'])
                    continue
                if registros_processados % 50 == 0:
                    pg_conn.commit()
                    print(f"📊 Processados {registros_processados} registros...")
            except psycopg2.Error as e:
                registros_com_erro += 1
                print(f"❌ Erro PostgreSQL no registro {registros_processados + 1}: {e}")
                log_linhas.append([tabela_pg, registros_processados, 'ERRO', str(e)])
                pg_conn.rollback()
                continue
            except Exception as e:
                registros_com_erro += 1
                print(f"⚠️ Erro geral no registro {registros_processados + 1}: {e}")
                log_linhas.append([tabela_pg, registros_processados, 'ERRO', str(e)])
                pg_conn.rollback()
                continue
        pg_conn.commit()
        print(f"✅ {tabela} migrada com sucesso!")
        print(f"📈 Estatísticas: {registros_processados} ok, {registros_com_erro} com erro")
        if export_csv:
            with open(f'log_migracao_{tabela_pg}.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['tabela', 'registro', 'status', 'detalhe'])
                writer.writerows(log_linhas)
            print(f"📝 Log exportado para log_migracao_{tabela_pg}.csv")
    except Exception as e:
        print(f"❌ Erro ao migrar {tabela}: {e}")
        if pg_conn:
            pg_conn.rollback()
    finally:
        if oracle_cursor:
            oracle_cursor.close()
        if pg_cursor:
            pg_cursor.close()
        if oracle_conn:
            oracle_conn.close()
        if pg_conn:
            pg_conn.close()

# Lista de tabelas para migrar
TABELAS = [
    'lei'
]

if __name__ == "__main__":
    print("🚀 Iniciando migração Oracle → PostgreSQL")
    print("─" * 50)
    if not TABELAS:
        print("ℹ️  Nenhuma tabela definida para migração.")
    else:
        for tabela in TABELAS:
            migrar_tabela(tabela, '     ', ' ', export_csv=True)
    print("─" * 50)
    print("🎉 Migração concluída!")