import psycopg2
import oracledb
from typing import Dict, List, Set, Tuple, Optional
from dataclasses import dataclass
from contextlib import contextmanager
import logging

# Configuração de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

oracledb.init_oracle_client(lib_dir='C:\instantclient_23_8')

# ==== CONFIGURAÇÕES DE CONEXÃO ====
ORACLE_CONFIG = {
    "user": "estagiario",
    "password": "estagiario",
    "dsn": "172.16.4.116/apexdb.guarulhos.sp.gov.br"
}

POSTGRES_CONFIG = {
    "user": "postgres",
    "password": "postgres",
    "host": "172.16.4.178",
    "port": "5432",
    "database": "sistema_pmg"
}

@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool
    original_name: str = None

@dataclass
class TableComparison:
    table_name: str
    oracle_columns: Dict[str, ColumnInfo]
    postgres_columns: Dict[str, ColumnInfo]
    oracle_count: Optional[int]
    postgres_count: Optional[int]

# ==== GERENCIADORES DE CONEXÃO ====
@contextmanager
def get_oracle_connection():
    conn = None
    try:
        conn = oracledb.connect(**ORACLE_CONFIG)
        yield conn
    except Exception as e:
        logger.error(f"Erro na conexão Oracle: {e}")
        raise
    finally:
        if conn:
            conn.close()

@contextmanager
def get_postgres_connection():
    conn = None
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        yield conn
    except Exception as e:
        logger.error(f"Erro na conexão PostgreSQL: {e}")
        raise
    finally:
        if conn:
            conn.close()

# ==== FUNÇÕES DE BANCO DE DADOS ====
def get_tables(connection, query: str, params: tuple = None) -> List[str]:
    """Função genérica para obter tabelas de qualquer banco"""
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params or ())
            return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Erro ao buscar tabelas: {e}")
        return []

def get_tables_oracle(schema: str) -> List[str]:
    """Obtém tabelas do Oracle"""
    query = """
        SELECT table_name FROM all_tables 
        WHERE owner = UPPER(:schema)
    """
    with get_oracle_connection() as conn:
        return get_tables(conn, query, {'schema': schema})

def get_tables_postgres(schema: str) -> List[str]:
    """Obtém tabelas do PostgreSQL"""
    query = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = %s AND table_type = 'BASE TABLE'
    """
    with get_postgres_connection() as conn:
        return get_tables(conn, query, (schema,))

def get_columns_oracle(schema: str, table: str) -> Dict[str, ColumnInfo]:
    """Obtém colunas do Oracle"""
    query = """
        SELECT column_name, data_type, nullable
        FROM all_tab_columns
        WHERE table_name = UPPER(:tabname) AND owner = UPPER(:schname)
        ORDER BY column_id
    """
    params = {'tabname': table, 'schname': schema}
    
    with get_oracle_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return {
                row[0].lower(): ColumnInfo(
                    name=row[0].lower(),
                    data_type=row[1],
                    nullable=row[2] == 'Y',
                    original_name=row[0]
                )
                for row in cursor.fetchall()
            }

def get_columns_postgres(schema: str, table: str) -> Dict[str, ColumnInfo]:
    """Obtém colunas do PostgreSQL"""
    query = """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = %s AND table_schema = %s
        ORDER BY ordinal_position
    """
    
    with get_postgres_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, (table, schema))
            return {
                row[0].lower(): ColumnInfo(
                    name=row[0].lower(),
                    data_type=row[1],
                    nullable=row[2] == 'YES',
                    original_name=row[0]
                )
                for row in cursor.fetchall()
            }

def count_records_oracle(schema: str, table: str) -> Optional[int]:
    """Conta registros no Oracle"""
    try:
        with get_oracle_connection() as conn:
            with conn.cursor() as cursor:
                # Usando bind parameters para evitar SQL injection
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                return cursor.fetchone()[0]
    except Exception as e:
        logger.warning(f"Erro ao contar registros Oracle para {schema}.{table}: {e}")
        return None

def count_records_postgres(schema: str, table: str) -> Optional[int]:
    """Conta registros no PostgreSQL"""
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
                return cursor.fetchone()[0]
    except Exception as e:
        logger.warning(f"Erro ao contar registros PostgreSQL para {schema}.{table}: {e}")
        return None

def normalize_type(data_type: str) -> str:
    """Normaliza tipos de dados para comparação"""
    type_map = {
        'varchar2': 'character varying',
        'number': 'numeric',
        'clob': 'text',
        'blob': 'bytea',
        'date': 'timestamp',
        'timestamp': 'timestamp'
    }
    base_type = data_type.split('(')[0].lower()
    return type_map.get(base_type, data_type.lower())

def compare_columns(col1: ColumnInfo, col2: ColumnInfo) -> List[str]:
    """Compara duas colunas e retorna lista de diferenças"""
    differences = []
    
    # Comparar tipos (com normalização)
    type1 = normalize_type(col1.data_type)
    type2 = normalize_type(col2.data_type)
    
    if type1 != type2:
        differences.append(f"TIPO: Oracle({col1.data_type}) x PG({col2.data_type})")
    
    # Comparar nulabilidade
    if col1.nullable != col2.nullable:
        null1 = "NULL" if col1.nullable else "NOT NULL"
        null2 = "NULL" if col2.nullable else "NOT NULL"
        differences.append(f"NULABILIDADE: Oracle({null1}) x PG({null2})")
    
    return differences

def compare_tables(comparison: TableComparison) -> Dict:
    """Compara duas tabelas e retorna estatísticas"""
    all_columns = set(comparison.oracle_columns.keys()) | set(comparison.postgres_columns.keys())
    
    stats = {
        'total_columns': len(all_columns),
        'equal_columns': 0,
        'different_columns': 0,
        'oracle_only': 0,
        'postgres_only': 0,
        'details': []
    }
    
    for col_name in sorted(all_columns):
        oracle_col = comparison.oracle_columns.get(col_name)
        postgres_col = comparison.postgres_columns.get(col_name)
        
        if oracle_col and not postgres_col:
            stats['oracle_only'] += 1
            stats['details'].append(
                f"    Só Oracle: {oracle_col.original_name} {oracle_col.data_type} "
                f"{'NULL' if oracle_col.nullable else 'NOT NULL'}"
            )
        elif postgres_col and not oracle_col:
            stats['postgres_only'] += 1
            stats['details'].append(
                f"    Só PG: {postgres_col.original_name} {postgres_col.data_type} "
                f"{'NULL' if postgres_col.nullable else 'NOT NULL'}"
            )
        else:
            differences = compare_columns(oracle_col, postgres_col)
            if not differences:
                stats['equal_columns'] += 1
            else:
                stats['different_columns'] += 1
                stats['details'].append(f"    {oracle_col.original_name}: {'; '.join(differences)}")
    
    return stats

def comparar_todos_schemas(schema_oracle: str, schema_pg: str):
    """Função principal de comparação entre schemas"""
    logger.info(f"Iniciando comparação: Oracle.{schema_oracle} vs PostgreSQL.{schema_pg}")
    
    # Obter tabelas
    oracle_tables = {t.lower(): t for t in get_tables_oracle(schema_oracle)}
    postgres_tables = {t.lower(): t for t in get_tables_postgres(schema_pg)}
    all_tables = set(oracle_tables.keys()) | set(postgres_tables.keys())
    
    logger.info(f"Encontradas {len(all_tables)} tabelas para comparar")
    
    for table_lower in sorted(all_tables):
        # Preparar dados para comparação
        oracle_table_name = oracle_tables.get(table_lower)
        postgres_table_name = postgres_tables.get(table_lower)
        
        # Obter colunas
        oracle_columns = (get_columns_oracle(schema_oracle, oracle_table_name) 
                         if oracle_table_name else {})
        postgres_columns = (get_columns_postgres(schema_pg, postgres_table_name) 
                           if postgres_table_name else {})
        
        # Contar registros
        oracle_count = (count_records_oracle(schema_oracle, oracle_table_name) 
                       if oracle_table_name else None)
        postgres_count = (count_records_postgres(schema_pg, postgres_table_name) 
                         if postgres_table_name else None)
        
        # Criar objeto de comparação
        table_comparison = TableComparison(
            table_name=table_lower,
            oracle_columns=oracle_columns,
            postgres_columns=postgres_columns,
            oracle_count=oracle_count,
            postgres_count=postgres_count
        )
        
        # Comparar tabelas
        stats = compare_tables(table_comparison)
        
        # Exibir resultados
        print(f"\nTabela: {table_lower}")
        print(f"  Colunas: {stats['total_columns']} | "
              f"Iguais: {stats['equal_columns']} | "
              f"Diferentes: {stats['different_columns']} | "
              f"Só Oracle: {stats['oracle_only']} | "
              f"Só PG: {stats['postgres_only']}")
        
        print(f"  Registros: Oracle = {oracle_count or '-'} | "
              f"PG = {postgres_count or '-'}")
        
        if stats['details']:
            print("  Diferenças:")
            for detail in stats['details']:
                print(detail)

def main():
    """Função principal"""
    try:
        comparar_todos_schemas('portal_informacao', 'sistema_pmg')
        logger.info("Comparação concluída com sucesso")
    except Exception as e:
        logger.error(f"Erro durante a comparação: {e}")
        raise

if __name__ == "__main__":
    main()