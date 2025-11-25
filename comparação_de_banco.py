import psycopg2
from typing import Dict, List, Any, Tuple

class NotNullComparator:
    def __init__(self, conn1_params: Dict, conn2_params: Dict):
        self.conn1 = psycopg2.connect(**conn1_params)
        self.conn2 = psycopg2.connect(**conn2_params)
    
    def get_all_tables(self, conn) -> List[Tuple]:
        """Obtém todas as tabelas"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog')
                ORDER BY table_schema, table_name
            """)
            return cur.fetchall()
    
    def get_columns_nullability(self, conn, schema: str, table: str) -> Dict[str, str]:
        """Obtém o status NOT NULL de cada coluna"""
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    column_name,
                    is_nullable,
                    data_type,
                    column_default
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table))
            
            columns = {}
            for row in cur.fetchall():
                columns[row[0]] = {
                    'is_nullable': row[1],  # 'YES' or 'NO'
                    'data_type': row[2],
                    'default': row[3]
                }
            return columns
    
    def compare_nullability_per_table(self, schema: str, table: str) -> Dict:
        """Compara NOT NULL para uma tabela específica"""
        cols1 = self.get_columns_nullability(self.conn1, schema, table)
        cols2 = self.get_columns_nullability(self.conn2, schema, table)
        
        differences = {
            'columns_missing': {
                'in_2': [],
                'in_1': []
            },
            'nullability_differences': [],
            'identical_columns': []
        }
        
        all_columns = set(cols1.keys()).union(set(cols2.keys()))
        
        for col_name in sorted(all_columns):
            col1 = cols1.get(col_name)
            col2 = cols2.get(col_name)
            
            # Coluna faltante
            if col1 and not col2:
                differences['columns_missing']['in_2'].append({
                    'column': col_name,
                    'nullable': col1['is_nullable'],
                    'data_type': col1['data_type']
                })
                continue
                
            if col2 and not col1:
                differences['columns_missing']['in_1'].append({
                    'column': col_name,
                    'nullable': col2['is_nullable'],
                    'data_type': col2['data_type']
                })
                continue
            
            # Comparar NOT NULL
            nullable1 = col1['is_nullable']  # 'YES' or 'NO'
            nullable2 = col2['is_nullable']  # 'YES' or 'NO'
            
            if nullable1 != nullable2:
                differences['nullability_differences'].append({
                    'column': col_name,
                    'bank1': 'NULLABLE' if nullable1 == 'YES' else 'NOT NULL',
                    'bank2': 'NULLABLE' if nullable2 == 'YES' else 'NOT NULL',
                    'data_type': col1['data_type'],
                    'default_1': col1['default'],
                    'default_2': col2['default']
                })
            else:
                differences['identical_columns'].append({
                    'column': col_name,
                    'nullable': 'NULLABLE' if nullable1 == 'YES' else 'NOT NULL',
                    'data_type': col1['data_type']
                })
        
        return differences
    
    def compare_all_tables_nullability(self) -> Dict:
        """Compara NOT NULL para TODAS as tabelas"""
        print("🔍 Analisando diferenças de NOT NULL em todas as tabelas...")
        
        tables1 = self.get_all_tables(self.conn1)
        tables2 = self.get_all_tables(self.conn2)
        
        tables1_keys = {(schema, name) for schema, name in tables1}
        tables2_keys = {(schema, name) for schema, name in tables2}
        
        results = {
            'tables_only_in_1': list(tables1_keys - tables2_keys),
            'tables_only_in_2': list(tables2_keys - tables1_keys),
            'tables_comparison': {}
        }
        
        common_tables = tables1_keys.intersection(tables2_keys)
        
        for schema, table_name in sorted(common_tables):
            print(f"  📋 Verificando NOT NULL: {schema}.{table_name}")
            
            table_diffs = self.compare_nullability_per_table(schema, table_name)
            
            table_key = f"{schema}.{table_name}"
            results['tables_comparison'][table_key] = {
                'has_nullability_differences': len(table_diffs['nullability_differences']) > 0,
                'differences': table_diffs
            }
        
        return results
    
    def generate_not_null_report(self, results: Dict):
        """Gera relatório focado em NOT NULL"""
        print("\n" + "=" * 100)
        print("🚫 RELATÓRIO DE DIFERENÇAS - NOT NULL")
        print("=" * 100)
        
        # Estatísticas gerais
        total_tables_with_diffs = sum(
            1 for table_info in results['tables_comparison'].values() 
            if table_info['has_nullability_differences']
        )
        
        total_nullability_differences = sum(
            len(table_info['differences']['nullability_differences']) 
            for table_info in results['tables_comparison'].values()
        )
        
        print(f"\n📊 ESTATÍSTICAS GERAIS:")
        print(f"   • Tabelas com diferenças NOT NULL: {total_tables_with_diffs}")
        print(f"   • Total de diferenças NOT NULL: {total_nullability_differences}")
        print(f"   • Tabelas apenas no Banco 1: {len(results['tables_only_in_1'])}")
        print(f"   • Tabelas apenas no Banco 2: {len(results['tables_only_in_2'])}")
        
        # Tabelas faltantes
        if results['tables_only_in_1']:
            print(f"\n❌ TABELAS EXISTENTES APENAS NO BANCO 1:")
            for schema, table in sorted(results['tables_only_in_1']):
                print(f"   🗂️  {schema}.{table}")
        
        if results['tables_only_in_2']:
            print(f"\n❌ TABELAS EXISTENTES APENAS NO BANCO 2:")
            for schema, table in sorted(results['tables_only_in_2']):
                print(f"   🗂️  {schema}.{table}")
        
        # TABELAS COM DIFERENÇAS NOT NULL
        tables_with_not_null_diffs = {
            k: v for k, v in results['tables_comparison'].items() 
            if v['has_nullability_differences']
        }
        
        if tables_with_not_null_diffs:
            print(f"\n⚠️  TABELAS COM DIFERENÇAS NOT NULL ({len(tables_with_not_null_diffs)}):")
            print("=" * 80)
            
            for table_name, table_info in sorted(tables_with_not_null_diffs.items()):
                print(f"\n📊 TABELA: {table_name}")
                
                differences = table_info['differences']
                
                # COLUNAS FALTANTES
                if differences['columns_missing']['in_2']:
                    print(f"\n   ❌ COLUNAS FALTANDO NO BANCO 2:")
                    for col in differences['columns_missing']['in_2']:
                        nullable_status = "NULLABLE" if col['nullable'] == 'YES' else "NOT NULL"
                        print(f"      - {col['column']} ({col['data_type']}) → {nullable_status}")
                
                if differences['columns_missing']['in_1']:
                    print(f"\n   ❌ COLUNAS FALTANDO NO BANCO 1:")
                    for col in differences['columns_missing']['in_1']:
                        nullable_status = "NULLABLE" if col['nullable'] == 'YES' else "NOT NULL"
                        print(f"      - {col['column']} ({col['data_type']}) → {nullable_status}")
                
                # DIFERENÇAS DE NOT NULL
                if differences['nullability_differences']:
                    print(f"\n   🔄 DIFERENÇAS DE NOT NULL:")
                    
                    for diff in differences['nullability_differences']:
                        print(f"\n      📍 COLUNA: {diff['column']}")
                        print(f"         Tipo: {diff['data_type']}")
                        print(f"         Banco 1: {diff['bank1']}")
                        print(f"         Banco 2: {diff['bank2']}")
                        
                        # Informações adicionais úteis
                        if diff['default_1'] or diff['default_2']:
                            print(f"         Default Banco 1: {diff['default_1']}")
                            print(f"         Default Banco 2: {diff['default_2']}")
                        
                        # Análise do impacto
                        if diff['bank1'] == 'NOT NULL' and diff['bank2'] == 'NULLABLE':
                            print(f"         ⚠️  IMPACTO: Banco 1 é mais restritivo (NOT NULL)")
                        elif diff['bank1'] == 'NULLABLE' and diff['bank2'] == 'NOT NULL':
                            print(f"         ⚠️  IMPACTO: Banco 2 é mais restritivo (NOT NULL)")
                
                # Estatísticas da tabela
                total_cols = (len(differences['nullability_differences']) + 
                            len(differences['identical_columns']) +
                            len(differences['columns_missing']['in_1']) +
                            len(differences['columns_missing']['in_2']))
                
                print(f"\n   📈 RESUMO DA TABELA:")
                print(f"      • Total de colunas: {total_cols}")
                print(f"      • Colunas com NOT NULL diferente: {len(differences['nullability_differences'])}")
                print(f"      • Colunas idênticas: {len(differences['identical_columns'])}")
        
        # TABELAS SEM DIFERENÇAS NOT NULL
        identical_tables = {
            k: v for k, v in results['tables_comparison'].items() 
            if not v['has_nullability_differences']
        }
        
        if identical_tables:
            print(f"\n✅ TABELAS COM NOT NULL IDÊNTICO ({len(identical_tables)}):")
            
            # Agrupar por schema para melhor visualização
            tables_by_schema = {}
            for table_full_name in identical_tables.keys():
                schema, table = table_full_name.split('.', 1)
                if schema not in tables_by_schema:
                    tables_by_schema[schema] = []
                tables_by_schema[schema].append(table)
            
            for schema, tables in sorted(tables_by_schema.items()):
                print(f"\n   📂 SCHEMA: {schema}")
                # Mostrar até 8 tabelas por linha
                for i in range(0, len(tables), 8):
                    line_tables = tables[i:i + 8]
                    print("      " + ", ".join(line_tables))
        
        # RELATÓRIO DE IMPACTO
        print(f"\n🎯 RELATÓRIO DE IMPACTO:")
        
        # Contar tipos de diferenças
        not_null_to_nullable = 0
        nullable_to_not_null = 0
        
        for table_info in results['tables_comparison'].values():
            for diff in table_info['differences']['nullability_differences']:
                if diff['bank1'] == 'NOT NULL' and diff['bank2'] == 'NULLABLE':
                    not_null_to_nullable += 1
                elif diff['bank1'] == 'NULLABLE' and diff['bank2'] == 'NOT NULL':
                    nullable_to_not_null += 1
        
        print(f"   • Colunas que eram NOT NULL e ficaram NULLABLE: {not_null_to_nullable}")
        print(f"   • Colunas que eram NULLABLE e ficaram NOT NULL: {nullable_to_not_null}")
        
        if not_null_to_nullable > 0:
            print(f"   ⚠️  ATENÇÃO: {not_null_to_nullable} colunas perderam a restrição NOT NULL!")
        if nullable_to_not_null > 0:
            print(f"   ⚠️  ATENÇÃO: {nullable_to_not_null} colunas ganharam restrição NOT NULL!")

# USO PRINCIPAL
if __name__ == "__main__":
    # CONFIGURAÇÃO DAS CONEXÕES
    DB1_CONFIG = {
        "host": "",
        "database": "",
        "user": "",
        "password": "",
        "port": 
    }
    
    DB2_CONFIG = {
        "host": "",
        "database": "",
        "user": "",
        "password": "",
        "port": 
    }
    
    try:
        print("🚀 Iniciando análise de NOT NULL...")
        comparator = NotNullComparator(DB1_CONFIG, DB2_CONFIG)
        results = comparator.compare_all_tables_nullability()
        comparator.generate_not_null_report(results)
        
    except Exception as e:
        print(f"❌ Erro: {e}")
        import traceback

        traceback.print_exc()
