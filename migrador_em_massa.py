
import pymysql
import psycopg2

def migrar_tabela(tabela):
    """Migra uma tabela do MySQL para PostgreSQL"""
    try:

        mysql_conn = pymysql.connect(
            host='', db='',
            user='', password='', port=3306
        )
        pg_conn = psycopg2.connect(
            host='', dbname='',
            user='', password=''
        )
        
        mysql_cursor = mysql_conn.cursor()
        pg_cursor = pg_conn.cursor()
        

        mysql_cursor.execute(f"SHOW COLUMNS FROM {tabela}")
        colunas_info = mysql_cursor.fetchall()
        tipos_colunas = [col[1] for col in colunas_info]
        nomes_colunas = [col[0] for col in colunas_info]
        
       
        mysql_cursor.execute(f"SELECT * FROM {tabela}")
        dados = mysql_cursor.fetchall()
        
        print(f"📋 Migrando {tabela}: {len(dados)} registros")
        
        
        for reg in dados:
            valores = []
            for i, valor in enumerate(reg):
                tipo_mysql = tipos_colunas[i].lower()
                nome_coluna = nomes_colunas[i].lower()
                
                if valor is None:
                    valores.append('NULL')
                elif 'blob' in tipo_mysql or 'binary' in tipo_mysql:
                    # BLOB/BINARY para BYTEA
                    if isinstance(valor, bytes):
                        valores.append(f"'\\x{valor.hex()}'")
                    else:
                        valor_bytes = "'"+(valor)+"'"
                elif 'tinyint' in tipo_mysql:
                   
                    valores.append(f"'{str(valor)}'")
                elif ('int(' in tipo_mysql or 
                      'integer' in tipo_mysql or 
                      'smallint' in tipo_mysql or 
                      'bigint' in tipo_mysql):
                   
                    valores.append(str(valor))
                elif ('datetime' in tipo_mysql or 
                      'timestamp' in tipo_mysql or 
                      'date' in tipo_mysql or 
                      'time' in tipo_mysql):
                    
                    if valor:
                        valores.append(f"'{valor}'")
                    else:
                        valores.append('NULL')
                elif isinstance(valor, str):
                    
                    valor_escaped = valor.replace("'", "''")
                    valores.append(f"'{valor_escaped}'")
                else:
               
                    valores.append(f"'{str(valor)}'")
            
           
            query = f"INSERT INTO chatbot.{tabela} VALUES ({','.join(valores)})"
            pg_cursor.execute(query)
        
        pg_conn.commit()
        print(f"✅ {tabela} migrada com sucesso!")
        
    except Exception as e:
        print(f"❌ Erro em {tabela}: {e}")
    finally:
        mysql_cursor.close()
        pg_cursor.close()
        mysql_conn.close()
        pg_conn.close()


TABELAS = [
'   '
]


if __name__ == "__main__":
    print("🚀 Iniciando migração MySQL → PostgreSQL")

    print("─" * 50)
    
    for tabela in TABELAS:
        migrar_tabela(tabela)


       
    print("─" * 50)
    print("🎉 Migração concluída! do banco Mysql para o PostgreSQL ")