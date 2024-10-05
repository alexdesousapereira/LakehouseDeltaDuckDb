from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Acessa as credenciais
azure_storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
azure_storage_access_key = os.getenv('AZURE_STORAGE_ACCESS_KEY')
azure_storage_client_id = os.getenv('AZURE_STORAGE_CLIENT_ID')
azure_storage_client_secret = os.getenv('AZURE_STORAGE_CLIENT_SECRET')
azure_storage_tenant_id = os.getenv('AZURE_STORAGE_TENANT_ID')

# Configurações do armazenamento
storage_options = {
    'AZURE_STORAGE_ACCOUNT_NAME': azure_storage_account_name,
    'AZURE_STORAGE_ACCESS_KEY': azure_storage_access_key,
    'AZURE_STORAGE_CLIENT_ID': azure_storage_client_id,
    'AZURE_STORAGE_CLIENT_SECRET': azure_storage_client_secret,
    'AZURE_STORAGE_TENANT_ID': azure_storage_tenant_id
}

# Função que escreve no Delta Lake
def escreve_delta(df, table_name, modo_escrita):
    uri = f'az://bronze/vendas/{table_name}'
    write_deltalake(uri, df, mode=modo_escrita, storage_options=storage_options)

# Função que lê do Delta Lake
def ler_delta(table_name):
    uri = f'az://bronze/vendas/{table_name}'
    return DeltaTable(uri, storage_options=storage_options)

# Verifica se a tabela Delta existe
def tabela_delta_existe(table_name):
    try:
        DeltaTable(f'az://bronze/vendas/{table_name}', storage_options=storage_options)
        return True
    except:
        return False
    
def run_bronze():
    processar_dados()

# Função principal que executa o processo completo
def processar_dados():
    # Conecta ao DuckDB e configura Azure e Delta
    con = duckdb.connect()
    con.sql("LOAD delta;")
    con.sql("LOAD azure;")

    con.sql(f"""
    CREATE SECRET azure_spn (
        TYPE AZURE,
        PROVIDER SERVICE_PRINCIPAL,
        TENANT_ID '{azure_storage_tenant_id}',
        CLIENT_ID '{azure_storage_client_id}',
        CLIENT_SECRET '{azure_storage_client_secret}',
        ACCOUNT_NAME '{azure_storage_account_name}'
    );
    """)

    # Função escrever arquivos
    arquivos = ['brands', 'categories', 'customers', 'products', 'staffs', 'stores', 'order_items', 'orders', 'stocks']
    for tabela in arquivos:
        if not tabela_delta_existe(tabela):
            df = con.sql(f"""
                SELECT * FROM 'abfss://landing/bike_store/{tabela}.csv'
            """).to_df()
            escreve_delta(df, tabela, 'append')

    # Processa incrementos nas dimensões
    arquivos_dimensoes = ['brands', 'categories', 'customers', 'products', 'staffs', 'stores']
    for tabela in arquivos_dimensoes:
        new_df = con.sql(f"SELECT * FROM 'abfss://landing/bike_store/{tabela}.csv'").to_df()
        tabela_dtl = ler_delta(tabela)
        coluna = 'category' if tabela == 'categories' else tabela[:-1]
        
        tabela_dtl.merge(
            source=new_df,
            predicate=f'target.{coluna}_id = source.{coluna}_id',
            source_alias="source",
            target_alias="target"
        ).when_not_matched_insert_all().execute()

    # Processa incremental em fatos: order_items e orders
    processa_fatos(con, 'order_items')
    processa_fatos(con, 'orders', 'order_date')

    # Processa dados de estoque
    df_stocks = con.sql(f"SELECT * FROM 'abfss://landing/bike_store/stocks.csv'").to_df()
    escreve_delta(df_stocks, 'stocks', 'overwrite')

    # Fecha a conexão
    con.close()

# Função para processar fatos de forma incremental
def processa_fatos(con, tabela, coluna_data=None):
    # Carrega a tabela Delta existente como pandas DataFrame
    tabela_delta = ler_delta(tabela).to_pandas()
    
    # Registra o DataFrame como uma tabela temporária no DuckDB
    con.register('tabela_delta_temp', tabela_delta)
    
    if tabela == 'order_items':
        # Executa a consulta SQL referenciando a tabela temporária registrada
        df = con.sql(f"""
        WITH dlt_order_items AS (
            SELECT * FROM tabela_delta_temp
        ),
        arquivo_order_items AS (
            SELECT * FROM 'abfss://landing/bike_store/order_items.csv'
        )
        SELECT AR.*
        FROM arquivo_order_items AR
        LEFT JOIN dlt_order_items DLT
        ON hash(AR.order_id, AR.product_id) = hash(DLT.order_id, DLT.product_id)
        WHERE DLT.order_id IS NULL
        """).to_df()
    
    elif coluna_data:
        # Processa outras tabelas com base na coluna de data
        df = con.sql(f"""
        WITH arquivo AS (
            SELECT * FROM 'abfss://landing/bike_store/{tabela}.csv'
        ), 
        dlt AS (
            SELECT MAX({coluna_data}) AS {coluna_data} FROM tabela_delta_temp
        )
        SELECT AR.* 
        FROM arquivo AR 
        WHERE AR.{coluna_data} > (SELECT {coluna_data} FROM dlt)
        """).to_df()

    # Se houver registros, escreve no Delta Lake
    if len(df) > 0:
        escreve_delta(df, tabela, 'append')

# Executa o pipeline completo
if __name__ == "__main__":
    run_bronze()
