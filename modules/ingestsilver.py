from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import pandas
import os
from dotenv import load_dotenv


# Conecta ao DuckDB
con = duckdb.connect()

# Carrega as variáveis do arquivo .env
load_dotenv()

# Acessa as credenciais do Azure
azure_storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
azure_storage_access_key = os.getenv('AZURE_STORAGE_ACCESS_KEY')
azure_storage_client_id = os.getenv('AZURE_STORAGE_CLIENT_ID')
azure_storage_client_secret = os.getenv('AZURE_STORAGE_CLIENT_SECRET')
azure_storage_tenant_id = os.getenv('AZURE_STORAGE_TENANT_ID')

# Configurações de armazenamento
storage_options = {
    'AZURE_STORAGE_ACCOUNT_NAME': azure_storage_account_name,
    'AZURE_STORAGE_ACCESS_KEY': azure_storage_access_key,
    'AZURE_STORAGE_CLIENT_ID': azure_storage_client_id,
    'AZURE_STORAGE_CLIENT_SECRET': azure_storage_client_secret, 
    'AZURE_STORAGE_TENANT_ID': azure_storage_tenant_id
}

# Funções para leitura e escrita no Delta Lake
def ler_delta_bronze(table_name):
    """Lê uma tabela Delta da camada Bronze."""
    uri = f'az://bronze/vendas/{table_name}'
    dt = DeltaTable(uri, storage_options=storage_options)
    return dt.to_pandas()

def ler_delta_silver(table_name):
    """Lê uma tabela Delta da camada Silver."""
    uri = f'az://silver/vendas/{table_name}'
    dt = DeltaTable(uri, storage_options=storage_options)
    return dt.to_pandas()

def escreve_delta_silver(df, table_name, modo_escrita):
    """Escreve dados na camada Silver do Delta Lake."""
    uri = f'az://silver/vendas/{table_name}'
    write_deltalake(uri, df, mode=modo_escrita, storage_options=storage_options)

def tabela_delta_existe(table_name):
    """Verifica se a tabela Delta já existe."""
    try:
        DeltaTable(f'az://silver/vendas/{table_name}', storage_options=storage_options)
        return True
    except:
        return False

# Função para processar a tabela order_sales
def processa_order_sales():
    """Processa e carrega dados de order_sales na camada Silver."""
    # Lendo dados da camada Bronze necessários
    order_items = ler_delta_bronze('order_items')
    orders = ler_delta_bronze('orders')
    products = ler_delta_bronze('products')
    brands = ler_delta_bronze('brands')
    categories = ler_delta_bronze('categories')
    customers = ler_delta_bronze('customers')
    staffs = ler_delta_bronze('staffs')
    stores = ler_delta_bronze('stores')

    if tabela_delta_existe('order_sales'):
        dtl_orders_sales = ler_delta_silver('order_sales')
        order_sales = con.sql(f"""
            WITH order_sales_bronze AS (
                SELECT
                    P.product_id,
                    P.product_name,
                    B.brand_name,
                    CT.category_name,
                    C.customer_id,
                    C.first_name || ' ' || C.last_name AS customer_name,
                    S.staff_id,
                    S.first_name || ' ' || S.last_name AS staff_name,
                    ST.store_id,
                    ST.store_name,
                    OI.order_id,
                    OI.item_id,
                    O.order_date,
                    OI.quantity,
                    OI.list_price,
                    OI.discount
                FROM order_items OI
                LEFT JOIN orders O ON OI.order_id = O.order_id
                LEFT JOIN products P ON P.product_id = OI.product_id
                LEFT JOIN brands B ON P.brand_id = B.brand_id
                LEFT JOIN categories CT ON P.category_id = CT.category_id
                LEFT JOIN customers C ON O.customer_id = C.customer_id
                LEFT JOIN staffs S ON O.staff_id = S.staff_id
                LEFT JOIN stores ST ON ST.store_id = O.store_id
            ),
            dt_order_sales AS (
                SELECT MAX(order_date) AS order_date FROM dtl_orders_sales
            )
            SELECT * FROM order_sales_bronze
            WHERE order_date > (SELECT order_date FROM dt_order_sales)
        """).to_df()

        if len(order_sales) > 0:
            escreve_delta_silver(order_sales, 'order_sales', 'append')
    else:
        order_sales = con.sql(f"""
            SELECT
                P.product_id,
                P.product_name,
                B.brand_name,
                CT.category_name,
                C.customer_id,
                C.first_name || ' ' || C.last_name AS customer_name,
                S.staff_id,
                S.first_name || ' ' || S.last_name AS staff_name,
                ST.store_id,
                ST.store_name,
                OI.order_id,
                OI.item_id,
                O.order_date,
                OI.quantity,
                OI.list_price,
                OI.discount
            FROM order_items OI
            LEFT JOIN orders O ON OI.order_id = O.order_id
            LEFT JOIN products P ON P.product_id = OI.product_id
            LEFT JOIN brands B ON P.brand_id = B.brand_id
            LEFT JOIN categories CT ON P.category_id = CT.category_id
            LEFT JOIN customers C ON O.customer_id = C.customer_id
            LEFT JOIN staffs S ON O.staff_id = S.staff_id
            LEFT JOIN stores ST ON ST.store_id = O.store_id
        """).to_df()

        escreve_delta_silver(order_sales, 'order_sales', 'append')

# Função para processar o snapshot de stocks
def processa_stocks_snapshot():
    """Processa e carrega dados de stocks_snapshot na camada Silver."""
    # Lendo dados da camada Bronze necessários
    stocks = ler_delta_bronze('stocks')

    stocks_snapshot = con.sql("""
        SELECT *, current_date AS dt_stock
        FROM stocks
    """).to_df()
    escreve_delta_silver(stocks_snapshot, 'stocks_snapshot', 'append')

# Função principal para orquestrar o pipeline
def run_silver():
    """Executa o processamento dos dados para a camada Silver."""
    # Processando as tabelas de order_sales e stocks_snapshot
    processa_order_sales()
    processa_stocks_snapshot()
    con.close()

# Executa o pipeline de dados
if __name__ == "__main__":
    run_silver()
    
