
from deltalake.writer import write_deltalake
from deltalake import DeltaTable
import duckdb
import pandas
import os
from dotenv import load_dotenv


# Carrega as variáveis do arquivo .env
load_dotenv()

# Carrega as variáveis do arquivo .env
load_dotenv()

# Acessa as credenciais
azure_storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
azure_storage_access_key = os.getenv('AZURE_STORAGE_ACCESS_KEY')
azure_storage_client_id = os.getenv('AZURE_STORAGE_CLIENT_ID')
azure_storage_client_secret = os.getenv('AZURE_STORAGE_CLIENT_SECRET')
azure_storage_tenant_id = os.getenv('AZURE_STORAGE_TENANT_ID')


storage_options = {
    'AZURE_STORAGE_ACCOUNT_NAME': azure_storage_account_name,
    'AZURE_STORAGE_ACCESS_KEY': azure_storage_access_key,
    'AZURE_STORAGE_CLIENT_ID': azure_storage_client_id,
    'AZURE_STORAGE_CLIENT_SECRET': azure_storage_client_secret, 
    'AZURE_STORAGE_TENANT_ID': azure_storage_tenant_id
}



con = duckdb.connect()


# ### Carga Completa arquivos


# ### Funções


def ler_delta_gold(table_name):
    uri= f'az://gold/vendas/{table_name}'
    
    dt = DeltaTable(uri, storage_options=storage_options)
    return dt.to_pandas()


def ler_delta_silver(table_name):
    uri= f'az://silver/vendas/{table_name}'
    
    dt = DeltaTable(uri, storage_options=storage_options)
    return dt.to_pandas()


def escreve_delta_gold(df,table_name, modoEscrita):
    uri= f'az://gold/vendas/{table_name}'
    write_deltalake( 
        uri,
        df,
        mode=modoEscrita,
        storage_options=storage_options
    )

def tabela_delta_existe(table_name):
    """Verifica se a tabela Delta já existe."""
    try:
        DeltaTable(f'az://gold/vendas/{table_name}', storage_options=storage_options)
        return True
    except:
        return False

def processa_fatos_dimesoes():

    orders_sales = ler_delta_silver('order_sales')

    # ### Dim Product

    if tabela_delta_existe('dim_products'):
        dtl_gold_products = ler_delta_gold('dim_products')
        dim_products = con.sql("""
        WITH products AS (
            SELECT DISTINCT 
                product_id,
                product_name,
                brand_name,
                category_name
            FROM orders_sales
        ),
        sk_products AS (
            SELECT 
                row_number() OVER (ORDER BY product_id) AS product_sk,
                product_id,
                product_name,
                brand_name,
                category_name,
                current_date as data_criacao_registro
            FROM products
        ),
        dt_gold_products AS (
            SELECT DISTINCT 
                product_id
            FROM dtl_gold_products
        )
        SELECT 
            *
        FROM sk_products
        WHERE product_id NOT IN (SELECT product_id FROM dt_gold_products)
        """).to_df()
        if len(dim_products) > 0 :
            escreve_delta_gold(dim_products, 'dim_products', 'append')

    else:
        dim_products = con.sql("""
        WITH products AS (
            SELECT DISTINCT 
                product_id,
                product_name,
                brand_name,
                category_name
            FROM orders_sales
        ),
        sk_products AS (
            SELECT 
                row_number() OVER (ORDER BY product_id) AS product_sk,
                product_id,
                product_name,
                brand_name,
                category_name,
                current_date as data_criacao_registro
            FROM products
        )
        SELECT 
            *
        FROM sk_products        
        """).to_df()
        escreve_delta_gold(dim_products, 'dim_products', 'overwrite')

    # ### Dim Customers

    if tabela_delta_existe('dim_customers'):
        dtl_gold_customers = ler_delta_gold('dim_customers')
        dim_customers = con.sql("""
        WITH customers as
        (
            SELECT DISTINCT 
                customer_id,
                customer_name
            FROM orders_sales
        ),
        sk_customers as
        (
            SELECT
                row_number() OVER (ORDER BY customer_id) as customer_sk,
                *,
                current_date as data_criacao_registro
            FROM customers
        ),
        dt_gold_customers AS (
            SELECT DISTINCT 
                customer_id
            FROM dtl_gold_customers
        )
        SELECT 
            * 
        FROM sk_customers
        WHERE customer_id NOT IN (SELECT customer_id FROM dt_gold_customers)
        """).to_df()
        if len(dim_customers) > 0 :
            escreve_delta_gold(dim_customers, 'dim_customers', 'append')
    else:
        dim_customers = con.sql("""
        WITH customers as
        (
            SELECT DISTINCT 
                customer_id,
                customer_name
            FROM orders_sales
        ),
        sk_customers as
        (
            SELECT
                row_number() OVER (ORDER BY customer_id) as customer_sk,
                *,
                current_date as data_criacao_registro
            FROM customers
        )
        SELECT 
            * 
        FROM sk_customers
        """).to_df()
        escreve_delta_gold(dim_customers, 'dim_customers', 'overwrite')

    # ### Dim Stafs

    if tabela_delta_existe('dim_staffs'):
        dtl_gold_staffs = ler_delta_gold('dim_staffs')
        dim_staffs = con.sql("""
        WITH staffs as
        (
            SELECT DISTINCT 
                staff_id,
                staff_name
            FROM orders_sales
        ),
        sk_staffs as
        (
            SELECT
                row_number() OVER (ORDER BY staff_id) as staff_sk,
                *,
                current_date as data_criacao_registro
            FROM staffs
        ),
        dt_gold_staffs AS (
            SELECT DISTINCT
                staff_id
            fROM dtl_gold_staffs
        )
        SELECT 
            *
        FROM  sk_staffs
        WHERE staff_id NOT IN (SELECT staff_id FROM dt_gold_staffs)
        """).to_df()
        if len(dim_staffs) > 0 :
            escreve_delta_gold(dim_staffs, 'dim_staffs', 'append')
    else:
        dim_staffs = con.sql("""
        WITH staffs as
        (
            SELECT DISTINCT 
                staff_id,
                staff_name
            FROM orders_sales
        ),
        sk_staffs as
        (
            SELECT
                row_number() OVER (ORDER BY staff_id) as staff_sk,
                *,
                current_date as data_criacao_registro
            FROM staffs
        )
        SELECT 
            *
        FROM sk_staffs
        """).to_df()
        escreve_delta_gold(dim_staffs, 'dim_staffs', 'overwrite')


    # ### Dim Stores

    if tabela_delta_existe('dim_stores'):
        dtl_gold_stores = ler_delta_gold('dim_stores')

        dim_stores = con.sql("""
        WITH stores as
        (
            SELECT DISTINCT 
                store_id,
                store_name
            FROM orders_sales
        ),
        sk_stores as
        (
            SELECT
                row_number() OVER (ORDER BY store_id) as store_sk,
                *,
                current_date as data_criacao_registro
            FROM stores
        ),
        dt_gold_stores AS (
            SELECT DISTINCT
                store_id
            FROM dtl_gold_stores
        )
        SELECT 
            * 
        FROM sk_stores
        WHERE
            store_id NOT IN (SELECT store_id FROM dt_gold_stores)
        """).to_df()
        if len(dim_stores) > 0 :
            escreve_delta_gold(dim_stores, 'dim_stores', 'append')
    else:
        dim_stores = con.sql("""
        WITH stores as
        (
            SELECT DISTINCT 
                store_id,
                store_name
            FROM orders_sales
        ),
        sk_stores as
        (
            SELECT
                row_number() OVER (ORDER BY store_id) as store_sk,
                *,
                current_date as data_criacao_registro
            FROM stores
        )
        SELECT 
            *
        FROM sk_stores  
        """).to_df()
        escreve_delta_gold(dim_stores, 'dim_stores', 'overwrite')

    # ### Dim Calendario
    if not tabela_delta_existe('dim_date'):
        dim_date = con.sql("""
        WITH tempo AS (
            SELECT 
                CAST(STRFTIME(generate_series, '%Y%m%d') AS INT) AS date_id,
                generate_series AS date,
                YEAR(generate_series) AS year,
                MONTH(generate_series) AS month,
                DAY(generate_series) AS day
            FROM generate_series(DATE '2010-01-01', DATE '2030-12-31', INTERVAL '1 DAY')
        ),
        sk_date AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY date_id) AS date_sk,
                *
            FROM tempo
        )

        SELECT * 
        FROM sk_date
        """).to_df()

        escreve_delta_gold(dim_date, 'dim_date', 'append')

    # ### FATOS
    dim_products = ler_delta_gold('dim_products')
    dim_customers = ler_delta_gold('dim_customers')
    dim_staffs = ler_delta_gold('dim_staffs')
    dim_stores = ler_delta_gold('dim_stores')
    dim_date = ler_delta_gold('dim_date')

    # #### Fact Sales

    dtl_orders_sales = ler_delta_silver('order_sales')

    if tabela_delta_existe('fact_sales'):
        dtl_gold_fact_sales = ler_delta_gold('fact_sales')
        fact_sales = con.sql("""
        SELECT 
            P.product_sk,
            C.customer_sk,
            ST.staff_sk,
            S.store_sk,
            D.date_sk,
            OS.item_id,
            OS.order_id,
            OS.order_date,
            OS.quantity,
            OS.list_price,
            OS.discount,
            current_date as data_criacao_registro
        FROM orders_sales OS
        LEFT JOIN dim_products P ON OS.product_id = P.product_id
        LEFT JOIN dim_customers C ON OS.customer_id = C.customer_id
        LEFT JOIN dim_staffs ST ON OS.staff_id = ST.staff_id
        LEFT JOIN dim_stores S ON OS.store_id = S.store_id
        LEFT JOIN dim_date D ON CAST(STRFTIME(OS.order_date, '%Y%m%d') AS INT) = D.date_id
        WHERE OS.order_date > (SELECT MAX(order_date) FROM dtl_gold_fact_sales)
        """).to_df()
        if len(fact_sales) > 0 :
            escreve_delta_gold(fact_sales, 'fact_sales', 'append')
    else:
        fact_sales = con.sql("""
        SELECT 
            P.product_sk,
            C.customer_sk,
            ST.staff_sk,
            S.store_sk,
            D.date_sk,
            OS.item_id,
            OS.order_id,
            OS.order_date,
            OS.quantity,
            OS.list_price,
            OS.discount,
            current_date as data_criacao_registro
        FROM orders_sales OS
        LEFT JOIN dim_products P ON OS.product_id = P.product_id
        LEFT JOIN dim_customers C ON OS.customer_id = C.customer_id
        LEFT JOIN dim_staffs ST ON OS.staff_id = ST.staff_id
        LEFT JOIN dim_stores S ON OS.store_id = S.store_id  
        LEFT JOIN dim_date D ON CAST(STRFTIME(OS.order_date, '%Y%m%d') AS INT) = D.date_id
        """).to_df()
        escreve_delta_gold(fact_sales, 'fact_sales', 'overwrite')

    # #### Fato Stocks

    stocks_snapchot = ler_delta_silver('stocks_snapshot')

    if tabela_delta_existe('stocks_snapshot'):
        dtl_gold_fact_stocks = ler_delta_gold('fato_stocks')
        fact_stocks = con.sql("""
            SELECT
                P.product_sk,
                S.store_sk,
                D.date_sk,
                SS.quantity,
                current_date as data_criacao_registro
            FROM stocks_snapchot SS
            LEFT JOIN dim_products P ON SS.product_id = P.product_id
            LEFT JOIN dim_stores S ON SS.store_id = S.store_id
            LEFT JOIN dim_date D ON cast(strftime(SS.dt_stock, '%Y%m%d') as int) = D.date_id
            WHERE SS.dt_stock > (
                SELECT
                    MAX(D.date)
                FROM dtl_gold_fact_stocks FS
                LEFT JOIN dim_date D ON FS.date_SK = D.date_SK
            )
        """).to_df()

        if len(fact_stocks) > 0 :
            escreve_delta_gold(fact_stocks, 'fato_stocks', 'append')
    else:
        fact_stocks = con.sql("""
            SELECT
                P.product_sk,
                S.store_sk,
                D.date_sk,
                SS.quantity,
                current_date as data_criacao_registro
            FROM stocks_snapchot SS
            LEFT JOIN dim_products P ON SS.product_id = P.product_id
            LEFT JOIN dim_stores S ON SS.store_id = S.store_id
            LEFT JOIN dim_date D ON cast(strftime(SS.dt_stock, '%Y%m%d') as int) = D.date_id
        """).to_df()
        escreve_delta_gold(fact_stocks, 'fato_stocks', 'overwrite')
    # ## Fecha conexões
    con.close()


def run_gold():
    """Executa o processamento dos dados para a camada gold."""
    processa_fatos_dimesoes()

if __name__ == "__main__":
    run_gold()