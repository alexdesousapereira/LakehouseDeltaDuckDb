
Power BI Desktop:
https://www.microsoft.com/pt-br/download/details.aspx?id=58494

Libs dependentes:
pip install matplotlib pandas


from deltalake import DeltaTable

storage_options = {
    'AZURE_STORAGE_ACCOUNT_NAME': 'conta de amazenamento',
    'AZURE_STORAGE_ACCESS_KEY': '',
    'AZURE_STORAGE_CLIENT_ID': '',
    'AZURE_STORAGE_CLIENT_SECRET': '',
    'AZURE_STORAGE_TENANT_ID': ''
}

uri = f'az://gold/vendas/dim_products'

dt = DeltaTable(uri, storage_options=storage_options)
dim_products = dt.to_pandas()
dim_products


dim_products
fact_sales

















