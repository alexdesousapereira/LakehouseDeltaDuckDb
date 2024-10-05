import os
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv
import glob

# Carrega variáveis de ambiente do arquivo .env
load_dotenv()

# Função para carregar as credenciais do Azure
def get_service_client():
    azure_storage_account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    azure_storage_access_key = os.getenv('AZURE_STORAGE_ACCESS_KEY')

    account_url = f"https://{azure_storage_account_name}.dfs.core.windows.net/"
    return DataLakeServiceClient(account_url=account_url, credential=azure_storage_access_key)

# Função para obter o client do container e do diretório
def get_directory_client(service_client, file_system_name, directory_name):
    container = service_client.get_file_system_client(file_system=file_system_name)
    return container.get_directory_client(directory_name)

# Função para carregar e ler arquivos CSV de um diretório local
def obter_arquivos_csv(diretorio_local):
    return glob.glob(f'{diretorio_local}/*.csv')

# Função para fazer upload de um arquivo para o Azure Data Lake
def upload_arquivo(file_client, caminho_arquivo):
    with open(caminho_arquivo, 'r') as arquivo_local:
        conteudo_arquivo = arquivo_local.read()
        file_client.append_data(data=conteudo_arquivo, offset=0, length=len(conteudo_arquivo))
        file_client.flush_data(len(conteudo_arquivo))

# Função run para iniciar o processo de upload
def run_landing():
    processar_upload_arquivos('dados', 'landing', 'bike_store')

# Função principal para executar o processo completo de upload
def processar_upload_arquivos(diretorio_local, file_system_name, directory_name):
    service_client = get_service_client()
    diretorio_client = get_directory_client(service_client, file_system_name, directory_name)

    arquivos = obter_arquivos_csv(diretorio_local)
    for caminho_arquivo in arquivos:
        nome_arquivo = os.path.basename(caminho_arquivo)
        file_client = diretorio_client.create_file(nome_arquivo)
        upload_arquivo(file_client, caminho_arquivo)

# Executa o upload dos arquivos
if __name__ == "__main__":
    run_landing()
