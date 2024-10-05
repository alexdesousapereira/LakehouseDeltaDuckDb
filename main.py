from modules.ingestlanding import run_landing
from modules.ingestbronze import run_bronze
from modules.ingestsilver import run_silver
from modules.ingestgold import run_gold

def main():
    print("Iniciando o processo de ingestão de dados...")
    run_landing()
    print("Dados na camada Landing processados.")
    print("Iniciando o processo de ingestão camada Bronze...")
    run_bronze()
    print("Dados na camada Bronze processados.")
    print("Iniciando o processo de ingestão camada Silver...")
    run_silver()
    print("Dados na camada Silver processados.")
    print("Iniciando o processo de ingestão camada Gold...")
    run_gold()
    print("Dados na camada Gold processados.")
    print("Processo de ingestão completo.")

if __name__ == "__main__":
    main()
