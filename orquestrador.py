import subprocess
import time
import logging
import schedule
import os

# --- Configuração ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("log_orquestrador.log"),
        logging.StreamHandler()
    ]
)

# Caminho base onde estão os scripts
CAMINHO_BASE = r"C:\Users\Planejamento\Documents\Drive"

# Lista completa dos scripts com caminho absoluto
scripts_para_rodar = [
    os.path.join(CAMINHO_BASE, "Qive.py"),    
    os.path.join(CAMINHO_BASE, "Qlik_View.py"),
    os.path.join(CAMINHO_BASE, "Qlik_Sense.py"),
    os.path.join(CAMINHO_BASE, "Tratamento das Vendas.py"),
    os.path.join(CAMINHO_BASE, "Tratamento dos Pendentes.py"),
    os.path.join(CAMINHO_BASE, "Painel de tanques.py"),
    os.path.join(CAMINHO_BASE, "Capacidade.py"),   
    os.path.join(CAMINHO_BASE, "Trafegus.py"),  
    os.path.join(CAMINHO_BASE, "Atualização Programados Drives.py"),  
    os.path.join(CAMINHO_BASE, "Bsoft.py"),
    os.path.join(CAMINHO_BASE, "Atualização Transito Drives.py"),
    os.path.join(CAMINHO_BASE, "Drive Anidro e Biodiesel.py"),
    os.path.join(CAMINHO_BASE, "Drive Hidratado.py"),
    os.path.join(CAMINHO_BASE, "Drive Diesel e Gasolina.py"),
    os.path.join(CAMINHO_BASE, "Transporte.py")   
    
]


# --- Lógica do Job ---
def executar_pipeline():
    logging.info("="*50)
    logging.info("AGENDAMENTO ACIONADO: Iniciando a execução do pipeline.")
    
    for script in scripts_para_rodar:
        try:
            logging.info(f"Executando script: '{script}'...")
            
            # Usando check=True para garantir que erros no subprocesso sejam capturados
            subprocess.run(['python', script], check=True)
            
            logging.info(f"Script '{script}' finalizado com sucesso.")
            
        except subprocess.CalledProcessError:
            # ALTERADO: Informa o erro e continua para o próximo script
            logging.error(f"ERRO: O script '{script}' retornou um erro. Pulando para o próximo script.")
            continue 
            
        except FileNotFoundError:
            # ALTERADO: Informa o erro e continua para o próximo script
            logging.error(f"ERRO: O arquivo do script '{script}' não foi encontrado. Pulando para o próximo script.")
            continue
            
        except Exception as e:
            # ALTERADO: Informa o erro e continua para o próximo script
            logging.error(f"Um erro inesperado ocorreu com o script '{script}': {e}. Pulando para o próximo script.")
            continue

    logging.info("Pipeline finalizado. Todos os scripts (que não falharam) foram executados.")

# --- Agendamento e Loop Principal ---
if __name__ == "__main__":
    logging.info("Orquestrador iniciado.")
    schedule.every(50).minutes.do(executar_pipeline)

    # Executa uma vez ao iniciar o script
    executar_pipeline()
    logging.info("Primeira execução concluída. O agendador assume a partir de agora.")

    while True:
        schedule.run_pending()
        time.sleep(1)