import pyautogui
import time
import sys
import os            # Para descobrir caminhos e rodar comandos
from PIL import Image # Para carregar a imagem e evitar erros de acentuação

# --- Configurações de Segurança ---
pyautogui.PAUSE = 0.5
pyautogui.FAILSAFE = True
# ----------------------------------

# =====================================================================
# CONFIGURAÇÃO DE CAMINHOS
# =====================================================================

try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
except NameError:
    script_dir = os.getcwd() 
print(f"O script está rodando a partir de: {script_dir}")

caminho_arquivo_pbix = r"C:\Users\Planejamento\Documents\Drive\Politica de Estoques.pbix"
nome_janela_arquivo = "Politica de Estoques"

# --- [CAMINHO ATUALIZADO] ---
# O script agora vai procurar por esta imagem no PASSO 6
caminho_imagem_clicar = r"C:\Users\Planejamento\Documents\Drive\AtualizacaoBI\Meu_Workspace.png"
# =====================================================================


def atualizar_bi_por_atalho():
    print("Iniciando a automação do Power BI Desktop...")

    try:
        # -----------------------------------------------------------------
        # PASSO 1: ABRIR O ARQUIVO .PBIX DIRETAMENTE
        # -----------------------------------------------------------------
        print(f"Abrindo o arquivo: {caminho_arquivo_pbix}...")
        os.system(f'start "" "{caminho_arquivo_pbix}"')
        
        # -----------------------------------------------------------------
        # PASSO 2: ESPERAR O PROGRAMA E O ARQUIVO CARREGAREM
        # -----------------------------------------------------------------
        tempo_de_espera = 30 
        print(f"Aguardando {tempo_de_espera} segundos para o PBI e o relatório abrirem...")
        time.sleep(tempo_de_espera) 

        # -----------------------------------------------------------------
        # PASSO 3: MAXIMIZAR E FOCAR A JANELA
        # -----------------------------------------------------------------
        try:
            janela_pbi = pyautogui.getWindowsWithTitle(nome_janela_arquivo)[0]
            if janela_pbi:
                janela_pbi.maximize()
                print("Janela do Power BI maximizada.")
                janela_pbi.activate()
                print("Janela do Power BI focada.")
                time.sleep(1)
            else:
                print("Aviso: Janela não encontrada pelo nome. Continuando...")
        except Exception as e:
            print(f"Aviso: Não consegui encontrar/maximizar a janela '{nome_janela_arquivo}'. Continuando...")

        # -----------------------------------------------------------------
        # PASSO 4: EXECUTAR A SEQUÊNCIA DE ATALHOS (ATUALIZAR)
        # -----------------------------------------------------------------
        print("Enviando sequência de atalhos (Alt, C, R, Tab, Enter...)...")
        pyautogui.press('alt')
        time.sleep(0.5)
        pyautogui.press('c')
        time.sleep(0.5)
        pyautogui.press('r')
        time.sleep(0.5)
        pyautogui.press('tab')
        time.sleep(0.5) 
        pyautogui.press('enter')
        print("Comando de atualização enviado. Aguardando 60s...")
        time.sleep(60) 
        pyautogui.press('tab')
        time.sleep(5) 
        pyautogui.press('tab')
        time.sleep(1)
        pyautogui.press('enter')
        print("Atualizado com sucesso.")
        time.sleep(2)

        # -----------------------------------------------------------------
        # PASSO 5: CLICAR NO MEIO DA TELA
        # -----------------------------------------------------------------
        print("Movendo para o centro da tela...")
        screenWidth, screenHeight = pyautogui.size()
        middleX = screenWidth // 2
        middleY = screenHeight // 2
        pyautogui.moveTo(middleX, middleY, duration=0.25)
        pyautogui.click()
        print(f"Clique realizado no centro da tela ({middleX}, {middleY}).")

        # -----------------------------------------------------------------
        # PASSO 6: PUBLICAR O RELATÓRIO (MODIFICADO)
        # -----------------------------------------------------------------
        print("--- INICIANDO ETAPA DE PUBLICAÇÃO ---")
        print("Enviando atalhos (Alt, C, P...)")
        
        pyautogui.press('alt')
        time.sleep(0.5)
        pyautogui.press('c')
        time.sleep(0.5)
        pyautogui.press('p')
        time.sleep(3)
        pyautogui.press('enter')
        time.sleep(3)
         # Abre a janela de publicar
        
        # --- [INÍCIO DA MODIFICAÇÃO] ---
        # Agora vamos procurar pela imagem 'Meu_Workspace.png'
        
        # Carrega a imagem usando PIL
        try:
            imagem_para_procurar = Image.open(caminho_imagem_clicar)
            print(f"Imagem '{caminho_imagem_clicar}' carregada com sucesso.")
        except Exception as e:
            print(f"ERRO: Não foi possível carregar o arquivo de imagem: {caminho_imagem_clicar}")
            print(f"Erro detalhado: {e}")
            print("Verifique se o caminho está correto e se a imagem existe.")
            sys.exit(1)
            
        print(f"Procurando por: {caminho_imagem_clicar}")
        
        posicao_imagem = None
        tempo_limite_procura = 30
        tempo_inicio = time.time()
        
        while (time.time() - tempo_inicio) < tempo_limite_procura:
            try:
                posicao_imagem = pyautogui.locateCenterOnScreen(
                    imagem_para_procurar,
                    confidence=0.9
                )
                
                if posicao_imagem:
                    print(f"Imagem encontrada em: {posicao_imagem}")
                    break
                    
            except pyautogui.ImageNotFoundException:
                pass
            except Exception as img_e:
                print(f"Erro ao procurar imagem: {img_e}")
                break 
                
            time.sleep(1)
            print(f"Ainda procurando a imagem... {int(time.time() - tempo_inicio)}s")

        if posicao_imagem:
            pyautogui.click(posicao_imagem, duration=0.25)
            print("Imagem clicada.")
            
            # ATENÇÃO: Adicione aqui os próximos passos
            # Ex: Se houver OUTRA janela (de "Substituir"), 
            # você precisará de outro 'time.sleep' ou de outra
            # busca por imagem para clicar no botão "Substituir".
            
            print("Aguardando 15 segundos para a publicação...")
            time.sleep(10)
        
            # Tenta fechar a janela final de "Sucesso!"
            pyautogui.press('tab')
            time.sleep(1)
            pyautogui.press('enter')

            time.sleep(1)
            pyautogui.press('enter')


            
            print("Publicação (provavelmente) concluída.")

            time.sleep(20)
            os.system("taskkill /IM PBIDesktop.exe /F")

            print("Script finalizado.")
            
        else:
            print(f"ERRO: Não foi possível encontrar a imagem '{caminho_imagem_clicar}' na tela após {tempo_limite_procura} segundos.")
            print("O script será encerrado.")
            sys.exit(1)
            
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    atualizar_bi_por_atalho()