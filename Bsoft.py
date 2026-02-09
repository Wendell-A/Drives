from playwright.sync_api import sync_playwright
import time
import pyautogui
import os
import traceback
import glob
import io 
from datetime import datetime, timedelta
import pandas as pd
import msal  # 🆕 Necessário (igual ao script Qive)
import requests # 🆕 Necessário (igual ao script Qive)

# ================= CARREGAMENTO DE AMBIENTE =================
from dotenv import load_dotenv
load_dotenv() 

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
HOSTNAME = os.getenv("HOSTNAME") 

# Configurações do SharePoint (Graph API)
SITE_NAME_URL = "/sites/Transportes" # Parte final da URL do site
TARGET_FILENAME = "Relatório de NF Bsoft.xlsx"
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

# ================= CONFIGURAÇÃO DO ROBÔ =================
pyautogui.FAILSAFE = True 
pyautogui.useImageNotFoundException(False)
CAMINHO_DOWNLOADS = os.path.join(os.path.expanduser("~"), "Downloads")
LARGURA_TELA, ALTURA_TELA = pyautogui.size()
REGION_TOPO = (0, 0, LARGURA_TELA, 200)

# ========================================================
# 🆕 FUNÇÕES DE UPLOAD (VINDAS DO SCRIPT QIVE)
# ========================================================

def get_access_token():
    """Gera o token usando as mesmas credenciais do script Qive."""
    if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET]):
        raise Exception("❌ ERRO: Variáveis do .env incompletas!")

    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        CLIENT_ID, authority=authority, client_credential=CLIENT_SECRET
    )
    scopes = ["https://graph.microsoft.com/.default"]
    result = app.acquire_token_for_client(scopes=scopes)
    
    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception(f"❌ Erro ao obter token: {result.get('error_description')}")

def upload_via_graph_api(df_final):
    """Sobe o arquivo usando Microsoft Graph API (Método moderno)."""
    print("☁️ Iniciando upload via Graph API...")
    
    try:
        token = get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        
        # 1. Buscar ID do Site
        print(f"   🔎 Buscando ID do site: {HOSTNAME}:{SITE_NAME_URL}")
        site_url_api = f"{GRAPH_API_URL}/sites/{HOSTNAME}:{SITE_NAME_URL}"
        site_resp = requests.get(site_url_api, headers=headers)
        
        if site_resp.status_code != 200:
            raise Exception(f"Erro ao achar site ({site_resp.status_code}): {site_resp.text}")
            
        site_id = site_resp.json()["id"]

        # 2. Buscar ID da Biblioteca 'Documentos' (Shared Documents)
        drives_url = f"{GRAPH_API_URL}/sites/{site_id}/drives"
        drives_resp = requests.get(drives_url, headers=headers)
        drives_resp.raise_for_status()
        
        drive_id = None
        for drive in drives_resp.json()["value"]:
            # O nome padrão da biblioteca "Documentos" no sistema é "Shared Documents" ou "Documentos"
            if drive["name"] == "Documentos" or drive["name"] == "Shared Documents": 
                drive_id = drive["id"]
                break
        
        if not drive_id:
            # Pega o drive padrão se não achar pelo nome
            drive_id = drives_resp.json()["value"][0]["id"]
            print("   ⚠️ Drive específico não achado, usando o Drive Padrão.")

        # 3. Preparar o Arquivo na Memória
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
            # Ajuste para garantir formatação de data no Excel se necessário, 
            # mas a conversão no dataframe já ajuda
            df_final.to_excel(writer, index=False)
        buffer.seek(0)
        file_content = buffer.read()

        # 4. Upload
        # Caminho: root (raiz) -> nome do arquivo
        upload_url = f"{GRAPH_API_URL}/sites/{site_id}/drives/{drive_id}/root:/{TARGET_FILENAME}:/content"
        
        headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        print(f"   🚀 Enviando arquivo: {TARGET_FILENAME}...")
        
        upload_resp = requests.put(upload_url, headers=headers, data=file_content)
        upload_resp.raise_for_status()
        
        print(f"✅ SUCESSO! Arquivo enviado. Link: {upload_resp.json().get('webUrl')}")

    except Exception as e:
        print(f"❌ FALHA NO UPLOAD: {e}")
        print(traceback.format_exc())

# ========================================================
# ROBÔ PRINCIPAL (Bsoft)
# ========================================================

def acessar_bsoft():
    print("\n================ INÍCIO DO ROBÔ (VERSÃO GRAPH API) =================\n")

    diretorio_atual = os.path.dirname(os.path.abspath(__file__))
    
    # 🆕 DEFININDO A SUBPASTA DE IMAGENS
    pasta_imagens = os.path.join(diretorio_atual, "imagens bsoft") 

    # Agora usamos 'pasta_imagens' em vez de 'diretorio_atual'
    img_login_remoto    = os.path.join(pasta_imagens, "login_remoto.png")
    img_bsoft_aberto    = os.path.join(pasta_imagens, "bsoft_aberto.png")
    img_cte_aberto      = os.path.join(pasta_imagens, "cte_aberto.png")
    img_relatorio_ok    = os.path.join(pasta_imagens, "relatorio_ok.png")
    img_diretorio       = os.path.join(pasta_imagens, "Diretorio.png")
    
    # --- 📍 DEFINIÇÃO FIXA DA POSIÇÃO (X, Y) ---
    memoria_posicao_seta = (946, 95)
    print(f"📍 Posição da seta fixada manualmente em: {memoria_posicao_seta}")

    print("🔍 Verificando imagens essenciais...")
    # Removi as imagens da seta da verificação, pois não são mais usadas
    imagens = [img_login_remoto, img_bsoft_aberto, img_cte_aberto, img_relatorio_ok, img_diretorio]
    for img in imagens:
        if not os.path.exists(img):
            print(f"❌ ERRO: Imagem faltante -> {img}")
            return
    print("✅ Imagens OK.\n")

    with sync_playwright() as p:
        print("🚀 Iniciando Chrome...")
        browser = p.chromium.launch(channel="chrome", headless=False, args=["--start-maximized"])
        context = browser.new_context(accept_downloads=True, no_viewport=True)
        page = context.new_page()

        print(f"⚙️ Configurando Chrome para salvar em: {CAMINHO_DOWNLOADS}")
        client = page.context.new_cdp_session(page)
        client.send("Page.setDownloadBehavior", {"behavior": "allow", "downloadPath": CAMINHO_DOWNLOADS})

        try:
            # ================= FASE 1: Login Site =================
            print("🌐 [Fase 1] Acessando Bsoft...")
            page.goto("https://sis.bsoft.com.br")
            page.wait_for_selector('input', timeout=15000)
            page.fill('input:visible', 'LLESS174')
            page.keyboard.press('Enter'); time.sleep(2)
            page.keyboard.type('bsoft2025')
            page.keyboard.press('Enter'); time.sleep(3)

            # ================= FASE 2: Acesso Remoto =================
            print("\n🖥️ [Fase 2] Buscando acesso remoto...")
            imagem_encontrada = None
            for i in range(60):
                # (Removido busca de seta aqui)
                imagem_encontrada = pyautogui.locateOnScreen(img_login_remoto, confidence=0.8, grayscale=True)
                if imagem_encontrada:
                    print(f"✅ Ícone encontrado ({i}s).")
                    break
                time.sleep(1)

            if not imagem_encontrada:
                print("❌ ERRO: Ícone remoto não apareceu.")
                return

            pyautogui.doubleClick(pyautogui.center(imagem_encontrada))
            time.sleep(5)
            print("🔑 Credenciais remotas...")
            pyautogui.write('felipe.queiroz'); pyautogui.press('tab')
            pyautogui.write('Felipe123!'); pyautogui.press('enter'); time.sleep(2)
            pyautogui.press('enter'); print("✅ Conectado."); time.sleep(8)

            # ================= FASE 3 a 7: Navegação =================
            print("📦 [Fase 3] Aguardando Sistema...")
            bsoft_carregado = False
            for i in range(120):
                if pyautogui.locateOnScreen(img_bsoft_aberto, confidence=0.7, grayscale=True):
                    bsoft_carregado = True; break
                # (Removido busca de seta aqui também)
                time.sleep(2)
            
            if not bsoft_carregado:
                print("❌ ERRO: Sistema não abriu."); return
            
            print("\n🧭 [Fase 4] Menu Alt+F...")
            time.sleep(3); pyautogui.hotkey('alt', 'f'); time.sleep(3)
            for _ in range(9): pyautogui.press('down'); time.sleep(1)
            pyautogui.press('right'); time.sleep(0.5)
            pyautogui.press('down'); time.sleep(0.5); pyautogui.press('down'); time.sleep(0.5); pyautogui.press('enter')

            print("⏳ Abrindo CTe...")
            for i in range(30):
                if pyautogui.locateOnScreen(img_cte_aberto, confidence=0.8): break
                time.sleep(1)
            else: return



            print("\n📅 [Fase 5] Configurando Data...")

            hoje = datetime.now()
            dia_da_semana = hoje.weekday()  # 0 = Segunda, 1 = Terça, ..., 6 = Domingo

            if dia_da_semana == 0:
                # Se for segunda (0), pega a data de anteontem (hoje - 2 dias)
                data_para_bsoft = (hoje - timedelta(days=2)).strftime("%d%m%Y")
                print(f"🗓️ Hoje é segunda-feira. Buscando dados desde sábado: {data_para_bsoft}")
            else:
                # Caso contrário, mantém a lógica de ontem (hoje - 1 dia)
                data_para_bsoft = (hoje - timedelta(days=1)).strftime("%d%m%Y")
                print(f"🗓️ Buscando dados de ontem: {data_para_bsoft}")

            # Digita a data calculada no sistema
            pyautogui.write(data_para_bsoft)
            time.sleep(2)
            

            print("\n📊 [Fase 6] Gerar Relatório...")
            pyautogui.hotkey('alt', 'f')
            for i in range(120):
                if pyautogui.locateOnScreen(img_relatorio_ok, confidence=0.8): break
                time.sleep(1)
            else: return

            print("\n💾 [Fase 7] Menu Exportar...")
            pyautogui.hotkey('alt', 'x'); time.sleep(1.5)
            pyautogui.press('down'); time.sleep(0.5); pyautogui.press('down'); time.sleep(0.5); pyautogui.press('enter'); time.sleep(2.5)
            for _ in range(5): pyautogui.press('tab'); time.sleep(0.5)
            time.sleep(0.5); pyautogui.press('down'); time.sleep(1.5); pyautogui.press('enter')
            for _ in range(4): pyautogui.press('tab'); time.sleep(0.5)
            time.sleep(1.5); pyautogui.press('enter')

           # ================= FASE 8: BAIXAR =================
            print("\n👆 [Fase 8] Clicar em Abrir/Download...")
            print(f"🎯 Usando posição FIXA da seta: {memoria_posicao_seta}")

            # 1. Clica na posição fixa da seta
            pyautogui.click(memoria_posicao_seta); time.sleep(1.5)
            
            # 2. Calcula o botão de download relativo à posição fixa
            novo_x = memoria_posicao_seta[0] + 35
            novo_y = memoria_posicao_seta[1] + 11
            
            print(f"🔽 Clicando no download em: {novo_x}, {novo_y}")
            pyautogui.click(x=novo_x, y=novo_y, duration=0.5); time.sleep(5) 

            print("⌨️ Comandos Finais..."); pyautogui.write('exp'); time.sleep(0.8)
            pyautogui.press('down'); time.sleep(0.5); pyautogui.press('enter'); time.sleep(0.5); pyautogui.press('enter')
            
            # ================= FASE 9: AGUARDAR ARQUIVO =================
            print("\n⏳ [Fase 9] Esperando 20s...")
            time.sleep(10) 
            print(f"🔎 Procurando em: {CAMINHO_DOWNLOADS}")
            lista_arquivos = glob.glob(os.path.join(CAMINHO_DOWNLOADS, '*')) 
            lista_arquivos = [f for f in lista_arquivos if os.path.isfile(f)]
            
            if not lista_arquivos:
                print("❌ ERRO: Downloads vazia."); return

            arquivo_recente = max(lista_arquivos, key=os.path.getmtime)
            print(f"✅ Arquivo encontrado: {os.path.basename(arquivo_recente)}")

            # ================= FASE 10: PROCESSAMENTO E UPLOAD =================
            print("\n🐼 [Fase 10] Processando dados...")
            
            try:
                df = None
                if arquivo_recente.lower().endswith(('.htm', '.html')):
                    try:
                        tabelas = pd.read_html(arquivo_recente, decimal=',', thousands='.')
                        if tabelas: df = tabelas[0]
                    except: print("❌ ERRO: Bibliotecas HTML ausentes."); return
                else:
                    df = pd.read_excel(arquivo_recente)

                if df is not None:
                    mapa_colunas = {
                        'Notas Fiscais': 'Número',
                        'Data de Emissão da NF': 'Data Emissão',
                        'Naturezas das Notas Fiscais': '[Item] Descrição',
                        'Local de Entrega': "Local de entrega",
                        'Soma dos Volumes': '[Item] Quantidade',
                        'Remetente - Nome': 'Remetente - Nome',
                        'Veículo - Placa': 'Placa1',
                        'Placa do Vinculado 1': 'Placa2',
                        'Placa do Vinculado 2': 'Placa3',
                        'Hora de Emissão': 'horario de carregamento',
                        'Consignatário - Nome': 'Deposito',
                        'Remetente - Nome': 'Nome PJ Emitente',
                        'Motorista - Nome': 'Motorista'
                    }
                    colunas_presentes = [col for col in mapa_colunas.keys() if col in df.columns]
                    if not colunas_presentes: print("❌ ERRO: Colunas esperadas não encontradas."); return

                    df_final = df[colunas_presentes].copy()
                    df_final = df_final.rename(columns=mapa_colunas)

                    # 🆕 ADICIONANDO COLUNA COM A DATA E HORA DE EXECUÇÃO
                    df_final["Data Execução"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

                    # ==========================================================
                    # 🛠️ AJUSTE SOLICITADO 1: NOTA FISCAL (Substituir , por /)
                    # ==========================================================
                    if 'Número' in df_final.columns:
                        # Converte para string e substitui vírgula por barra
                        df_final['Número'] = df_final['Número'].astype(str).str.replace(',', '/', regex=False)

                    # ==========================================================
                    # 🛠️ AJUSTE SOLICITADO 2: DATA EMISSÃO (Remover Hora)
                    # ==========================================================
                    if 'Data Emissão' in df_final.columns:
                        # Converte para datetime (dayfirst=True garante DD/MM/AAAA)
                        # .dt.date extrai apenas a parte da data
                        df_final['Data Emissão'] = pd.to_datetime(df_final['Data Emissão'], dayfirst=True, errors='coerce').dt.date

                    # ==========================================================
                    # 🛠️ AJUSTE: HORÁRIO DE CARREGAMENTO (somente HH:MM)
                    # ==========================================================
                    if 'horario de carregamento' in df_final.columns:
                        df_final['horario de carregamento'] = (
                            df_final['horario de carregamento']
                            .astype(str)
                            .str.strip()
                            .str[:5]
                        )

                    if "Local de entrega" in df_final.columns:
                        try:
                            df_final["Local de entrega"] = df_final["Local de entrega"].astype(str)
                            split_data = df_final["Local de entrega"].str.split('/', n=1, expand=True)
                            df_final['Cidade'] = split_data[0].str.strip()
                            if split_data.shape[1] > 1: df_final['UF'] = split_data[1].str.strip()
                            else: df_final['UF'] = ""
                        except Exception as e: print(f"⚠️ Erro ao separar Cidade/UF: {e}")

                    col_qtd = '[Item] Quantidade'
                    if col_qtd in df_final.columns:
                        df_final[col_qtd] = pd.to_numeric(df_final[col_qtd], errors='coerce')
                        df_final = df_final[df_final[col_qtd] > 40000]

                    coluna_alvo_prod = '[Item] Descrição'
                    if coluna_alvo_prod in df_final.columns:
                        df_final[coluna_alvo_prod] = df_final[coluna_alvo_prod].astype(str).str.strip()
                        regras = [
                            ('Gasolina C', 'Gasolina C'), ('Gasolina A', 'Gasolina A'), ('Anidro', 'Anidro'), 
                            ('Hidrat', 'Hidratado'), ('Biodiesel', 'Biodiesel'), ('A S10', 'Diesel A S10'),
                            ('A S500', 'Diesel A S500'), ('B S10', 'Diesel B S10'), ('B S500', 'Diesel B S500')
                        ]
                        for termo, valor_final in regras:
                            mask = df_final[coluna_alvo_prod].str.contains(termo, case=False, na=False)
                            df_final.loc[mask, coluna_alvo_prod] = valor_final
                        
                    # ================= 🆕 UPLOAD SHAREPOINT (NOVO MÉTODO) =================
                    upload_via_graph_api(df_final)
                    
                    try:
                        os.remove(arquivo_recente)
                        print("🧹 Arquivo temporário local excluído.")
                    except: pass
                else: print("❌ Falha na leitura.")

            except Exception as e:
                print(f"❌ Erro no Pandas: {e}")
                print(traceback.format_exc())

            print("\n🎉 FIM DO PROCESSO 🎉")

        except Exception:
            print("\n🔥 ERRO CRÍTICO GERAL 🔥")
            print(traceback.format_exc())
        finally:
            time.sleep(5)
            browser.close()

if __name__ == "__main__":
    acessar_bsoft()