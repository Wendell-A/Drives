from playwright.sync_api import sync_playwright
import os
import io
import traceback
import pandas as pd
import msal
import requests
import xlrd
from dotenv import load_dotenv

# ================= CARREGAMENTO DE AMBIENTE =================
load_dotenv() 

TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
HOSTNAME = os.getenv("HOSTNAME") 

# Configurações do SharePoint (Graph API)
SITE_NAME_URL = "/sites/Transportes"
TARGET_FILENAME = "Relatório de NF Trafegus.xlsx"
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

def upload_para_sharepoint(conteudo_arquivo):
    print("\n🔐 Autenticando no Microsoft Graph...")
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(CLIENT_ID, client_credential=CLIENT_SECRET, authority=authority)
    
    token_response = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    
    if "access_token" in token_response:
        access_token = token_response['access_token']
        headers = {"Authorization": f"Bearer {access_token}"}

        # 1. Buscar ID do Site
        site_res = requests.get(f"{GRAPH_API_URL}/sites/{HOSTNAME}:{SITE_NAME_URL}", headers=headers)
        site_id = site_res.json().get("id")

        # 2. Buscar ID da Drive
        drive_res = requests.get(f"{GRAPH_API_URL}/sites/{site_id}/drive", headers=headers)
        drive_id = drive_res.json().get("id")

        # 3. Upload (Substituindo o arquivo)
        upload_url = f"{GRAPH_API_URL}/drives/{drive_id}/root:/{TARGET_FILENAME}:/content"
        upload_res = requests.put(upload_url, headers=headers, data=conteudo_arquivo)

        if upload_res.status_code in [200, 201]:
            print(f"✅ Upload concluído com sucesso no SharePoint: {TARGET_FILENAME}")
        else:
            print(f"❌ Erro no upload: {upload_res.text}")
    else:
        print("❌ Falha na obtenção do token MSAL.")

def processar_e_subir_trafegus():
    print("\n================ INÍCIO DO PROCESSO TRAFEGUS =================")

    with sync_playwright() as p:
        print("🚀 Iniciando Chrome...")
        browser = p.chromium.launch(channel="chrome", headless=False)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        try:
            # ================= FASE 1: Login =================
            print("🌐 Acessando Trafegus...")
            page.goto("https://refit.trafegus.com.br/trafegusweb/public/login", wait_until="load")
            page.wait_for_selector("input", timeout=15000)
            page.fill("input", "LEONARDO_INACIO")
            page.keyboard.press("Tab")
            page.keyboard.type("@LISILV@11")
            page.keyboard.press("Enter")
            page.wait_for_load_state("networkidle", timeout=30000)

            # ================= FASE 2: Navegação =================
            print("🖱️ Clicando em Logístico...")
            page.wait_for_selector("a[data-bs-toggle='dropdown']:has-text('Logístico')", timeout=15000)
            page.click("a[data-bs-toggle='dropdown']:has-text('Logístico')")

            print("🖱️ Abrindo Monitor Logístico...")
            page.wait_for_selector("#item_monitlogpadrao", timeout=15000)
            
            with context.expect_page() as new_page_info:
                page.click("#item_monitlogpadrao")
            
            monitor_page = new_page_info.value
            monitor_page.wait_for_load_state("load")
            monitor_page.wait_for_timeout(5000)

            # ================= FASE 3: Download =================
            seletor_excel = "#excel"
            alvo = monitor_page
            for frame in monitor_page.frames:
                if "monitor" in frame.url.lower() or frame.locator(seletor_excel).count() > 0:
                    alvo = frame
                    break
            
            with monitor_page.expect_download(timeout=60000) as download_info:
                alvo.wait_for_selector(seletor_excel, timeout=20000)
                alvo.click(seletor_excel)

            download = download_info.value
            path_temp = os.path.join(os.getcwd(), "temp_trafegus_export.xls")
            download.save_as(path_temp)
            print(f"📁 Arquivo baixado temporariamente.")

            # ================= FASE 4: Leitura (Sua Lógica Original) =================
            print("📊 Lendo e filtrando dados...")
            df_raw = None
            
            try:
                # TENTATIVA 1: XLRD com tratamento de corrupção
                book = xlrd.open_workbook(path_temp, ignore_workbook_corruption=True)
                df_raw = pd.read_excel(book, engine='xlrd', header=None)
            except Exception as e:
                print(f"ℹ️ Tentativa com xlrd falhou ({e}), tentando read_html...")
                try:
                    # TENTATIVA 2: READ_HTML
                    tabelas = pd.read_html(path_temp, encoding='latin-1')
                    df_raw = tabelas[0]
                except Exception as e2:
                    print(f"❌ Falha total na leitura: {e2}")

            if df_raw is not None:
                # Ajuste de Cabeçalho
                df_raw.columns = df_raw.iloc[1].astype(str).str.strip()
                df = df_raw.iloc[2:].reset_index(drop=True)

                colunas_alvo = ["Placa", "Descrição da Rota", "Última Posição", "Data Última Posição", "UF da Última Posição", "Motorista"]
                colunas_existentes = [c for c in colunas_alvo if c in df.columns]
                df_filtrado = df[colunas_existentes]

                if not df_filtrado.empty:
                    print(f"✅ Dados processados ({len(df_filtrado)} linhas).")
                    
                    # Converter para Excel em memória para upload
                    buffer = io.BytesIO()
                    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
                        df_filtrado.to_excel(writer, index=False, sheet_name='Dados')
                    
                    # ================= FASE 5: Upload =================
                    upload_para_sharepoint(buffer.getvalue())
                else:
                    print("⚠️ DataFrame vazio após filtragem.")
            
            # Limpa o arquivo temporário
            if os.path.exists(path_temp):
                os.remove(path_temp)

        except Exception:
            print(f"\n🔥 ERRO NO PROCESSO: {traceback.format_exc()}")
        finally:
            browser.close()

if __name__ == "__main__":
    processar_e_subir_trafegus()