from playwright.sync_api import sync_playwright
import time
import os
import pandas as pd
import numpy as np
import re
import unicodedata
from typing import List, Optional, Dict, Any, Set
import io 
import msal 
import requests 
from dotenv import load_dotenv 
from datetime import datetime

# --- Carrega as variáveis de ambiente ---
load_dotenv()

TENANT_ID: str = os.getenv("TENANT_ID")
CLIENT_ID: str = os.getenv("CLIENT_ID")
CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
HOSTNAME: str = os.getenv("HOSTNAME")
GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

def safe_filename(name: str) -> str:
    return re.sub(r'[\\/:"*?<>|]+', "", str(name)).strip()

def _remove_accents_and_normalize(text: str) -> str:
    if pd.isna(text):
        return ""
    s = str(text)
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s.lower()

def _limpar_cnpj_texto(cnpj_series: pd.Series) -> pd.Series:
    """Helper para normalizar CNPJ/CPF como texto limpo com 14 digitos."""
    if cnpj_series.empty:
        return cnpj_series
    
    # Remove tudo que não é número
    limpo = cnpj_series.astype(str).str.strip().str.replace(r'[^\d]', '', regex=True)
    
    # Garante 14 caracteres (recupera zero à esquerda perdido pelo Excel)
    return limpo.str.zfill(14)

def carregar_lista_exclusao(caminho_arquivo: str) -> Set[str]:
    """Carrega lista de exclusão (Blacklist)."""
    if not os.path.exists(caminho_arquivo):
        print(f"ℹ️ Arquivo de exclusão não encontrado em {caminho_arquivo}. Nenhuma exclusão será aplicada.")
        return set()
    try:
        df_excluir = pd.read_excel(caminho_arquivo, dtype=str)
        col_cnpj = "CNPJ" if "CNPJ" in df_excluir.columns else "CNPJ Emitente"
        col_prod = "Produto" if "Produto" in df_excluir.columns else "[Item] Descrição"

        if col_cnpj not in df_excluir.columns or col_prod not in df_excluir.columns:
            print(f"⚠️ Arquivo de exclusão não contém colunas esperadas ('CNPJ' e 'Produto').")
            return set()
        
        print(f"🔄 Carregando arquivo de exclusão '{os.path.basename(caminho_arquivo)}'...")
        df_excluir["CNPJ_Limpo"] = _limpar_cnpj_texto(df_excluir[col_cnpj])
        df_excluir["Produto_Limpo"] = df_excluir[col_prod].astype(str).str.strip()
        
        chaves_exclusao = df_excluir["CNPJ_Limpo"] + "|" + df_excluir["Produto_Limpo"]
        chaves_unicas = set(chaves_exclusao.dropna())
        print(f"✅ Carregadas {len(chaves_unicas)} chaves de exclusão.")
        return chaves_unicas
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo de exclusão: {e}")
        return set()

def aplicar_de_para_descricao(series_descricao: pd.Series) -> pd.Series:
    """
    Aplica mapeamento de combustíveis.
    ATENÇÃO: Retorna NaN (Vazio) para tudo que NÃO for mapeado.
    """
    original = series_descricao.fillna("").astype(str).str.strip()
    norm = original.apply(_remove_accents_and_normalize)
    mask_aditivo = norm.str.contains(r"\baditivo\b", na=False)

    patterns = [
        # --- REGRAS ESPECÍFICAS (MINÚSCULAS) ---
        (r"diesel\s+a\s+s-?10", "Diesel A S10"), 
        (r"diesel\s+a\s+s-?500", "Diesel A S500"),
        (r"gasolina\s*[- ]?\s*c\b", "Gasolina C"),

        # --- REGRAS GENÉRICAS ---
        (r"gasolina a", "Gasolina A"),
        (r"\bb\s*-?\s*s-?10\b", "Diesel B S10"),
        (r"\bb\s*-?\s*s-?500\b", "Diesel B S500"),
        (r"(?:diesel|a)\s*-?\s*s-?10\b", "Diesel A S10"),
        (r"(?:diesel|a)\s*-?\s*s-?500\b", "Diesel A S500"),
        
        # Outros
        (r"\bb100\b", "Biodiesel"),
        (r"etilico", "Hidratado"), 
        (r"mgo", "Mgo"),
        (r"maritimo", "Mgo"),
        (r"biodiesel", "Biodiesel"),
        (r"anidro", "Anidro"),
        (r"hidratado", "Hidratado"),
    ]

    resultado = pd.Series(data=[np.nan] * len(series_descricao), index=series_descricao.index, dtype=object)

    for pat, val in patterns:
        mask_padrao = norm.str.contains(pat, na=False)
        mask_aplicar = mask_padrao & ~mask_aditivo
        resultado.loc[mask_aplicar] = val

    resultado = resultado.astype(object)
    return resultado

def extrair_placas_motorista(texto: Optional[str]) -> List[Optional[str]]:
    placas: List[Optional[str]] = [None, None, None]
    motorista: Optional[str] = None
    lista_placas_encontradas = []

    if isinstance(texto, str):

        # Normaliza
        texto_busca = texto

        # REGEX PARA PLACA (antiga + Mercosul)
        rgx_validacao_placa = r'([A-Z]{3}\s*-?\s*(?:\d{4}|\d[A-Z]\d{2}))'

        # -----------------------------
        # NOVA REGRA: tratar PLACA colada
        # -----------------------------
        # Ex: PlacaDVS9285PKA3D63PKA9J21

        padrao_placa_colada = r'(Placas?|veiculo?|veículo?)([A-Z0-9]{7,30})'
        coladas = re.findall(padrao_placa_colada, texto_busca, flags=re.IGNORECASE)

        for _, bloco in coladas:
            placas_encontradas_bloco = re.findall(rgx_validacao_placa, bloco, flags=re.IGNORECASE)
            for p in placas_encontradas_bloco:
                p_limpa = p.replace(" ", "").replace("-", "").upper()
                if p_limpa not in lista_placas_encontradas:
                    lista_placas_encontradas.append(p_limpa)

        # -----------------------------
        # REGRA ORIGINAL (janela após "Placa")
        # -----------------------------

        matches_palavra = re.finditer(r'\bPlacas?\b', texto_busca, re.IGNORECASE)

        for match in matches_palavra:
            inicio_janela = match.end()
            fim_janela = min(inicio_janela + 35, len(texto_busca))

            trecho_focado = texto_busca[inicio_janela:fim_janela]

            placas_no_trecho = re.findall(rgx_validacao_placa, trecho_focado, re.IGNORECASE)

            for p in placas_no_trecho:
                p_limpa = p.replace(" ", "").replace("-", "").upper()
                if p_limpa not in lista_placas_encontradas:
                    lista_placas_encontradas.append(p_limpa)

        # Aplica no retorno: Placa1, Placa2, Placa3
        for i in range(min(3, len(lista_placas_encontradas))):
            placas[i] = lista_placas_encontradas[i]

        # ----------------------------------------------------
        # (restante da função — parte do MOTORISTA permanece)
        # ----------------------------------------------------

        padroes_motorista = [
            r'N\. da OC / Motorista / Placa:\s*\d+\s+([a-zà-ÿ\s]+?)\s+[A-Z]{3}-?\d[A-Z]\d{2,3}',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*:\s*([a-zà-ÿ\s]+?)\s*-\s*(?:Placa[\w\s\d]*|CPF|CNH|DADOS DO LAUDO)',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*[:\-]?\s*(?:\d+\s+)?([a-zà-ÿ\s]+?)\s+(?:CPF:|CNH:)',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*[:\-]?\s*(?:\d+\s+)?([a-zà-ÿ\s]+?)\s+(?:Densidade:|Temperatura:)',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*[:\-]?\s*(?:\d+\s*)?([A-ZÀ-Ÿ\s]{5,40})(?:$|\n|\r| - )'
        ]

        for padrao in padroes_motorista:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                nome_potencial = match.group(1).strip().strip('-').strip()
                if len(nome_potencial) > 4 and "DECLARO" not in nome_potencial.upper():
                    motorista = nome_potencial
                    break

    return placas + [motorista]


def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> Optional[str]:
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = msal.ConfidentialClientApplication(
        client_id, authority=authority, client_credential=client_secret
    )
    scopes = ["https://graph.microsoft.com/.default"]
    result = app.acquire_token_for_client(scopes=scopes)
    
    if "access_token" in result:
        return result["access_token"]
    else:
        print(f"❌ Erro ao obter token: {result.get('error_description')}")
        return None

def upload_df_to_sharepoint(df: pd.DataFrame, tenant_id: str, client_id: str, client_secret: str, hostname: str, site_name: str, library_name: str, file_name: str, folder_path: str = ""):
    access_token = get_access_token(tenant_id, client_id, client_secret)
    if not access_token:
        raise Exception("Falha ao obter token de acesso.")
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        print(f"Buscando Site ID para '{site_name}'...")
        site_url = f"{GRAPH_API_URL}/sites/{hostname}:{site_name}"
        site_resp = requests.get(site_url, headers=headers)
        site_resp.raise_for_status()
        site_id = site_resp.json()["id"]

        print(f"Buscando Drive ID para biblioteca '{library_name}'...")
        drives_url = f"{GRAPH_API_URL}/sites/{site_id}/drives"
        drives_resp = requests.get(drives_url, headers=headers)
        drives_resp.raise_for_status()
        
        drive_id = None
        for drive in drives_resp.json()["value"]:
            if drive["name"].lower() == library_name.lower():
                drive_id = drive["id"]
                break
        
        if not drive_id:
            raise Exception(f"Biblioteca '{library_name}' não encontrada no site.")

        print(f"Convertendo DataFrame '{file_name}' para Excel em memória...")
        output = io.BytesIO()
        df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        file_content = output.read()

        if folder_path:
            upload_url = f"{GRAPH_API_URL}/sites/{site_id}/drives/{drive_id}/root:/{folder_path}/{file_name}:/content"
        else:
            upload_url = f"{GRAPH_API_URL}/sites/{site_id}/drives/{drive_id}/root/children/{file_name}/content"

        print(f"Fazendo upload de '{file_name}' para SharePoint...")
        headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        upload_resp = requests.put(upload_url, headers=headers, data=file_content)
        upload_resp.raise_for_status()
        
        print(f"✅ Upload de '{file_name}' concluído com sucesso!")
        print(f"   Link: {upload_resp.json().get('webUrl')}")

    except requests.exceptions.HTTPError as e:
        print(f"❌ ERRO DE API no upload de {file_name}: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"❌ Erro inesperado no upload de {file_name}: {e}")

# ==============================================================================
# FUNÇÃO PRINCIPAL
# ==============================================================================

def test():
    home_dir = os.path.expanduser("~")
    download_dir = os.path.join(home_dir, "Documentos", "Qive") 
    os.makedirs(download_dir, exist_ok=True)

    # --- Caminhos dos Arquivos de Configuração ---
    pasta_empresas = os.path.join(home_dir, "Documentos", "Sieg", "empresas")
    arquivo_whitelist = os.path.join(pasta_empresas, "CNPJ.xlsx")
    arquivo_blacklist = os.path.join(pasta_empresas, "CNPJ NAO ELETIVOS.xlsx")

    # --- Carregar Listas de Segurança ANTES de tudo ---
    set_cnpjs_validos = set()
    if os.path.exists(arquivo_whitelist):
        try:
            print("🛡️ Carregando Whitelist de CNPJs (Destinatários permitidos)...")
            df_white = pd.read_excel(arquivo_whitelist, dtype=str)
            col_white = "CNPJ Destinatário" if "CNPJ Destinatário" in df_white.columns else df_white.columns[0]
            # Helper com zfill(14)
            set_cnpjs_validos = set(_limpar_cnpj_texto(df_white[col_white]))
            print(f"✅ Whitelist carregada: {len(set_cnpjs_validos)} CNPJs autorizados.")
        except Exception as e:
            print(f"⚠️ Erro ao ler Whitelist: {e}")
    else:
        print("⚠️ Whitelist (CNPJ.xlsx) não encontrada. O filtro de destinatário será ignorado.")

    # Carrega Blacklist
    chaves_para_excluir = carregar_lista_exclusao(arquivo_blacklist)

    print(f"📂 Diretório de download: {download_dir}")
    
    # ---------------------------------------------------------
    # PARTE 1: EXTRAÇÃO VIA PLAYWRIGHT
    # ---------------------------------------------------------
    
    print("🚀 Iniciando navegador...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_viewport_size({"width": 1366, "height": 768})

        try:
            print("🌐 Acessando login...")
            page.goto("https://app.arquivei.com.br/Plus/search-nfe")
            page.wait_for_selector('input[type="email"]', timeout=15000)
            page.fill('input[type="email"]', "planejamento@logisticafit.com.br")
            time.sleep(1)
            page.fill('input[type="password"]', "Refit@2025")
            time.sleep(1)
            page.keyboard.press("Enter")
            
            page.wait_for_selector("#select-role", timeout=30000)
            time.sleep(5) 
            print("✅ Login realizado!")

            print("📦 Selecionando 'Items'...")
            if page.is_visible("#select-detail"):
                page.click("#select-detail")
                time.sleep(1)
                try:
                    page.locator("div[class*='option']").filter(has_text=re.compile("Itens|Items", re.I)).first.click()
                except:
                    page.keyboard.type("Itens")
                    page.keyboard.press("Enter")
                time.sleep(2)
            
            print("🔘 Clicando 'Todos os filtros'...")
            try:
                page.wait_for_selector("#label-all-filters", state="visible", timeout=10000)
                page.click("#label-all-filters")
            except:
                page.locator("text=Todos os filtros").click()
            time.sleep(2)

            print("📅 Selecionando 'Mês Atual'...")
            if page.is_visible("#emission-period"):
                page.click("#emission-period")
                time.sleep(1)
                try:
                    page.locator("div[class*='option']").filter(
                        has_text=re.compile("Mês atual|Este mês|Current Month", re.I)
                    ).first.click()
                except:
                    page.keyboard.type("Mês atual")
                    page.keyboard.press("Enter")
            time.sleep(2)

            print("⬇️ Iniciando exportação...")
            if page.is_visible("#export-report-before-view"):
                page.click("#export-report-before-view")
                time.sleep(1)
                page.locator("a").filter(has_text="Exportar Para Excel").first.click()
            else:
                page.locator("button").filter(has_text="Exportar").first.click()

            print("⏳ Aguardando download...")
            page.wait_for_selector("text=Relatórios Gerados", timeout=20000)
            
            botao_download = "tbody tr:first-child button:has-text('Download')"
            page.wait_for_selector(botao_download, state="visible", timeout=150000)
            
            with page.expect_download(timeout=60000) as download_info:
                page.click(botao_download)
                
            download = download_info.value
            caminho_final = os.path.join(download_dir, f"Relatorio_Qive_{int(time.time())}.xlsx")
            download.save_as(caminho_final)
            print(f"\n🎉 Arquivo baixado: {caminho_final}")

        except Exception as e:
            print(f"\n❌ Erro na extração: {e}")
            return 

        browser.close()

    # ---------------------------------------------------------
    # PARTE 2: PROCESSAMENTO
    # ---------------------------------------------------------
    
    print("\n📊 --- INICIANDO PROCESSAMENTO E DIAGNÓSTICO ---")

    arquivos = [f for f in os.listdir(download_dir) if f.endswith(".xlsx") and "Consolidado" not in f and "DEBUG" not in f]
    
    if not arquivos:
        print("⚠️ Nenhum arquivo .xlsx novo encontrado!")
        return

    arquivos.sort(key=lambda x: os.path.getmtime(os.path.join(download_dir, x)), reverse=True)
    arquivo_recente = arquivos[0]
    caminho = os.path.join(download_dir, arquivo_recente)
    
    print(f"📥 Processando arquivo: {arquivo_recente}")
    
    try:
        xls = pd.ExcelFile(caminho)
        abas = xls.sheet_names
        print(f"📑 Abas encontradas: {abas}")
        
        aba_alvo = abas[0]
        for aba in abas:
            if "relat" in aba.lower() or "dados" in aba.lower() or "nfe" in aba.lower():
                aba_alvo = aba
                print(f"✅ Aba alvo detectada pelo nome: '{aba_alvo}'")
                break
        
        if aba_alvo == abas[0] and len(abas) > 1:
            print(f"⚠️ Nome 'Relatório' não achado. Assumindo que a 2ª aba é a correta.")
            aba_alvo = abas[1]
            print(f"👉 Usando aba: '{aba_alvo}'")

        df = pd.read_excel(xls, sheet_name=aba_alvo)
        
    except Exception as e:
        print(f"❌ Erro crítico ao abrir o Excel: {e}")
        return

    qtd_inicial = len(df)
    print(f"🔢 Linhas lidas da aba '{aba_alvo}': {qtd_inicial}")
    
    if qtd_inicial == 0:
        print("❌ A ABA LIDA ESTÁ VAZIA!")
        return

    # --- Renomeações e Padronizações ---
    for col in df.columns:
        c_low = col.lower()
        if "natureza" in c_low and "op" in c_low:
            df.rename(columns={col: "Natureza Operação"}, inplace=True)
        elif "autoriza" in c_low and "data" in c_low:
            df.rename(columns={col: "Data Autorização"}, inplace=True)

    # --- Mapeamento de Colunas com Transportadora ---
    mapa_colunas = {
        "CNPJ_CPF_Emit": "CNPJ Emitente",
        "Rz_Emit": "Nome PJ Emitente",
        "UF_Emit": "UF Origem",
        "Produto": "[Item] Descrição",
        "Numero": "Número",
        "Quantidade": "[Item] Quantidade",
        "Dt_Emissao": "Data Emissão",
        "CNPJ_CPF_Dest": "CNPJ Destinatário",
        "Rz_Dest": "Destinatário",
        "UF_Dest": "UF Destino",
        "Valor_Total_Nota": "[Item] Valor Total Bruto",
        "Valor_Unitario": "[Item] Valor Unitário",
        "Chave": "Chave de Acesso",
        "Status": "Status",
        "Info_Adic": "Dados Adicionais",
        "Rz_Transp": "Razão social do transportador",
        "CNPJ_CPF_Transp": "CNPJ do transportador",
        "Transportadora": "Razão social do transportador",
        "CNPJ Transportadora": "CNPJ do transportador"
    }
    df.rename(columns=mapa_colunas, inplace=True)

    # =========================================================
    # 🆕 CONVERSÃO DE CUBAGEM (M3 -> LITROS)
    # Regra: Se [Item] Valor Unitário > 1000, divide unitário por 1000 e multiplica quantidade por 1000
    # =========================================================
    if "[Item] Valor Unitário" in df.columns and "[Item] Quantidade" in df.columns:
        print("💧 Verificando volumetria (Conversão M3 -> Litros pela regra do Valor Unitário > 1000)...")
        
        # Garante que as colunas sejam numéricas (trata erros como NaN)
        df["[Item] Valor Unitário"] = pd.to_numeric(df["[Item] Valor Unitário"], errors='coerce')
        df["[Item] Quantidade"] = pd.to_numeric(df["[Item] Quantidade"], errors='coerce')
        
        # Cria máscara para identificar onde o valor unitário indica cubagem
        mask_m3 = df["[Item] Valor Unitário"] > 1000
        
        qtd_convertida = mask_m3.sum()
        
        if qtd_convertida > 0:
            print(f"   🔄 Convertendo {qtd_convertida} linhas detectadas em M3...")
            
            # Aplica a conversão somente nas linhas da máscara
            df.loc[mask_m3, "[Item] Valor Unitário"] = df.loc[mask_m3, "[Item] Valor Unitário"] / 1000
            df.loc[mask_m3, "[Item] Quantidade"] = df.loc[mask_m3, "[Item] Quantidade"] * 1000
            
    # =========================================================

    # =========================================================
    # 🛡️ ÁREA DE FILTRAGEM
    # =========================================================

    # 1. Filtro Whitelist (CNPJ Destinatário)
    if "CNPJ Destinatário" in df.columns and set_cnpjs_validos:
        print("🛡️ Aplicando filtro de CNPJ Destinatário (Whitelist)...")
        # Garante a formatação correta antes de filtrar
        df["_CNPJ_Dest_Clean"] = _limpar_cnpj_texto(df["CNPJ Destinatário"])
        
        antes_white = len(df)
        df = df[df["_CNPJ_Dest_Clean"].isin(set_cnpjs_validos)]
        df.drop(columns=["_CNPJ_Dest_Clean"], inplace=True)
        print(f"   📉 Removidas {antes_white - len(df)} linhas de filiais não autorizadas.")

    # 2. Filtro Postos (Emitente)
    if "Nome PJ Emitente" in df.columns:
        print("🛡️ Filtrando emissores 'Posto'...")
        antes_posto = len(df)
        mask_posto = df["Nome PJ Emitente"].astype(str).str.contains(r"Posto\s|Postos\s", case=False, na=False, regex=True)
        df = df[~mask_posto]
        print(f"   📉 Removidas {antes_posto - len(df)} linhas de Postos.")

    # 3. Limpeza de CNPJ Emitente vazio
    if "CNPJ Emitente" in df.columns:
        antes_cnpj = len(df)
        df = df.dropna(subset=['CNPJ Emitente'])
        print(f"📉 Filtro CNPJ Nulo: removeu {antes_cnpj - len(df)} linhas.")

    # 4. Filtro de Produtos (AGORA ATIVO NOVAMENTE!)
    if "[Item] Descrição" in df.columns:
        print("🛡️ Filtrando Produtos (Somente combustíveis de interesse)...")
        # Aplica o De/Para. O que não casar vira NaN.
        df["[Item] Descrição"] = aplicar_de_para_descricao(df["[Item] Descrição"])
        
        antes_prod = len(df)
        # Remove as linhas que viraram NaN
        df = df.dropna(subset=["[Item] Descrição"])
        print(f"📉 Filtro Produto: removeu {antes_prod - len(df)} linhas não mapeadas.")

    # 5. Filtro Blacklist
    if chaves_para_excluir and "CNPJ Emitente" in df.columns and "[Item] Descrição" in df.columns:
        print("🛡️ Aplicando filtro de Exclusão Específica (Blacklist)...")
        df["_CNPJ_Emit_Clean"] = _limpar_cnpj_texto(df["CNPJ Emitente"])
        df["_Produto_Str"] = df["[Item] Descrição"].astype(str)
        df["_Chave_Check"] = df["_CNPJ_Emit_Clean"] + "|" + df["_Produto_Str"]
        
        mask_excluir = df["_Chave_Check"].isin(chaves_para_excluir)
        
        if mask_excluir.any():
            print(f"   ℹ️ Excluindo {mask_excluir.sum()} registros presentes na Blacklist.")
            
        df = df[~mask_excluir]
        df.drop(columns=["_CNPJ_Emit_Clean", "_Produto_Str", "_Chave_Check"], inplace=True)

    # =========================================================

    # --- EXTRAÇÃO DE HORÁRIO ---
    if "Data Autorização" in df.columns:
        df["Data Autorização"] = pd.to_datetime(df["Data Autorização"], dayfirst=True, errors='coerce')
        df["horario de carregamento"] = df["Data Autorização"].dt.strftime('%H:%M:%S')
    else:
        df["horario de carregamento"] = ""

    # --- PLACAS ---
    if "Dados Adicionais" in df.columns:
        novas = df['Dados Adicionais'].apply(extrair_placas_motorista).apply(pd.Series)
        novas.columns = ['Placa1', 'Placa2', 'Placa3', 'Motorista']
        df = pd.concat([df, novas], axis=1)

    df["Data Atualização"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    # --- SELEÇÃO FINAL ---
    colunas_finais = [
        "CNPJ Emitente", "Nome PJ Emitente", "UF Origem", 
        "[Item] Descrição", "Número", "[Item] Quantidade", 
        "Data Emissão", "CNPJ Destinatário", "Destinatário", 
        "UF Destino", "[Item] Valor Total Bruto", "[Item] Valor Unitário", 
        "Chave de Acesso", "Status", 
        "Natureza Operação", "Data Autorização", "horario de carregamento",
        "Dados Adicionais", "Placa1", "Placa2", "Placa3", "Motorista",
        "Razão social do transportador", "CNPJ do transportador",
        "Data Atualização"
    ]

    colunas_presentes = [c for c in colunas_finais if c in df.columns]
    df_final = df[colunas_presentes]

    # --- RESULTADO FINAL ---
    qtd_final = len(df_final)
    print(f"\n🏁 CONTAGEM FINAL: {qtd_final} linhas prontas.")

    if qtd_final == 0:
        print("❌❌ ERRO: Arquivo final vazio!")
        df.to_excel(os.path.join(download_dir, "DEBUG_ERRO_VAZIO.xlsx"), index=False)
    else:
        final_path = os.path.join(download_dir, "Consolidado_Qive.xlsx")
        df_final.to_excel(final_path, index=False)
        print(f"✅ Salvo em: {final_path}")

        print("\n☁️ Enviando ARQUIVO PRINCIPAL para SharePoint...")
        try:
            if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET, HOSTNAME]):
                print("⚠️ .env incompleto.")
            else:
                upload_df_to_sharepoint(
                    df=df_final,
                    tenant_id=TENANT_ID,
                    client_id=CLIENT_ID,
                    client_secret=CLIENT_SECRET,
                    hostname=HOSTNAME,
                    site_name="/sites/Transportes",
                    library_name="Documentos",
                    file_name="Relatório de NF Qive.xlsx",
                    folder_path=""
                )
                
                # =================================================================
                # 🆕 LÓGICA DE CARREGAMENTOS CIF (CORREÇÃO NOTAÇÃO CIENTÍFICA)
                # =================================================================
                print("\n🚚 Gerando relatório específico: CARREGAMENTOS CIF...")
                
                col_cnpj_transp = "CNPJ do transportador"
                
                if col_cnpj_transp in df_final.columns:
                    # 1. Cria cópia
                    df_cif = df_final.copy()
                    
                    # -----------------------------------------------------------
                    # CORREÇÃO CRÍTICA AQUI:
                    # Usa uma função lambda para formatar floats expandidos (sem 'E+13')
                    # Se for float/int, formata como '%.0f' (inteiro sem decimal).
                    # Se for texto, mantém texto.
                    # -----------------------------------------------------------
                    df_cif['__cnpj_temp'] = df_cif[col_cnpj_transp].apply(
                        lambda x: '{:.0f}'.format(x) if isinstance(x, (float, int)) and not pd.isna(x) else str(x)
                    )
                    
                    # Agora aplica a limpeza de caracteres não numéricos e o zfill
                    df_cif['__cnpj_temp'] = (
                        df_cif['__cnpj_temp']
                        .str.replace(r'\D', '', regex=True) # Remove pontos, traços, espaços
                        .str.zfill(14)                      # Garante 14 digitos
                    )
                    
                    # 3. Define lista de exclusão (Transferências e FOB)
                    cnpjs_excluir = ["17451156000191", "17451156000272"]
                    
                    # 4. Aplica filtros
                    mask_excluir = df_cif['__cnpj_temp'].isin(cnpjs_excluir)
                    
                    # Verifica vazios (string vazia, 'nan' string ou NaN real)
                    mask_vazio = (
                        (df_cif['__cnpj_temp'] == '') | 
                        (df_cif['__cnpj_temp'] == 'nan') | 
                        (df_cif['__cnpj_temp'].isna()) |
                        (df_cif['__cnpj_temp'] == '00000000000000') # Caso tenha vindo 0 numérico
                    )
                    
                    # DEBUG: Mostra o que ele achou para termos certeza
                    # print("DEBUG - CNPJs processados:", df_cif['__cnpj_temp'].unique())

                    # Filtro final: Mantém apenas o que NÃO é excluir e NÃO é vazio
                    df_cif = df_cif[~mask_excluir & ~mask_vazio]
                    
                    # Remove coluna temporária
                    df_cif.drop(columns=['__cnpj_temp'], inplace=True)
                    
                    print(f"ℹ️ {len(df_cif)} linhas identificadas como CIF.")
                    
                    if not df_cif.empty:
                        # Opcional: Converter a coluna visual para texto para não ficar E+13 no Excel final
                        df_cif[col_cnpj_transp] = df_cif[col_cnpj_transp].astype(str).str.replace(r'\.0$', '', regex=True)

                        # Salva backup local
                        cif_path = os.path.join(download_dir, "Carregamentos_CIF.xlsx")
                        df_cif.to_excel(cif_path, index=False)
                        
                        # Upload para SharePoint
                        upload_df_to_sharepoint(
                            df=df_cif,
                            tenant_id=TENANT_ID,
                            client_id=CLIENT_ID,
                            client_secret=CLIENT_SECRET,
                            hostname=HOSTNAME,
                            site_name="/sites/Transportes",
                            library_name="Documentos",
                            file_name="Carregamentos CIF.xlsx",
                            folder_path="" 
                        )
                    else:
                        print("⚠️ Nenhuma carga CIF encontrada neste processamento.")
                else:
                    print("❌ Coluna 'CNPJ do transportador' não encontrada. Relatório CIF pulado.")

        except Exception as e:
            print(f"❌ Falha no upload: {e}")

if __name__ == "__main__":
    test()