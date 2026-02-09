from playwright.sync_api import sync_playwright
import time
import os
import pandas as pd
import numpy as np
import re
import unicodedata
from typing import List, Optional, Dict, Any, Set
import io # Para salvar o excel em memória
import msal # Para autenticação MS
import requests # Para chamadas à API Graph
from dotenv import load_dotenv # Para ler o .env
from datetime import datetime

# --- NOVOS IMPORTS (Para Código de Barras e Imagens) ---
import xlsxwriter # <--- NOVO: Biblioteca específica para escrever Excel com melhor controle de imagem
import barcode
from barcode.writer import ImageWriter
from PIL import Image as PILImage # Para otimização de imagem

# --- Carrega as variáveis de ambiente do .env ---
load_dotenv()

TENANT_ID: str = os.getenv("TENANT_ID")
CLIENT_ID: str = os.getenv("CLIENT_ID")
CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
HOSTNAME: str = os.getenv("HOSTNAME")

GRAPH_API_URL = "https://graph.microsoft.com/v1.0"

# --- Dicionário de Mapeamento de Colunas (De/Para) ---
DE_PARA_COLUNAS: Dict[str, str] = {
    # 'De' (Original SIEG) : 'Para' (Padronizado)
    "Produto": "[Item] Descrição",
    "Chave": "chave de acesso",
    "Quantidade": "[Item] Quantidade",
    "UF_Dest": "UF Destino",
    "Numero": "Número",
    "Dt_Emissao": "data emissão",
    "UF_Emit": "UF Origem",
    "Placa1": "Placa do veículo",
    "Info_Adic": "dados adicionais",
    "CNPJ_CPF_Emit": "CNPJ Emitente",
    "Rz_Emit": "nome pj emitente",
    "Rz_Dest": "destinatário",
    "Valor_Unitario": "[Item] Valor Unitário",
}

def safe_filename(name: str) -> str:
    """Remove caracteres inválidos para nomes de arquivo"""
    return re.sub(r'[\\/:"*?<>|]+', "", str(name)).strip()

def _remove_accents_and_normalize(text: str) -> str:
    """Remove acentos, reduz espaços, devolve lowercase"""
    if pd.isna(text):
        return ""
    s = str(text)
    s = s.strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"\s+", " ", s)
    return s.lower()

def _limpar_cnpj_texto(cnpj_series: pd.Series) -> pd.Series:
    """Helper para normalizar CNPJ/CPF como texto limpo."""
    if cnpj_series.empty:
        return cnpj_series
    return cnpj_series.astype(str).str.strip().str.replace(r'[^\d]', '', regex=True)

def aplicar_de_para_descricao(series_descricao: pd.Series) -> pd.Series:
    """Aplica mapeamento de nomes padronizados para os produtos."""
    original = series_descricao.fillna("").astype(str).str.strip()
    norm = original.apply(_remove_accents_and_normalize)

    mask_aditivo = norm.str.contains(r"\baditivo\b", na=False)

    patterns = [
        (r"gasolina c", "Gasolina C"),
        (r"gasolina a", "Gasolina A"),
        (r"\bb\s*-?\s*s-?10\b", "Diesel B S10"),
        (r"\bb\s*-?\s*s-?500\b", "Diesel B S500"),
        (r"(?:diesel|a)\s*-?\s*s-?10\b", "Diesel A S10"),
        (r"(?:diesel|a)\s*-?\s*s-?500\b", "Diesel A S500"),
        (r"\bb100\b", "Biodiesel"),
        (r"\bMgo\b", "Mgo"),
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
    """Extrai até 3 placas e o nome do motorista."""
    placas_final_list: List[Optional[str]] = [None, None, None]
    motorista: Optional[str] = None

    if isinstance(texto, str):
        plate_pattern_inner = r'[A-Z]{3}-?\d{4}|[A-Z]{3}-?\d[A-Z]\d{2}'
        found_plates_set: Set[str] = set()

        regex_safe = rf'\b({plate_pattern_inner})\b'
        easy_plates = re.findall(regex_safe, texto)
        found_plates_set.update(easy_plates)

        regex_stuck_blobs = rf'(?:Placa|Placas)\s*((?:{plate_pattern_inner})+)'
        stuck_blobs = re.findall(regex_stuck_blobs, texto, re.IGNORECASE)

        if stuck_blobs:
            regex_individual_plates = rf'({plate_pattern_inner})'
            for blob in stuck_blobs:
                plates_from_blob = re.findall(regex_individual_plates, blob)
                found_plates_set.update(plates_from_blob)
        
        encontradas = list(found_plates_set)
        for i in range(min(3, len(encontradas))):
            placas_final_list[i] = encontradas[i]
            
        padroes_motorista = [
            r'N\. da OC / Motorista / Placa:\s*\d+\s+([a-zà-ÿ\s]+?)\s+[A-Z]{3}-?\d[A-Z]\d{2,3}',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*:\s*([a-zà-ÿ\s]+?)\s*-\s*(?:Placa[\w\s\d]*|CPF|CNH|DADOS DO LAUDO)',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*[:\-]?\s*(?:\d+\s+)?([a-zà-ÿ\s]+?)\s+(?:CPF:|CNH:)',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*[:\-]?\s*(?:\d+\s+)?([a-zà-ÿ\s]+?)\s+(?:Densidade:|Temperatura:)',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*[:\-]?\s*(?:\d+\s+)?([a-zà-ÿ\s]+?)\s*[\s\/]+\s*(?:CPF|CNH|Placa[\w\s\d]*|NUMERO DA O\.C\.|LOCAL DE DESCARGA)',
            r'(?:motorista|mot\.?|nome do motorista|nome motorista)\s*[:\-]?\s*(?:\d+\s*)?([A-ZÀ-Ÿ\s]{5,40})(?:$|\n|\r| - )'
        ]

        for padrao in padroes_motorista:
            match = re.search(padrao, texto, re.IGNORECASE)
            if match:
                nome_potencial = match.group(1).strip().strip('-').strip()
                if len(nome_potencial) > 4 and "DECLARO" not in nome_potencial.upper():
                    motorista = nome_potencial
                    break

    return placas_final_list + [motorista]

def carregar_lista_exclusao(caminho_arquivo: str) -> Set[str]:
    """Carrega lista de exclusão."""
    if not os.path.exists(caminho_arquivo):
        print(f"ℹ️ Arquivo de exclusão não encontrado em {caminho_arquivo}. Nenhuma exclusão será aplicada.")
        return set()
    try:
        df_excluir = pd.read_excel(caminho_arquivo, dtype=str)
        if "CNPJ" not in df_excluir.columns or "Produto" not in df_excluir.columns:
            print(f"⚠️ Arquivo de exclusão não contém colunas CNPJ e Produto.")
            return set()
        print(f"🔄 Carregando arquivo de exclusão '{os.path.basename(caminho_arquivo)}'...")
        df_excluir["CNPJ_Limpo"] = _limpar_cnpj_texto(df_excluir["CNPJ"])
        df_excluir["Produto_Limpo"] = df_excluir["Produto"].astype(str).str.strip()
        chaves_exclusao = df_excluir["CNPJ_Limpo"] + "|" + df_excluir["Produto_Limpo"]
        chaves_unicas = set(chaves_exclusao.dropna())
        print(f"✅ Carregadas {len(chaves_unicas)} chaves de exclusão.")
        return chaves_unicas
    except Exception as e:
        print(f"❌ Erro ao ler o arquivo de exclusão: {e}")
        return set()

def get_access_token(tenant_id: str, client_id: str, client_secret: str) -> Optional[str]:
    """Obtém um token de acesso da API Microsoft Graph."""
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

# --- ATUALIZADO: GERA EXCEL COM XLSXWRITER (CORRIGE ERRO DE FILTRO) ---
def gerar_excel_com_barras(df: pd.DataFrame, col_chave: str = "chave de acesso") -> io.BytesIO:
    """
    Gera um arquivo Excel em memória usando XlsxWriter.
    Isso permite fixar a imagem à célula (object_position: 1),
    evitando que elas se amontoem ao filtrar.
    """
    print("🏭 Gerando códigos de barras e otimizando imagens (via XlsxWriter)...")
    
    output = io.BytesIO()
    
    # 1. Cria o Workbook e Worksheet com XlsxWriter
    # 'in_memory': True é importante para não criar arquivos temporários
    workbook = xlsxwriter.Workbook(output, {'in_memory': True})
    worksheet = workbook.add_worksheet("Relatorio")

    # Formatos básicos
    header_format = workbook.add_format({'bold': True, 'bg_color': '#D3D3D3', 'border': 1})
    cell_format = workbook.add_format({'valign': 'vcenter', 'text_wrap': False})

    # 2. Escrever Cabeçalhos
    colunas = list(df.columns)
    colunas.append("Código de Barras") # Adiciona coluna extra
    
    for col_num, value in enumerate(colunas):
        worksheet.write(0, col_num, value, header_format)

    # Identifica índices das colunas
    try:
        idx_chave = df.columns.get_loc(col_chave)
        idx_barras = len(df.columns) # A nova coluna será a última
    except KeyError:
        print(f"⚠️ Coluna '{col_chave}' não encontrada. Gerando sem barras.")
        workbook.close()
        output.seek(0)
        return output

    # Configuração do gerador de Código de Barras
    Code128 = barcode.get_barcode_class('code128')
    writer_options = {
        'module_width': 0.4,
        'module_height': 10.0,
        'font_size': 1,
        'quiet_zone': 1.0,
        'write_text': False
    }

    # Ajusta largura da coluna de barras
    worksheet.set_column(idx_barras, idx_barras, 50)

    # 3. Loop para escrever dados e inserir imagens
    # df.values retorna um array numpy, iteramos sobre ele
    for row_num, row_data in enumerate(df.values, start=1):
        # A. Escreve os dados textuais da linha
        for col_num, cell_value in enumerate(row_data):
            # Trata NaNs para string vazia (xlsxwriter não gosta de NaN nativo)
            val = "" if pd.isna(cell_value) else cell_value
            # Converte datas para string se necessário ou mantém formato
            if isinstance(val, (pd.Timestamp, datetime)):
                val = val.strftime('%d/%m/%Y')
            
            worksheet.write(row_num, col_num, val, cell_format)

        # B. Gera e insere o Código de Barras
        chave = str(row_data[idx_chave]).strip()

        if chave and chave.isdigit():
            try:
                # Gera Barcode na memória (BytesIO)
                rv = io.BytesIO()
                code_img = Code128(chave, writer=ImageWriter())
                code_img.write(rv, options=writer_options)

                # Otimização com Pillow (Resize)
                rv.seek(0)
                img_pil = PILImage.open(rv)
                
                # Redimensiona mantendo proporção (Largura ~600px para qualidade)
                basewidth = 600
                w_percent = (basewidth / float(img_pil.size[0]))
                h_size = int((float(img_pil.size[1]) * float(w_percent)))
                img_pil = img_pil.resize((basewidth, h_size), PILImage.LANCZOS)
                
                # Salva no buffer final como PNG
                img_buffer = io.BytesIO()
                img_pil.save(img_buffer, format="PNG", optimize=True)
                img_buffer.seek(0)

                # C. Inserir no Excel com XlsxWriter
                # object_position: 1 -> Move and size with cells (CORREÇÃO DO FILTRO)
                worksheet.insert_image(row_num, idx_barras, "barcode.png", {
                    'image_data': img_buffer,
                    'x_scale': 0.5, 
                    'y_scale': 0.5,
                    'x_offset': 5,
                    'y_offset': 5,
                    'object_position': 1 
                })
                
                # Define altura da linha (pixels) para a imagem caber
                worksheet.set_row(row_num, 80)

            except Exception as e:
                # Se der erro na imagem, deixa em branco ou poe aviso
                worksheet.write(row_num, idx_barras, "Erro img", cell_format)
        else:
             # Caso não tenha chave válida
             worksheet.write(row_num, idx_barras, "-", cell_format)

    # Fecha o workbook e retorna o buffer
    workbook.close()
    output.seek(0)
    print("✅ Geração de códigos de barras (XlsxWriter) concluída!")
    return output

# --- UPLOAD ATUALIZADO: Aceita binário ou DataFrame ---
def upload_df_to_sharepoint(df: Optional[pd.DataFrame], 
                            tenant_id: str, 
                            client_id: str, 
                            client_secret: str, 
                            hostname: str, 
                            site_name: str, 
                            library_name: str, 
                            file_name: str,
                            folder_path: str = "",
                            file_content_binary: Optional[io.BytesIO] = None):
    """
    Faz o upload. Se 'file_content_binary' for passado, usa ele. 
    Caso contrário, converte o 'df' para Excel.
    """
    
    # 1. Obter Access Token
    access_token = get_access_token(tenant_id, client_id, client_secret)
    if not access_token:
        raise Exception("Falha ao obter token de acesso.")
    
    headers = {"Authorization": f"Bearer {access_token}"}
    
    try:
        # 2. Obter o ID do Site
        print(f"Buscando Site ID para '{site_name}'...")
        site_url = f"{GRAPH_API_URL}/sites/{hostname}:{site_name}"
        site_resp = requests.get(site_url, headers=headers)
        site_resp.raise_for_status()
        site_id = site_resp.json()["id"]
        print(f"✅ Site ID encontrado: {site_id}")

        # 3. Obter o ID da Biblioteca (Drive)
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
        print(f"✅ Drive ID encontrado: {drive_id}")

        # 4. Define o conteúdo do arquivo
        if file_content_binary:
            print("Usando arquivo Excel pré-gerado (com barras)...")
            file_content = file_content_binary.read()
        else:
            print("Convertendo DataFrame para Excel em memória (sem barras)...")
            output = io.BytesIO()
            df.to_excel(output, index=False, engine='openpyxl')
            output.seek(0)
            file_content = output.read()

        # 5. Definir URL de Upload
        if folder_path:
            upload_url = f"{GRAPH_API_URL}/sites/{site_id}/drives/{drive_id}/root:/{folder_path}/{file_name}:/content"
        else:
            upload_url = f"{GRAPH_API_URL}/sites/{site_id}/drives/{drive_id}/root/children/{file_name}/content"

        # 6. Fazer o Upload (PUT)
        print(f"Fazendo upload de '{file_name}' para SharePoint...")
        headers["Content-Type"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        
        upload_resp = requests.put(upload_url, headers=headers, data=file_content)
        upload_resp.raise_for_status()
        
        print("\n✅✅ Upload para o SharePoint concluído com sucesso! ✅✅")
        print(f"Arquivo: {upload_resp.json().get('name')}")
        print(f"Link: {upload_resp.json().get('webUrl')}")

    except requests.exceptions.HTTPError as e:
        print(f"❌ ERRO DE API: {e.response.status_code} - {e.response.text}")
    except Exception as e:
        print(f"❌ Erro inesperado no upload: {e}")


def test():
    # --- Diretórios ---
    home_dir = os.path.expanduser("~")

    sieg_dir = os.path.join(home_dir, "Documentos", "Sieg")
    empresas_dir = os.path.join(sieg_dir, "empresas")
    os.makedirs(sieg_dir, exist_ok=True)
    os.makedirs(empresas_dir, exist_ok=True)

    print(f"📂 Downloads em: {sieg_dir}")
    print(f"📂 Arquivos auxiliares em: {empresas_dir}")

    # --- Carregar chaves de exclusão ---
    caminho_exclusao = os.path.join(empresas_dir, "CNPJ NAO ELETIVOS.xlsx")
    chaves_para_excluir = carregar_lista_exclusao(caminho_exclusao)

    # --- Lê planilha com CNPJs ---
    cnpj_file = os.path.join(empresas_dir, "CNPJ.xlsx")
    try:
        df = pd.read_excel(cnpj_file, dtype={"CNPJ Destinatário": str})
    except FileNotFoundError:
        print(f"❌ Erro: Arquivo {cnpj_file} não encontrado.")
        return

    print(f"📄 {len(df)} registros brutos carregados de {cnpj_file}")

    # --- Limpeza CNPJs ---
    df["CNPJ Destinatário"] = df["CNPJ Destinatário"].astype(str).str.strip()
    df = df.dropna(subset=["CNPJ Destinatário"])
    df = df[df["CNPJ Destinatário"] != ""]

    before_dedupe = len(df)
    df = df.drop_duplicates(subset=["CNPJ Destinatário"], keep="first")
    after_dedupe = len(df)
    print(f"🗑️ Removidas {before_dedupe - after_dedupe} linhas duplicadas de CNPJ.")
    print(f"➡️ {after_dedupe} CNPJs únicos a processar.")

    set_cnpjs_mestre_limpos = set(_limpar_cnpj_texto(df["CNPJ Destinatário"]))
    print(f"ℹ️ Criado set mestre com {len(set_cnpjs_mestre_limpos)} CNPJs limpos.")

    # --- INÍCIO PLAYWRIGHT ---
    with sync_playwright() as p:
        # Ajuste o path do Chrome se necessário
        browser = p.chromium.launch(
            executable_path="C:/Program Files/Google/Chrome/Application/chrome.exe",
            headless=False
        )
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_viewport_size({"width": 1500, "height": 1050})

        # --- LOGIN ---
        if df.empty:
            print("Nenhum CNPJ para processar.")
            return

        first_row = df.iloc[0]
        first_cnpj = str(first_row["CNPJ Destinatário"]).strip()
        first_cnpj_num = "".join(filter(str.isdigit, first_cnpj)).zfill(14)
        first_url = f"https://hub.sieg.com/detalhes-do-cliente?id=47450-{first_cnpj_num}"

        print(f"🌐 Acessando primeiro cliente ({first_cnpj_num}) para login...")
        page.goto(first_url)
        try:
            page.wait_for_selector("#txtEmail", timeout=20000)
            page.locator("#txtEmail").fill("elivelton.pereira@refit.com.br")
            page.locator("#txtPassword").fill("Timao*2012")
            page.keyboard.press("Tab")
            page.keyboard.press("Enter")
            print("✅ Login realizado com sucesso!")
            time.sleep(5)
        except Exception as e:
            print(f"⚠️ Erro no login ou já logado: {e}")

        # --- Loop para todos os CNPJs ---
        total = len(df)
        for idx, row in df.iterrows():
            destinatario = str(row.get("Destinatário", "Desconhecido")).strip()
            raw_cnpj = str(row["CNPJ Destinatário"]).strip()
            cnpj_num = "".join(filter(str.isdigit, raw_cnpj)).zfill(14)

            if not cnpj_num or cnpj_num == "0" * 14:
                print(f"⚠️ Linha {idx+1}: CNPJ inválido/ausente — pulando.")
                continue

            url = f"https://hub.sieg.com/detalhes-do-cliente?id=47450-{cnpj_num}"
            print(f"\n➡️ ({idx+1}/{total}) {destinatario} | CNPJ: {cnpj_num}")

            # --- 1. Tenta carregar a página (Máximo 20 segundos) ---
            try:
                page.goto(url, timeout=20000)
                # Pequena pausa para garantir renderização básica
                time.sleep(2) 
            except Exception as e:
                print(f"❌ Timeout/Erro ao carregar a página: {e}")
                continue # Pula para o próximo CNPJ imediatamente

            # --- 2. Tenta abrir o menu de extração (Máximo 5 segundos) ---
            try:
                page.wait_for_selector("#accordion-excel", state="visible", timeout=5000)
                page.locator("#accordion-excel").click()
                time.sleep(1)
            except Exception:
                print("⚠️ Botão de Excel não encontrado (provável erro na página do cliente). Pulando...")
                continue

            # --- 3. Selecionar “Detalhamento de Produtos” ---
            try:
                if page.locator("#ddlTypeExport").nth(1).is_visible():
                    page.locator("#ddlTypeExport").nth(1).select_option(value="3")
                    time.sleep(1)
                else:
                    print("⚠️ Dropdown de exportação não visível.")
                    continue
            except Exception as e:
                print(f"⚠️ Erro ao selecionar opção: {e}")
                continue

            # --- 4. Exportar arquivo (Máximo 30 segundos para iniciar) ---
            print("⬇️ Tentando baixar...")
            try:
                btn_exportar = page.locator("a.btn-outline-excel:has-text('Exportar')")
                if not btn_exportar.is_visible():
                    print("⚠️ Botão 'Exportar' não apareceu.")
                    continue

                with page.expect_download(timeout=30000) as download_info:
                    btn_exportar.click()

                download = download_info.value
                safe_name = safe_filename(destinatario)
                base_nome = f"{safe_name}_{cnpj_num[-2:]}"
                destino = os.path.join(sieg_dir, f"{base_nome}.xlsx")

                download.save_as(destino)
                print(f"✅ Arquivo baixado: {destino}")

            except Exception as e:
                print(f"❌ Falha no download (timeout ou erro): {e}")
                continue

        print("\n🏁 Processo concluído com sucesso!")
        browser.close()

    # --- UNIFICAÇÃO ---
    print("\n📊 Iniciando unificação dos arquivos baixados...")

    arquivos = [f for f in os.listdir(sieg_dir) if f.endswith(".xlsx") and f != "Consolidado_Sieg.xlsx"]
    if not arquivos:
        print("⚠️ Nenhum arquivo .xlsx encontrado para unificar.")
        return

    colunas_como_texto = {
        'CNPJ_CPF_Emit': str, 'CNPJ_CPF_Dest': str,
        'Chave': str, 'Numero': str 
    }

    tabelas = []
    for arquivo in arquivos:
        caminho = os.path.join(sieg_dir, arquivo)
        try:
            df_temp = pd.read_excel(caminho, dtype=colunas_como_texto)
            df_temp["Origem_Arquivo"] = arquivo
            
            if "Chave" in df_temp.columns:
                df_temp = df_temp.dropna(subset=["Chave"])
            elif "Numero" in df_temp.columns:
                df_temp = df_temp.dropna(subset=["Numero"])
            
            tabelas.append(df_temp)
            print(f"📥 Lido: {arquivo} ({len(df_temp)} linhas)")
        except Exception as e:
            print(f"⚠️ Erro ao ler {arquivo}: {e}")

    if tabelas:
        consolidado = pd.concat(tabelas, ignore_index=True)

        # Filtros e Limpezas de Consolidação
        if "CNPJ_CPF_Emit" in consolidado.columns:
            consolidado = consolidado.dropna(subset=['CNPJ_CPF_Emit'])

        if "Chave" in consolidado.columns:
            print("🔄 Verificando duplicatas (Chave)...")
            consolidado = consolidado.dropna(subset=['Chave'])
            consolidado = consolidado.drop_duplicates(subset=["Chave"], keep="first")

        if "CNPJ_CPF_Dest" in consolidado.columns and set_cnpjs_mestre_limpos:
            print(f"🔄 Aplicando filtro 'CNPJ_CPF_Dest'...")
            consolidado["CNPJ_Dest_Limpo"] = _limpar_cnpj_texto(consolidado["CNPJ_CPF_Dest"])
            mask_manter = consolidado["CNPJ_Dest_Limpo"].isin(set_cnpjs_mestre_limpos)
            consolidado = consolidado[mask_manter]
            consolidado = consolidado.drop(columns=["CNPJ_Dest_Limpo"])

        # Conversão M² -> Litro
        if "Quantidade" in consolidado.columns and "Valor_Unitario" in consolidado.columns:
            print("🔄 Padronizando Quantidade...")
            consolidado["Quantidade"] = pd.to_numeric(consolidado["Quantidade"], errors='coerce')
            consolidado["Valor_Unitario"] = pd.to_numeric(consolidado["Valor_Unitario"], errors='coerce')
            mask_converter = consolidado["Valor_Unitario"] > 10.0
            if mask_converter.sum() > 0:
                consolidado.loc[mask_converter, "Valor_Unitario"] = consolidado.loc[mask_converter, "Valor_Unitario"] / 1000
                consolidado.loc[mask_converter, "Quantidade"] = consolidado.loc[mask_converter, "Quantidade"] * 1000

        # Filtro Posto
        if "Rz_Emit" in consolidado.columns:
            print("🔄 Filtrando 'Posto'...")
            mask_posto = consolidado["Rz_Emit"].astype(str).str.contains(r"Posto\s|Postos\s", case=False, na=False, regex=True)
            consolidado = consolidado[~mask_posto]

        # Colunas finais
        colunas_desejadas = [
            "CNPJ_CPF_Emit", "Rz_Emit", "UF_Emit", "Produto", "Numero",
            "Quantidade", "Dt_Emissao", "CNPJ_CPF_Dest", "Rz_Dest",
            "UF_Dest", "Valor_Total_Nota", "Valor_Unitario", "Chave",
            "Status", "Info_Adic"
        ]
        colunas_existentes = [c for c in colunas_desejadas if c in consolidado.columns]
        consolidado = consolidado[colunas_existentes]

        # Aplica De/Para Produto
        if "Produto" in consolidado.columns:
            print("🔄 Aplicando de/para Produto...")
            consolidado["Produto"] = aplicar_de_para_descricao(consolidado["Produto"])
            consolidado = consolidado.dropna(subset=["Produto"])

        # Exclusão via arquivo
        if ("CNPJ_CPF_Emit" in consolidado.columns and "Produto" in consolidado.columns and chaves_para_excluir):
            print(f"🔄 Aplicando exclusão...")
            consolidado["CNPJ_Emit_Limpo"] = _limpar_cnpj_texto(consolidado["CNPJ_CPF_Emit"])
            consolidado["Produto_Limpo"] = consolidado["Produto"].astype(str)
            consolidado["Chave_Verificacao"] = consolidado["CNPJ_Emit_Limpo"] + "|" + consolidado["Produto_Limpo"]
            
            mask_excluir = consolidado["Chave_Verificacao"].isin(chaves_para_excluir)
            
            # (Opcional) Print de amostra
            if mask_excluir.any(): 
                chaves_excluidas_do_consolidado = consolidado[mask_excluir]["Chave_Verificacao"].unique()
                print(f"ℹ️ (Amostra excluída): {list(chaves_excluidas_do_consolidado)[:5]}")

            consolidado = consolidado[~mask_excluir]
            consolidado = consolidado.drop(columns=["CNPJ_Emit_Limpo", "Produto_Limpo", "Chave_Verificacao"])

        # Placas
        if "Info_Adic" in consolidado.columns:
            print("🔄 Extraindo Placa/Motorista...")
            novas_colunas = consolidado['Info_Adic'].apply(extrair_placas_motorista).apply(pd.Series)
            novas_colunas.columns = ['Placa1', 'Placa2', 'Placa3', 'Motorista']
            consolidado = pd.concat([consolidado, novas_colunas], axis=1) 
            consolidado["Placa do veículo"] = consolidado["Placa1"]

        # Timestamp
        agora = datetime.now()
        timestamp_str = agora.strftime("%Y-%m-%d %H:%M:%S") 
        consolidado["Data_Hora_Atualizacao"] = timestamp_str
        
        # Reordenação final
        print("🔄 Reordenando colunas...")
        consolidado = consolidado.rename(columns=DE_PARA_COLUNAS).copy() 
        cols_sem_info = [c for c in consolidado.columns if c != 'dados adicionais']
        nova_ordem = cols_sem_info + ['dados adicionais']
        nova_ordem = [c for c in nova_ordem if c in consolidado.columns]
        consolidado = consolidado[nova_ordem]
        colunas_para_print = nova_ordem[:] 

        # --- GERAÇÃO DO ARQUIVO COM BARRAS E UPLOAD ---
        
        # 1. Gera o Excel com barras na memória (Isso evita salvar arquivo gigante no disco)
        # Atenção: Passamos 'chave de acesso' porque o DF já foi renomeado pelo DE_PARA_COLUNAS acima
        excel_com_barras_bytes = gerar_excel_com_barras(consolidado, col_chave="chave de acesso")

        # 2. Salva uma cópia local (já com barras)
        consolidado_path = os.path.join(sieg_dir, "Consolidado_Sieg.xlsx")
        with open(consolidado_path, "wb") as f:
            f.write(excel_com_barras_bytes.getbuffer())
        print(f"\n✅ Arquivo consolidado local (com barras) criado: {consolidado_path}")

        # 3. Faz o upload para o SharePoint
        print("\n☁️ Iniciando upload para o SharePoint...")
        try:
            excel_com_barras_bytes.seek(0) # Volta para o início do arquivo na memória
            
            if not all([TENANT_ID, CLIENT_ID, CLIENT_SECRET, HOSTNAME]):
                print("⚠️ Variáveis de ambiente incompletas. Pulando upload.")
            else:
                upload_df_to_sharepoint(
                    df=None, # Não precisamos passar o DF pois vamos passar o binário
                    tenant_id=TENANT_ID,
                    client_id=CLIENT_ID,
                    client_secret=CLIENT_SECRET,
                    hostname=HOSTNAME,
                    site_name="/sites/Transportes",
                    library_name="Documentos",
                    file_name="Relatório de NF Sieg.xlsx",
                    folder_path="",
                    file_content_binary=excel_com_barras_bytes # Passamos o arquivo gerado
                )
        except Exception as e:
            print(f"❌ Erro fatal durante o upload para o SharePoint: {e}")

        print(f"\n🧾 Colunas finais incluídas: {', '.join(colunas_para_print)}")
    else:
        print("⚠️ Nenhum arquivo pôde ser lido para unificação.")

if __name__ == "__main__":
    test()