# -*- coding: utf-8 -*-

import io
import os
import logging
import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any, Optional
import warnings
from datetime import date, datetime, timedelta
import openpyxl

# --- BIBLIOTECAS PARA O GOOGLE SHEETS ---
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
warnings.filterwarnings('ignore', category=FutureWarning) # Ignora o aviso de concat que vimos
load_dotenv()

# ==============================================================================
# 2. CLASSES DE CONFIGURAÇÃO E CLIENTES (SHAREPOINT & GOOGLE)
# ==============================================================================
# (Nenhuma alteração nesta seção, ela permanece a mesma)
def autenticar_google_sheets():
    """Autentica com a API do Google e retorna o cliente."""
    logging.info("--- Autenticando com a API do Google Sheets ---")
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'credenciais.json')
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        client = gspread.authorize(creds)
        logging.info("✅ Autenticação com Google Sheets bem-sucedida.")
        return client
    except Exception as e:
        logging.error(f"❌ ERRO durante a autenticação com Google: {e}")
        return None

class Config:
    """⚙️ Centraliza todas as configurações e parâmetros da aplicação."""
    TENANT_ID: str = os.getenv("TENANT_ID")
    CLIENT_ID: str = os.getenv("CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
    HOSTNAME: str = os.getenv("HOSTNAME")
    SITE_PATH: str = "/sites/DataLake"
    DRIVE_NAME: str = "Documentos"
    FOLDER_PATH: str = "Bases"
    
    KEYWORDS_TO_EXCLUDE: List[str] = ["backup", "modelo", "corrompida", "corrompido", "dinamica"]

    FILENAME_MAP: Dict[str, str] = {
        "ARUJA": "Aruja", "BARRA_MANSA": "Barra Mansa", "BCAG": "BCAG",
        "CAVALINI": "Cavalini", "CROSS": "Cross Terminais", "DIRECIONAL_FILIAL": "Direcional Filial",
        "DIRECIONAL_MATRIZ": "Direcional Matriz", "FLAG": "Flag", "GRANEL_QUIMICA": "Granel Química",
        "MANGUINHOS": "Caxias", "PETRONORTE": "Petronorte", "REFIT_BASE": "Refit", "RODOPETRO_CAPIVARI": "Capivari",
        "SANTOS_BRASIL": "Santos Brasil", "SGP": "SGP", "STOCK_": "Stock",
        "STOCKMAT": "Stockmat", "TIF": "TIF", "TLIQ": "Tliq", "TRANSO": "Transo",
        "TRR_AB": "Americo", "TRR_CATANDUVA": "Catanduva", "VAISHIA": "Vaishia"
    }

    @staticmethod
    def validate():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET]):
            raise ValueError("❌ Faltam credenciais no arquivo .env.")
        logging.info("Credenciais de ambiente para SharePoint carregadas com sucesso.")

class SharePointClient:
    """Classe para interagir com a API do Microsoft Graph para o SharePoint."""
    def __init__(self, config: Config):
        self.config = config
        self.access_token = self._get_access_token()
        self.site_id = self._get_site_id()
        self.drive_id = self._get_drive_id()

    def _api_request(self, method: str, url: str, **kwargs) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.request(method, url, headers=headers, **kwargs)
        response.raise_for_status()
        if response.content and 'application/json' in response.headers.get('Content-Type', ''):
            return response.json()
        return None

    def _get_access_token(self) -> str:
        url = f"https://login.microsoftonline.com/{self.config.TENANT_ID}/oauth2/v2.0/token"
        data = {
            "client_id": self.config.CLIENT_ID, "scope": "https://graph.microsoft.com/.default",
            "client_secret": self.config.CLIENT_SECRET, "grant_type": "client_credentials"
        }
        response = requests.post(url, data=data)
        response.raise_for_status()
        return response.json()["access_token"]

    def _get_site_id(self) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{self.config.HOSTNAME}:{self.config.SITE_PATH}"
        return self._api_request('get', url)["id"]

    def _get_drive_id(self) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        drives = self._api_request('get', url).get("value", [])
        drive_name_lower = self.config.DRIVE_NAME.lower()
        for drive in drives:
            if drive['name'].lower() == drive_name_lower: return drive['id']
        raise FileNotFoundError(f"Biblioteca '{self.config.DRIVE_NAME}' não encontrada.")

    def get_files_in_folder(self) -> List[Dict[str, Any]]:
        path_segment = f"/root:/{requests.utils.quote(self.config.FOLDER_PATH)}:" if self.config.FOLDER_PATH else "/root"
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}{path_segment}/children"
        return self._api_request('get', url).get("value", [])
    
    def extrair_bloco_de_dados(self, file_id: str, file_name: str, nome_aba_fonte: str, texto_inicial: str) -> Optional[pd.DataFrame]:
        try:
            url_item = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}"
            download_url = self._api_request('get', url_item).get('@microsoft.graph.downloadUrl')
            if not download_url: return None
            
            response_content = requests.get(download_url, timeout=60)
            response_content.raise_for_status()
            
            xls = pd.ExcelFile(io.BytesIO(response_content.content))
            actual_sheet_name = next((s for s in xls.sheet_names if s.lower() == nome_aba_fonte.lower()), None)
            
            if not actual_sheet_name:
                logging.info(f"Aba '{nome_aba_fonte}' não encontrada no arquivo '{file_name}'. Pulando.")
                return None

            df_full = pd.read_excel(xls, sheet_name=actual_sheet_name, header=None)
            
            start_row_index = -1
            for index, row in df_full.iterrows():
                if any(texto_inicial in str(cell).lower() for cell in row if pd.notna(cell)):
                    start_row_index = index
                    break
            
            if start_row_index == -1: return None

            df_data = df_full.iloc[start_row_index:].copy().reset_index(drop=True)
            stop_row_index = next((index for index, row in df_data.iterrows() if row.isnull().all()), -1)
            df_block = df_data.iloc[:stop_row_index] if stop_row_index != -1 else df_data
            
            if df_block.empty: return None
            
            new_header = df_block.iloc[0]
            df_final = df_block[1:].copy()
            df_final.columns = new_header
            df_final.reset_index(drop=True, inplace=True)
            df_final.dropna(axis=1, how='all', inplace=True)

            return df_final

        except Exception as e:
            logging.error(f"Falha ao processar o bloco '{texto_inicial}' no arquivo {file_name}. Erro: {e}")
            return None

# ==============================================================================
# 3. FUNÇÕES DE DADOS (COLETA E SALVAMENTO)
# ==============================================================================
# (Nenhuma alteração nesta seção, ela permanece a mesma)
def coletar_dados(sp_client: SharePointClient, files_to_process: list, config: Config, nome_aba_fonte: str, texto_inicial: str) -> pd.DataFrame:
    list_of_dataframes, success_count = [], 0
    
    for item in files_to_process:
        file_name = item['name']
        df = sp_client.extrair_bloco_de_dados(item['id'], file_name, nome_aba_fonte, texto_inicial)
        
        if df is not None and not df.empty:
            mapped_name = next((value for key, value in config.FILENAME_MAP.items() if key.lower() in file_name.lower()), file_name)
            df['Origem'] = mapped_name
            list_of_dataframes.append(df)
            success_count += 1
            
    logging.info(f"[{nome_aba_fonte}] Processamento concluído: {success_count} de {len(files_to_process)} arquivos continham dados válidos.")
            
    if not list_of_dataframes:
        return pd.DataFrame()
        
    return pd.concat(list_of_dataframes, ignore_index=True)

def salvar_no_sheets(client, df, url_planilha, nome_aba):
    try:
        logging.info(f"Abrindo planilha para salvar na aba '{nome_aba}'...")
        spreadsheet = client.open_by_url(url_planilha)
        worksheet = spreadsheet.worksheet(nome_aba)
        worksheet.clear()
        df_str = df.astype(str).replace('nan', '')
        set_with_dataframe(worksheet, df_str, include_index=False, include_column_header=True, resize=True)
        logging.info(f"✅ {len(df)} linhas salvas com sucesso na aba '{nome_aba}'.")
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"❌ ERRO: A aba '{nome_aba}' não foi encontrada na planilha. Crie-a manualmente.")
    except Exception as e:
        logging.error(f"❌ Ocorreu um erro ao salvar na aba '{nome_aba}': {e}")

# ==============================================================================
# 4. FUNÇÃO DE ORQUESTRAÇÃO DE PROCESSO
# ==============================================================================
# <<< NOVO PARÂMETRO 'manter_historico' ADICIONADO >>>
def executar_processo(google_client, sp_client, files_to_process, config, url_sheets, nome_aba_fonte, texto_inicial, nome_aba_destino, aplicar_filtro_data=False, manter_historico=True):
    logging.info(f"--- INICIANDO PROCESSO PARA '{nome_aba_destino}' ---")
    
    df_bruto = coletar_dados(sp_client, files_to_process, config, nome_aba_fonte, texto_inicial)
    
    if df_bruto.empty:
        logging.warning(f"O processo para '{nome_aba_destino}' terminou, mas nenhum dado novo foi coletado.")
        return

    if aplicar_filtro_data:
        logging.info(f"Aplicando filtro de data (> hoje - 3 dias) para '{nome_aba_destino}'...")
        if df_bruto.empty or df_bruto.shape[1] == 0:
            logging.warning("DataFrame está vazio, pulando filtro de data.")
        else:
            coluna_data = df_bruto.columns[0]
            df_bruto[coluna_data] = pd.to_datetime(df_bruto[coluna_data], errors='coerce')
            df_bruto.dropna(subset=[coluna_data], inplace=True)
            data_limite = datetime.now() - timedelta(days=3)
            linhas_antes = len(df_bruto)
            df_bruto = df_bruto[df_bruto[coluna_data] > data_limite].copy()
            linhas_depois = len(df_bruto)
            logging.info(f"Filtro aplicado. {linhas_antes - linhas_depois} linhas removidas. {linhas_depois} linhas restantes.")
            
    # <<< LÓGICA DE HISTÓRICO OU SUBSTITUIÇÃO DIRETA >>>
    if manter_historico:
        logging.info(f"Modo Histórico: Preservando dados antigos da aba '{nome_aba_destino}'.")
        today_str = date.today().strftime('%Y-%m-%d')
        df_bruto['Data_Atualizacao'] = today_str
        
        df_historico_preservado = pd.DataFrame()
        try:
            logging.info(f"Lendo histórico existente da aba '{nome_aba_destino}'...")
            spreadsheet = google_client.open_by_url(url_sheets)
            worksheet = spreadsheet.worksheet(nome_aba_destino)
            df_historico = get_as_dataframe(worksheet, evaluate_formulas=False, empty_value=np.nan).dropna(how='all')
            
            if not df_historico.empty and 'Data_Atualizacao' in df_historico.columns:
                df_historico['Data_Atualizacao'] = pd.to_datetime(df_historico['Data_Atualizacao'], errors='coerce').dt.strftime('%Y-%m-%d')
                df_historico_preservado = df_historico[df_historico['Data_Atualizacao'] != today_str]
            else:
                df_historico_preservado = df_historico
        except gspread.exceptions.WorksheetNotFound:
            logging.warning(f"Aba '{nome_aba_destino}' não encontrada. Um novo histórico será criado.")
        except Exception as e:
            logging.warning(f"Não foi possível ler o histórico da aba '{nome_aba_destino}'. Pode estar vazia. Erro: {e}")
        
        df_final = pd.concat([df_historico_preservado, df_bruto], ignore_index=True)
    else:
        # Modo de Substituição: usa apenas os dados brutos, sem histórico ou data de atualização
        logging.info(f"Modo Substituição: Os dados na aba '{nome_aba_destino}' serão completamente substituídos.")
        df_final = df_bruto

    cols_unnamed_to_drop = [col for col in df_final.columns if 'Unnamed' in str(col)]
    if cols_unnamed_to_drop:
        df_final.drop(columns=cols_unnamed_to_drop, inplace=True)
        
    salvar_no_sheets(google_client, df_final, url_sheets, nome_aba_destino)

# ==============================================================================
# 5. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    """Orquestrador principal que executa todos os processos."""
    try:
        logging.info("====== INICIANDO EXECUÇÃO COMPLETA DO SCRIPT ======")
        config = Config()
        config.validate()
        
        google_client = autenticar_google_sheets()
        if not google_client:
            raise ConnectionError("Falha na autenticação com Google. O processo será interrompido.")
            
        sp_client = SharePointClient(config)
        
        logging.info("Buscando lista de arquivos no SharePoint...")
        all_items = sp_client.get_files_in_folder()
        files_to_process = [item for item in all_items if "file" in item and not any(k in item['name'].lower() for k in config.KEYWORDS_TO_EXCLUDE)]
        logging.info(f"Encontrados {len(files_to_process)} arquivos para processar.")
        
        url_sheets = "https://docs.google.com/spreadsheets/d/19mc4J3oIm5oO_6oz5fjyNjgKE3lqgiw1H3rwyt8FuPE/edit?usp=sharing"

        # --- PROCESSO 1: CAPACIDADE ---
        # (Mantém o histórico, como antes)
        executar_processo(
            google_client=google_client, sp_client=sp_client, files_to_process=files_to_process, config=config,
            url_sheets=url_sheets,
            nome_aba_fonte="PAINEL DE TANQUES",
            texto_inicial="lastro",
            nome_aba_destino="Capacidade",
            manter_historico=True
        )
        
        # --- PROCESSO 2: TRANSFERÊNCIAS ---
        # <<< PARÂMETRO 'manter_historico=False' ADICIONADO E NOME DA ABA CORRIGIDO >>>
        executar_processo(
            google_client=google_client, sp_client=sp_client, files_to_process=files_to_process, config=config,
            url_sheets=url_sheets,
            nome_aba_fonte="MOV. TQ",
            texto_inicial="produto",
            nome_aba_destino="Transf", # Corrigido para "Transf" como no seu log
            aplicar_filtro_data=True,
            manter_historico=False # Não mantém histórico, apenas substitui
        )

        logging.info("====== EXECUÇÃO COMPLETA DO SCRIPT FINALIZADA ======")

    except Exception as e:
        logging.critical(f"❌ PROCESSO INTERROMPIDO POR ERRO CRÍTICO: {e}", exc_info=True)

if __name__ == "__main__":
    main()