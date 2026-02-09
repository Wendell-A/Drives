# -*- coding: utf-8 -*-

import io
import os
import logging
import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv
from typing import List, Dict, Any
import warnings
from datetime import date, datetime 

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
load_dotenv()

# ==============================================================================
# 2. CONFIGURAÇÃO GERAL (SHAREPOINT E PARÂMETROS)
# ==============================================================================

class Config:
    """⚙️ Centraliza todas as configurações e parâmetros da aplicação."""
    TENANT_ID: str = os.getenv("TENANT_ID")
    CLIENT_ID: str = os.getenv("CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
    HOSTNAME: str = os.getenv("HOSTNAME")

    # --- CONFIGURAÇÃO DE LEITURA (ORIGEM: Site DataLake / Folder Bases) ---
    DATALAKE_CONFIG: Dict[str, Any] = {
        "name": "DataLake Leitura",
        "site_path": "/sites/DataLake",
        "drive_name": "Documentos",
        "folder_path": "Bases",
        "sheet_name": "PAINEL DE TANQUES",
    }
    
    # --- CONFIGURAÇÃO DE ESCRITA (DESTINO: Site Transportes / Folder Disponibilidade) ---
    DESTINATION_CONFIG: Dict[str, Any] = {
        "name": "Site Transportes Escrita",
        "site_path": "/sites/Transportes",
        "drive_name": "Documentos",
        "folder_path": "Disponibilidade",
        "sheet_name": "Base_Consolidada",
    }
    
    SHAREPOINT_DEST_FILE: str = "Painel_Tanques_Consolidado.xlsx" 
    SHAREPOINT_DEST_SHEET: str = "Base_Consolidada"

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
    
    PRODUTOS_EXCLUIDOS: List[str] = ["GAS C", "B100", "AS10", "AS500","GAS, C"]

    @staticmethod
    def validate():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET]):
            raise ValueError("❌ Faltam credenciais no arquivo .env.")
        logging.info("Credenciais de ambiente para SharePoint carregadas com sucesso.")

# ==============================================================================
# 3. CLIENTE SHAREPOINT
# ==============================================================================

class SharePointClient:
    """Classe para interagir com a API do Microsoft Graph para o SharePoint."""
    def __init__(self, site_config: Dict[str, Any], config: Config):
        self.site_config = site_config
        self.config = config
        self.access_token = self._get_access_token()
        self.site_id = self._get_site_id()
        self.drive_id = self._get_drive_id()

    def _api_request(self, method: str, url: str, json: Dict = None, data=None) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            response = requests.request(method, url, headers=headers, json=json, data=data)
            response.raise_for_status()
            is_json_response = 'application/json' in response.headers.get('Content-Type', '')
            if response.content and is_json_response:
                return response.json()
            return None
        except requests.exceptions.HTTPError as e:
            logging.error(f"Erro na API ({method} {url}): {e.response.text}")
            raise

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
        url = f"https://graph.microsoft.com/v1.0/sites/{self.config.HOSTNAME}:{self.site_config['site_path']}"
        return self._api_request('get', url)["id"]

    def _get_drive_id(self) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        drives = self._api_request('get', url).get("value", [])
        drive_name_lower = self.site_config['drive_name'].lower()
        for drive in drives:
            if drive['name'].lower() == drive_name_lower: return drive['id']
        raise FileNotFoundError(f"Biblioteca '{self.site_config['drive_name']}' não encontrada.")

    def get_files_in_folder(self) -> List[Dict[str, Any]]:
        path_segment = f"/root:/{requests.utils.quote(self.site_config['folder_path'])}:" if self.site_config['folder_path'] else "/root"
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}{path_segment}/children"
        return self._api_request('get', url).get("value", [])

    def read_excel_sheet(self, file_id: str, file_name: str) -> pd.DataFrame | None:
        try:
            url_item = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}"
            download_url = self._api_request('get', url_item).get('@microsoft.graph.downloadUrl')
            if not download_url: return None
            
            response_content = requests.get(download_url, timeout=60)
            xls = pd.ExcelFile(io.BytesIO(response_content.content))
            sheet_name_to_find = self.site_config['sheet_name'].lower()
            actual_sheet_name = next((s for s in xls.sheet_names if s.lower() == sheet_name_to_find), None)
            
            if actual_sheet_name:
                df_full = pd.read_excel(xls, sheet_name=actual_sheet_name, header=None)
                start_row_index = -1
                search_text = "controle de tanque"
                for index, row in df_full.iterrows():
                    if any(search_text in str(cell).lower() for cell in row if pd.notna(cell)):
                        start_row_index = index
                        break
                if start_row_index == -1: return None
                df_data = df_full.iloc[start_row_index + 1:].copy()
                new_header = df_data.iloc[0]
                df_data = df_data[1:]
                df_data.columns = new_header
                df_data.reset_index(drop=True, inplace=True)
                stop_row_index = next((index for index, row in df_data.iterrows() if any('disponivel para' in str(cell).lower() or 'pedidos em tela' in str(cell).lower() for cell in row)), -1)
                df_final = df_data.iloc[:stop_row_index] if stop_row_index != -1 else df_data
                return df_final.dropna(axis=1, how='all')
            return None
        except Exception as e:
            logging.error(f"Falha ao ler o arquivo {file_name}. Erro: {e}")
            return None

    def read_sharepoint_history(self, file_path: str, sheet_name: str) -> pd.DataFrame:
        """Lê o histórico existente no arquivo de destino do SharePoint."""
        logging.info(f"Buscando histórico existente em: {file_path}")
        try:
            item = self.get_item_by_path(file_path)
            download_url = item.get('@microsoft.graph.downloadUrl')
            if not download_url: return pd.DataFrame()

            response = requests.get(download_url, timeout=60)
            xls = pd.ExcelFile(io.BytesIO(response.content))
            
            if sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                df.columns = [str(c).upper().strip() for c in df.columns]
                return df
            return pd.DataFrame()
        except Exception as e:
            logging.warning(f"Histórico não encontrado ou inacessível: {e}")
            return pd.DataFrame()

    def get_item_by_path(self, item_path: str) -> Dict:
        folder_prefix = f"/{self.site_config['folder_path']}" if self.site_config.get('folder_path') else ""
        full_path = f"/root:{folder_prefix}/{item_path}"
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}{full_path}"
        return self._api_request('get', url)

    def _convert_to_excel_col(self, n: int) -> str:
        result = ''
        while n >= 0:
            result = chr(n % 26 + ord('A')) + result
            n = n // 26 - 1
        return result

    def overwrite_sheet_with_dataframe(self, file_path: str, sheet_name: str, df: pd.DataFrame):
        logging.info(f"--- Gravando no SharePoint: {file_path} (Aba: {sheet_name}) ---")
        try:
            item = self.get_item_by_path(file_path)
            file_id = item['id']
            
            url_sheets = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets"
            existing_sheets = self._api_request('get', url_sheets).get('value', [])
            target_sheet_exists = any(s['name'].lower() == sheet_name.lower() for s in existing_sheets)
            
            if not target_sheet_exists:
                self._api_request('post', url_sheets, json={'name': sheet_name})
            else:
                url_clear = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range/clear"
                self._api_request('post', url_clear, json={'applyTo': 'contents'})

            if df.empty: return

            df_clean = df.copy()
            df_clean.columns = [str(c) if pd.notna(c) else "" for c in df_clean.columns]
            header_list = list(df_clean.columns)
            
            # Limpeza de valores para o formato Excel Online
            values = [header_list]
            for row in df_clean.values.tolist():
                clean_row = []
                for cell in row:
                    val = str(cell) if pd.notna(cell) else ""
                    val = val.replace('.0', '') if val.endswith('.0') else val
                    clean_row.append(val)
                values.append(clean_row)

            num_rows, num_cols = len(values), len(values[0])
            end_col_letter = self._convert_to_excel_col(num_cols - 1)
            address = f"A1:{end_col_letter}{num_rows}"

            url_write = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range(address='{address}')"
            self._api_request('patch', url_write, json={'values': values})
            logging.info(f"✅ Gravado com sucesso.")
        except Exception as e:
            logging.error(f"❌ Erro ao gravar SharePoint: {e}")

# ==============================================================================
# 4. FUNÇÕES DE PROCESSAMENTO
# ==============================================================================

def coletar_dados_do_datalake(source_config: Dict[str, Any], general_config: Config) -> pd.DataFrame:
    logging.info(f"--- Iniciando coleta da fonte: {source_config['name']} ---")
    sp_client = SharePointClient(source_config, general_config)
    all_items = sp_client.get_files_in_folder()
    
    if not all_items:
        return pd.DataFrame()
        
    list_of_dataframes = []
    files_to_process = [item for item in all_items if "file" in item and not any(k in item['name'].lower() for k in general_config.KEYWORDS_TO_EXCLUDE)]

    for item in files_to_process:
        df = sp_client.read_excel_sheet(item['id'], item['name'])
        
        if df is not None and not df.empty:
            # --- SOLUÇÃO PARA O ERRO: Remover duplicatas de colunas antes da concatenação ---
            # 1. Forçar nomes para string e Maiúsculo para padronizar
            df.columns = [str(c).strip().upper() for c in df.columns]
            
            # 2. Identificar colunas duplicadas e manter apenas a primeira ocorrência
            if not df.columns.is_unique:
                logging.warning(f"⚠️ Colunas duplicadas detectadas no arquivo {item['name']}. Limpando...")
                df = df.loc[:, ~df.columns.duplicated()]
            
            mapped_name = next((v for k, v in general_config.FILENAME_MAP.items() if k.lower() in item['name'].lower()), item['name'])
            df['Origem'] = mapped_name
            list_of_dataframes.append(df)
            
    if not list_of_dataframes:
        return pd.DataFrame()

    # Agora a concatenação será segura, pois todos os dfs têm colunas únicas
    return pd.concat(list_of_dataframes, ignore_index=True)

def aplicar_de_para_empresa(df: pd.DataFrame) -> pd.DataFrame:
    if 'EMPRESA' not in df.columns: return df
    try:
        home_dir = os.path.expanduser('~')
        caminho_de_para = os.path.join(home_dir, 'Documentos', 'De Para', 'Empresa.csv')
        df_de_para = pd.read_csv(caminho_de_para, sep=';', encoding='latin-1')
        df_de_para.columns = df_de_para.columns.str.strip()
        df_merged = pd.merge(df, df_de_para[['De', '2_EMPRESA']], left_on='EMPRESA', right_on='De', how='left')
        df_merged['EMPRESA'] = df_merged['2_EMPRESA'].fillna(df_merged['EMPRESA'])
        return df_merged.drop(columns=['De', '2_EMPRESA'])
    except:
        return df

# ==============================================================================
# 5. EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    try:
        logging.info("--- INICIANDO PROCESSO SHAREPOINT (COM HISTÓRICO) ---")
        Config.validate()
        
        # 1. Coleta dados novos
        df_novos = coletar_dados_do_datalake(Config.DATALAKE_CONFIG, Config)
        if df_novos.empty:
            logging.warning("Nenhum dado novo encontrado no DataLake.")
            return

        # 2. Tratamento Inicial dos novos dados
        df_novos.columns = [str(c).upper().strip() for c in df_novos.columns]
        df_novos = df_novos.loc[:, ~df_novos.columns.duplicated()]
        
        cols_to_fill = ['TANQUE', 'PRODUTO']
        for col in cols_to_fill:
            if col in df_novos.columns:
                df_novos[col] = df_novos[col].replace(r'^\s*(-)?\s*$', np.nan, regex=True).ffill()
        
        if 'EMPRESA' in df_novos.columns:
            df_novos['EMPRESA'] = df_novos['EMPRESA'].astype(str)
            df_novos = df_novos[~df_novos['EMPRESA'].str.strip().isin(['-', '', 'nan'])]
        
        df_novos.drop(columns=['TANQUE'], inplace=True, errors='ignore')

        today_str = date.today().strftime('%Y-%m-%d')
        df_novos['DATA_ATUALIZACAO'] = today_str
        df_novos['DATA_HORA_EXECUCAO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # 3. Gestão de Histórico no SharePoint
        sp_writer = SharePointClient(Config.DESTINATION_CONFIG, Config)
        df_historico_antigo = sp_writer.read_sharepoint_history(Config.SHAREPOINT_DEST_FILE, Config.SHAREPOINT_DEST_SHEET)

        if not df_historico_antigo.empty:
            # Limpeza de duplicatas: Remove do histórico o que já existe com a data de hoje
            if 'DATA_ATUALIZACAO' in df_historico_antigo.columns:
                df_historico_antigo['DATA_ATUALIZACAO'] = pd.to_datetime(df_historico_antigo['DATA_ATUALIZACAO']).dt.strftime('%Y-%m-%d')
                df_historico_preservado = df_historico_antigo[df_historico_antigo['DATA_ATUALIZACAO'] != today_str]
            else:
                df_historico_preservado = df_historico_antigo
            
            df_final = pd.concat([df_historico_preservado, df_novos], ignore_index=True)
            logging.info(f"📊 Histórico preservado: {len(df_historico_preservado)} linhas.")
        else:
            df_final = df_novos

        # 4. Filtros Finais e DE-PARA
        df_final = df_final.loc[:, ~df_final.columns.str.contains('^UNNAMED', case=False, na=False)]
        if 'PRODUTO' in df_final.columns:
            df_final = df_final[~df_final['PRODUTO'].astype(str).str.strip().str.upper().isin(Config.PRODUTOS_EXCLUIDOS)]
        
        df_final = aplicar_de_para_empresa(df_final)

        # 5. Salva Consolidado e Atual
        sp_writer.overwrite_sheet_with_dataframe(Config.SHAREPOINT_DEST_FILE, Config.SHAREPOINT_DEST_SHEET, df_final)
        
        df_atual = df_final[df_final['DATA_ATUALIZACAO'] == today_str].copy()
        sp_writer.overwrite_sheet_with_dataframe(Config.SHAREPOINT_DEST_FILE, "Atual", df_atual)

        logging.info("✅ Sucesso total! Processo finalizado apenas no SharePoint.")

    except Exception as e:
        logging.critical(f"❌ ERRO CRÍTICO: {e}", exc_info=True)

if __name__ == "__main__":
    main()