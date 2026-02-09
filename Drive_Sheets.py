# -*- coding: utf-8 -*-
"""
Este script executa um processo de três fases:
1. COLETA: Conecta-se ao SharePoint (Descargas) e ao Google
Sheets (Transportes).
2. CRUZAMENTO E ATUALIZAÇÃO: Executa uma lógica de
cruzamento "inteligente".
3. ATUALIZAÇÃO E RELATÓRIO: Atualiza os dados no Google
Sheets e gera um relatório de divergências.
"""

import re
import os
import io
import sys
import time
import logging
import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Set
import warnings

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
load_dotenv()

class Config:
    """⚙️ Centraliza todas as configurações e parâmetros da aplicação."""
    TENANT_ID: str = os.getenv("TENANT_ID")
    CLIENT_ID: str = os.getenv("CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
    HOSTNAME: str = os.getenv("HOSTNAME")

    # --- NOVA CONFIGURAÇÃO: MAPA DE CORREÇÃO DE PRODUTOS ---
    # Corrige nomes errados vindos do DataLake antes do processamento
    PRODUCT_CORRECTIONS: Dict[str, str] = {
        "gasolina c comum": "gasolina c",
        "gasolina c aditivada": "gasolina c",
        "oleo diesel b s10": "diesel b s10"
    }
    # -------------------------------------------------------

    DESCARGAS_CONFIG: Dict[str, Any] = {
        "name": "Descargas DataLake",
        "site_path": "/sites/DataLake",
        "drive_name": "Documentos",
        "folder_path": "Bases",
        "sheet_name": "Descarga",
        "header": ['faturista', 'produto', 'origem', 'empresa', 'data', 'hora', 'placa', 'motorista', 'nota', 'quantidade_nf', 'op_tanque', 'aditivar', 'aditivo', 'dias_em_espera', 'status', 'data_de_descarga', 'hr_entrada'],
        "products_to_include": ['anidro', 'hidratado', 'biodiesel', 'gasolina a', 'gasolina c', 'diesel a s10', 'diesel b s10', 'diesel a s500', 'diesel b s500', 'mgo']
    }

    GOOGLE_SHEETS_CONFIG: Dict[str, Any] = {
        "name": "Transportes",
        "credentials_path": r"C:\Users\Planejamento\Documents\Drive\credenciais.json",
        "sheet_urls": [
            "https://docs.google.com/spreadsheets/d/1bu3CR46-D62laZyUcxuTlookn0kjEsJjvv0frzjJ03c/edit?usp=sharing", #Diesel
            "https://docs.google.com/spreadsheets/d/1K22quWCg2XTpfgenx-n5nQ5efDr6_ntcyX3iCExJ958/edit?usp=sharing" #Gasolina
        ],
        "sheet_name_to_read": "Base",
        "header": ["sm", "data_prev_carregamento", "expedidor", "cidade_origem", "ufo", "destinatario", "recebedor", "cidade_destino", "ufd", "produto", "motorista", "cavalo", "carreta1", "carreta2", "transportadora", "nfe", "volume_l", "data_de_carregamento", "horario_de_carregamento", "data_chegada", "data_descarga", "status"],
        "status_para_incluir": ["Em Trânsito", "Aguardando Descarga", "Em Trânsito By Pass", "Aguardando By Pass"]
    }
    
    RELATORIO_DIVERGENCIA_CONFIG: Dict[str, Any] = {
        "url": "https://docs.google.com/spreadsheets/d/1Il5VDZUuEbKVf78ne1QdRqbRyHrd1EFXc3VRhU-2Wmo/edit?usp=sharing",
        "sheet_name": "Página1"
    }

    KEYWORDS_TO_EXCLUDE: List[str] = ["backup", "modelo", "corrompida", "corrompido", "dinamica"]
    FILENAME_MAP: Dict[str, str] = {
        "ARUJA": "Aruja", "BARRA_MANSA": "Barra Mansa", "BCAG": "BCAG", "CAVALINI": "Cavalini", "CROSS": "Cross Terminais", "DIRECIONAL_FILIAL": "Direcional Filial",
        "DIRECIONAL_MATRIZ": "Direcional Matriz", "FLAG": "Flag", "GRANEL_QUIMICA": "Granel Química", "MANGUINHOS": "Caxias", "PETRONORTE": "Petronorte", "REFIT_BASE": "Refit",
        "RODOPETRO_CAPIVARI": "Capivari", "SANTOS_BRASIL": "Santos Brasil", "SGP": "SGP", "STOCK": "Stock", "STOCKMAT": "Stockmat",
        "TIF": "TIF", "TLIQ": "Tliq", "TRANSO": "Transo",
        "TRR_AB": "Americo", "TRR_CATANDUVA": "Catanduva", "VAISHIA": "Vaishia"
    }

    @staticmethod
    def validate():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET]):
            raise ValueError("❌ Faltam credenciais do SharePoint no arquivo .env.")
        if not os.path.exists(Config.GOOGLE_SHEETS_CONFIG["credentials_path"]):
            raise ValueError("❌ Arquivo de credenciais do Google Sheets não encontrado.")
        logging.info("Configurações de ambiente carregadas.")

class SharePointClient:
    def __init__(self, site_config: Dict[str, Any]):
        self.site_config = site_config
        self.access_token = self._get_access_token()
        self.site_id = self._get_site_id()
        self.drive_id = self._get_drive_id()
    def _api_request(self, method: str, url: str, json: Dict = None) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.request(method, url, headers=headers, json=json)
        response.raise_for_status()
        return response.json() if response.content else None
    def _get_access_token(self) -> str:
        url = f"https://login.microsoftonline.com/{Config.TENANT_ID}/oauth2/v2.0/token"
        data = {"client_id": Config.CLIENT_ID, "scope": "https://graph.microsoft.com/.default", "client_secret": Config.CLIENT_SECRET, "grant_type": "client_credentials"}
        response = requests.post(url, data=data)
        response.raise_for_status()
        return response.json()["access_token"]
    def _get_site_id(self) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{Config.HOSTNAME}:{self.site_config['site_path']}"
        return self._api_request('get', url)["id"]
    def _get_drive_id(self) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        drives = self._api_request('get', url).get("value", [])
        drive_name_lower = self.site_config['drive_name'].lower()
        for drive in drives:
            if drive['name'].lower() == drive_name_lower:
                return drive['id']
        raise FileNotFoundError(f"Biblioteca '{self.site_config['drive_name']}' não encontrada.")
    def get_files_in_folder(self) -> List[Dict[str, Any]]:
        path_segment = f"/root:/{requests.utils.quote(self.site_config['folder_path'])}:" if self.site_config['folder_path'] else "/root"
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}{path_segment}/children"
        return self._api_request('get', url).get("value", [])
    def read_excel_sheet(self, file_id: str, file_name: str) -> pd.DataFrame | None:
        try:
            url_item = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}"
            download_url = self._api_request('get', url_item).get('@microsoft.graph.downloadUrl')
            if not download_url:
                return None
            response_content = requests.get(download_url, timeout=60)
            response_content.raise_for_status()
            xls = pd.ExcelFile(io.BytesIO(response_content.content))
            sheet_name_to_find = self.site_config['sheet_name'].lower()
            actual_sheet_name = next((s for s in xls.sheet_names if s.lower() == sheet_name_to_find), None)
            if actual_sheet_name:
                df = pd.read_excel(xls, sheet_name=actual_sheet_name, header=None)
                header_list = self.site_config['header']
                header_keyword = 'produto'
                for i, row in df.head(15).iterrows():
                    if any(str(cell).strip().lower() == header_keyword for cell in row):
                        df_data = df.iloc[i + 1:].copy()
                        num_expected_cols = len(header_list)
                        df_data = df_data.iloc[:, :num_expected_cols]
                        df_data.columns = header_list
                        return df_data.reset_index(drop=True)
            return None
        except Exception as e:
            logging.error(f"Falha ao ler o arquivo {file_name} (ID: {file_id}). Erro: {e}")
            return None

class GoogleSheetsClient:
    def __init__(self, credentials_path: str):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
            self.client = gspread.authorize(creds)
            logging.info("✅ Autenticação com a API do Google Sheets bem-sucedida.")
        except Exception as e:
            logging.error(f"❌ Falha na autenticação com o Google Sheets: {e}")
            raise
    def get_data_as_dataframe(self, spreadsheet_url: str, sheet_name: str, header_config: list) -> pd.DataFrame:
        try:
            spreadsheet = self.client.open_by_url(spreadsheet_url)
            sheet = spreadsheet.worksheet(sheet_name)
            all_data = sheet.get_all_values()
            header_keyword = 'sm'
            header_row_index = -1
            for i, row in enumerate(all_data[:15]):
                if header_keyword in [str(cell).strip().lower() for cell in row]:
                    header_row_index = i
                    break
            if header_row_index == -1:
                logging.warning(f"Cabeçalho 'sm' não encontrado em '{spreadsheet.title}'.")
                return pd.DataFrame()
            df = pd.DataFrame(all_data[header_row_index + 1:])
            num_expected_cols = len(header_config)
            if df.shape[1] < num_expected_cols:
                df = df.reindex(columns=range(num_expected_cols))
            df = df.iloc[:, :num_expected_cols]
            df.columns = header_config
            df['__gs_url'] = spreadsheet_url
            df['__gs_sheet_name'] = sheet_name
            df['__gs_row_index'] = df.index + header_row_index + 2
            logging.info(f"Dados lidos da planilha '{spreadsheet.title}', aba '{sheet_name}'.")
            return df
        except gspread.exceptions.WorksheetNotFound:
            logging.error(f"Aba '{sheet_name}' não encontrada.")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Erro ao ler dados do Google Sheets: {e}")
            return pd.DataFrame()
    def update_status_banner(self, spreadsheet_url: str, sheet_name: str, message: str, color: str):
        try:
            spreadsheet = self.client.open_by_url(spreadsheet_url)
            sheet = spreadsheet.worksheet(sheet_name)
            colors = {"RED": {"red": 0.9, "green": 0.2, "blue": 0.2}, "YELLOW": {"red": 1.0, "green": 0.9, "blue": 0.4}, "GREEN": {"red": 0.2, "green": 0.7, "blue": 0.2}}
            format_request = {"repeatCell": {"range": {"sheetId": sheet.id, "startRowIndex": 0, "endRowIndex": 1, "startColumnIndex": 0, "endColumnIndex": 22}, "cell": {"userEnteredFormat": {"backgroundColor": colors.get(color, {"red": 1})}}, "fields": "userEnteredFormat.backgroundColor"}}
            spreadsheet.batch_update({"requests": [format_request]})
            sheet.update_acell('A1', message)
            logging.info(f"Painel de status atualizado em '{spreadsheet.title}': {message}")
        except Exception as e:
            logging.error(f"Erro ao atualizar o painel de status no Google Sheets: {e}")
    def batch_update_data(self, spreadsheet_url: str, sheet_name: str, updates_df: pd.DataFrame, header_config: list):
        try:
            spreadsheet = self.client.open_by_url(spreadsheet_url)
            sheet = spreadsheet.worksheet(sheet_name)
            cell_updates = []
            cols_to_update = ['status', 'data_chegada', 'data_descarga']
            for _, row in updates_df.iterrows():
                row_idx = row['__gs_row_index']
                for col_name in cols_to_update:
                    if col_name in row and pd.notna(row[col_name]):
                        value = row[col_name]
                        if isinstance(value, (datetime, pd.Timestamp)):
                            value = value.strftime('%d/%m/%Y')
                        col_idx = header_config.index(col_name) + 1
                        cell = gspread.Cell(row=row_idx, col=col_idx, value=str(value))
                        cell_updates.append(cell)
            if cell_updates:
                sheet.update_cells(cell_updates, value_input_option='USER_ENTERED')
                logging.info(f"{len(cell_updates)} células de dados atualizadas em lote em '{spreadsheet.title}'.")
        except Exception as e:
            logging.error(f"Erro na atualização em lote de dados no Google Sheets: {e}")
    def clear_and_write_dataframe(self, spreadsheet_url: str, sheet_name: str, df_to_write: pd.DataFrame):
            try:
                spreadsheet = self.client.open_by_url(spreadsheet_url)
                sheet = spreadsheet.worksheet(sheet_name)
                logging.info(f"Limpando a planilha de relatório '{spreadsheet.title}', aba '{sheet_name}'...")
                sheet.clear()
                
                df_safe = df_to_write.fillna('')
                df_safe = df_safe.astype(str)
                
                data_to_write = [df_safe.columns.values.tolist()] + df_safe.values.tolist()
                
                logging.info(f"Escrevendo {len(df_to_write)} linhas de divergência no relatório...")
                
                sheet.update(range_name='A1', values=data_to_write, value_input_option='USER_ENTERED')
                
                logging.info("Relatório de divergências atualizado com sucesso.")
            except Exception as e:
                logging.error(f"Falha ao atualizar o relatório de divergências: {e}", exc_info=True)
                raise

def carregar_dados_sharepoint(source_config: Dict[str, Any]) -> pd.DataFrame:
    logging.info(f"--- Iniciando coleta da fonte SharePoint: {source_config['name']} ---")
    sp_client = SharePointClient(source_config)
    all_items = sp_client.get_files_in_folder()
    if not all_items:
        return pd.DataFrame()
    list_of_dataframes = []
    for item in all_items:
        if "folder" in item:
            continue
        file_name = item['name']
        if any(keyword in file_name.lower() for keyword in Config.KEYWORDS_TO_EXCLUDE):
            continue
        df = sp_client.read_excel_sheet(item['id'], file_name)
        if df is not None and not df.empty:
            df['Fonte do Arquivo'] = file_name
            list_of_dataframes.append(df)
    if not list_of_dataframes:
        return pd.DataFrame()
    consolidated_df = pd.concat(list_of_dataframes, ignore_index=True)
    logging.info(f"Fonte '{source_config['name']}' consolidada. Total de {len(consolidated_df)} linhas brutas.")
    return consolidated_df

def carregar_dados_google_sheets(source_config: Dict[str, Any]) -> pd.DataFrame:
    logging.info(f"--- Iniciando coleta da fonte Google Sheets: {source_config['name']} ---")
    gs_client = GoogleSheetsClient(source_config["credentials_path"])
    list_of_dataframes = []
    all_sheets_data = pd.DataFrame() # DataFrame para armazenar todos os dados brutos
    
    for url in source_config["sheet_urls"]:
        df = gs_client.get_data_as_dataframe(url, source_config["sheet_name_to_read"], source_config["header"])
        if not df.empty:
            list_of_dataframes.append(df)

    if not list_of_dataframes:
        logging.warning("Nenhum dado válido extraído das planilhas Google.")
        return pd.DataFrame()
        
    all_sheets_data = pd.concat(list_of_dataframes, ignore_index=True)
    logging.info(f"Fonte '{source_config['name']}' consolidada. Total de {len(all_sheets_data)} linhas brutas.")
    return all_sheets_data

def _normalizar_texto_para_chave(series: Any) -> Any:
    if isinstance(series, pd.Series):
        return series.astype(str).str.strip().str.lower().str.replace('[^a-z0-9]', '', regex=True)
    else:
        return re.sub('[^a-z0-9]', '', str(series).strip().lower())

def processar_dados_descargas(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    logging.info("Processando dados de Descargas (Filtros e Chaves)...")
    
    df = df.reset_index(drop=True)
    
    logging.info("Verificando e tratando notas fiscais múltiplas...")
    df['nota'] = df['nota'].astype(str)
    df['nota'] = df['nota'].str.split('/')
    df = df.explode('nota')
    df['nota'] = df['nota'].str.strip()

    df = df.reset_index(drop=True)
    logging.info(f"DataFrame de descargas expandido para {len(df)} linhas após tratar múltiplas notas.")
    
    # --- INÍCIO DA MELHORIA 2: Excluir produtos com 'devolvido', 'devolução' ou 'cancelado' ---
    palavras_a_excluir = ['devolvido', 'devolução', 'cancelado']
    logging.info(f"Removendo produtos que contenham: {palavras_a_excluir}")
    df = df[~df['produto'].astype(str).str.contains('|'.join(palavras_a_excluir), case=False, na=False)]
    # --- FIM DA MELHORIA 2 ---

    # =========================================================================
    # --- NOVA LÓGICA DE CORREÇÃO DE NOMES DE PRODUTOS ---
    # =========================================================================
    logging.info("Padronizando nomes de produtos (Correção De/Para)...")
    
    # 1. Normaliza para minúsculo e remove espaços nas pontas
    df['produto'] = df['produto'].astype(str).str.strip().str.lower()
    
    # 2. Aplica o mapa de substituição definido na Config
    df['produto'] = df['produto'].replace(Config.PRODUCT_CORRECTIONS)
    # =========================================================================

    df = df[df['produto'].astype(str).str.lower().isin(Config.DESCARGAS_CONFIG['products_to_include'])]
    data_limite = datetime.now() - timedelta(days=20)
    df['data'] = pd.to_datetime(df['data'], errors='coerce', dayfirst=True)
    df = df[df['data'].notna() & (df['data'] > data_limite)]
    
    logging.info("Aplicando regra 'de-para' na fonte do arquivo para padronização.")
    df['Fonte Padronizada'] = df['Fonte do Arquivo']
    for key, value in Config.FILENAME_MAP.items():
        mask = df['Fonte do Arquivo'].astype(str).str.contains(key, case=False, na=False)
        df.loc[mask, 'Fonte Padronizada'] = value
    
    # --- INÍCIO DA MELHORIA 1: Atualização da chave primária de Descargas ---
    df['chave_primaria'] = (_normalizar_texto_para_chave(df['nota']) + '_' +
                              _normalizar_texto_para_chave(df['produto']) + '_' +
                              _normalizar_texto_para_chave(df['Fonte Padronizada']))
    # --- FIM DA MELHORIA 1 ---

    # Mantendo a chave secundária para a Etapa 2 do cruzamento
    df['chave_placa_fonte_produto'] = (_normalizar_texto_para_chave(df['placa']) + '_' +
                                       _normalizar_texto_para_chave(df['produto']) + '_' +
                                       _normalizar_texto_para_chave(df['Fonte Padronizada']))
    return df

def processar_dados_transportes(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    logging.info("Processando dados de Transportes (Filtros e Chaves)...")
    cfg = Config.GOOGLE_SHEETS_CONFIG
    df['status'] = df['status'].astype(str)
    status_validos = [s.lower() for s in cfg['status_para_incluir']]
    df_filtrado = df[df['status'].str.lower().isin(status_validos)].copy()
    
    if df_filtrado.empty:
        return pd.DataFrame()
    
    # --- INÍCIO DA MELHORIA 1: Atualização da chave primária de Transportes ---
    df_filtrado['chave_primaria'] = (_normalizar_texto_para_chave(df_filtrado['nfe']) + '_' +
                                       _normalizar_texto_para_chave(df_filtrado['produto']) + '_' +
                                       _normalizar_texto_para_chave(df_filtrado['recebedor']))
    # --- FIM DA MELHORIA 1 ---
    
    # Mantendo a chave secundária para a Etapa 2 do cruzamento
    df_filtrado['chave_placa_recebedor_produto'] = (_normalizar_texto_para_chave(df_filtrado['cavalo']) + '_' +
                                                    _normalizar_texto_para_chave(df_filtrado['produto']) + '_' +
                                                    _normalizar_texto_para_chave(df_filtrado['recebedor']))
    df_filtrado['data_de_carregamento'] = pd.to_datetime(df_filtrado['data_de_carregamento'], errors='coerce', dayfirst=True)
    return df_filtrado

def cruzar_e_atualizar_transportes(df_transportes: pd.DataFrame, df_descargas: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int], Set[int]]:
    if not isinstance(df_transportes, pd.DataFrame):
        df_transportes = pd.DataFrame()
    transportes_atualizado = df_transportes.copy()
    indices_descargas_usados = set()
    if not transportes_atualizado.empty:
        transportes_atualizado['fonte_atualizacao'] = 'Não Atualizado'
    if transportes_atualizado.empty or df_descargas.empty:
        logging.warning("Uma das fontes de dados está vazia, pulando a etapa de cruzamento.")
        return transportes_atualizado, {}, indices_descargas_usados
    
    contadores = {"etapa1_transito_para_aguardando": 0, "etapa1_transito_para_descarregado": 0, "etapa1_aguardando_para_descarregado": 0, "etapa2_transito_para_aguardando": 0, "etapa2_transito_para_descarregado": 0, "etapa2_aguardando_para_descarregado": 0}
    
    descargas_com_indice = df_descargas.copy()
    descargas_com_indice['original_index'] = descargas_com_indice.index
    
    # --- INÍCIO DA MELHORIA 1: Usando a nova chave primária para o cruzamento ---
    descargas_unicas = descargas_com_indice.drop_duplicates(subset=['chave_primaria'], keep='last')
    mapa_chaves = descargas_unicas.set_index('chave_primaria').to_dict('index')

    for index, transporte in transportes_atualizado.iterrows():
        match = mapa_chaves.get(transporte['chave_primaria'])
    # --- FIM DA MELHORIA 1 ---
        if match:
            indices_descargas_usados.add(match['original_index'])
            
            status_original = str(transporte['status']).lower()
            novo_status = 'aguardando descarga' if pd.isna(match.get('data_de_descarga')) else 'descarregado'
            
            if novo_status != status_original:
                transportes_atualizado.loc[index, 'data_chegada'] = match['data']
                transportes_atualizado.loc[index, 'fonte_atualizacao'] = 'Etapa 1 - Chave Primária'
                if 'trânsito' in status_original and novo_status == 'aguardando descarga':
                    transportes_atualizado.loc[index, 'status'] = 'AGUARDANDO DESCARGA'
                    contadores['etapa1_transito_para_aguardando'] += 1
                elif 'trânsito' in status_original and novo_status == 'descarregado':
                    transportes_atualizado.loc[index, 'status'] = 'DESCARREGADO'
                    transportes_atualizado.loc[index, 'data_descarga'] = match['data_de_descarga']
                    contadores['etapa1_transito_para_descarregado'] += 1
                elif 'aguardando' in status_original and novo_status == 'descarregado':
                    transportes_atualizado.loc[index, 'status'] = 'DESCARREGADO'
                    transportes_atualizado.loc[index, 'data_descarga'] = match['data_de_descarga']
                    contadores['etapa1_aguardando_para_descarregado'] += 1

    descargas_unicas_placa = descargas_com_indice.drop_duplicates(subset=['chave_placa_fonte_produto'], keep='last')
    mapa_placa = descargas_unicas_placa.set_index('chave_placa_fonte_produto').to_dict('index')
    
    transportes_para_etapa_2 = transportes_atualizado[transportes_atualizado['fonte_atualizacao'] == 'Não Atualizado']
    for index, transporte in transportes_para_etapa_2.iterrows():
        chave_busca_placa = _normalizar_texto_para_chave(transporte['cavalo']) + '_' + _normalizar_texto_para_chave(transporte['produto']) + '_' + _normalizar_texto_para_chave(transporte['recebedor'])
        match = mapa_placa.get(chave_busca_placa)
        if match:
            if pd.notna(transporte['data_de_carregamento']) and pd.to_datetime(match['data']) >= transporte['data_de_carregamento']:
                indices_descargas_usados.add(match['original_index'])
                
                status_original = str(transporte['status']).lower()
                novo_status = 'aguardando descarga' if pd.isna(match.get('data_de_descarga')) else 'descarregado'
                if novo_status != status_original:
                    transportes_atualizado.loc[index, 'data_chegada'] = match['data']
                    transportes_atualizado.loc[index, 'fonte_atualizacao'] = 'Etapa 2 - Placa'
                    if 'trânsito' in status_original and novo_status == 'aguardando descarga':
                        transportes_atualizado.loc[index, 'status'] = 'AGUARDANDO DESCARGA'
                        contadores['etapa2_transito_para_aguardando'] += 1
                    elif 'trânsito' in status_original and novo_status == 'descarregado':
                        transportes_atualizado.loc[index, 'status'] = 'DESCARREGADO'
                        transportes_atualizado.loc[index, 'data_descarga'] = match['data_de_descarga']
                        contadores['etapa2_transito_para_descarregado'] += 1
                    elif 'aguardando' in status_original and novo_status == 'descarregado':
                        transportes_atualizado.loc[index, 'status'] = 'DESCARREGADO'
                        transportes_atualizado.loc[index, 'data_descarga'] = match['data_de_descarga']
                        contadores['etapa2_aguardando_para_descarregado'] += 1
    
    return transportes_atualizado, contadores, indices_descargas_usados
    
def main():
    try:
        logging.info("--- INICIANDO PROCESSO COMPLETO DE COLETA E ATUALIZAÇÃO ---")
        Config.validate()
        
        raw_descargas = carregar_dados_sharepoint(Config.DESCARGAS_CONFIG)
        raw_transportes = carregar_dados_google_sheets(Config.GOOGLE_SHEETS_CONFIG)
        
        df_descargas = processar_dados_descargas(raw_descargas) if not raw_descargas.empty else pd.DataFrame()
        df_transportes = processar_dados_transportes(raw_transportes) if not raw_transportes.empty else pd.DataFrame()
        
        df_transportes_final, contadores, indices_usados = cruzar_e_atualizar_transportes(df_transportes, df_descargas)
        
        df_updates = df_transportes_final[df_transportes_final['fonte_atualizacao'] != 'Não Atualizado']
        
        gs_client = GoogleSheetsClient(Config.GOOGLE_SHEETS_CONFIG["credentials_path"])
        
        # =========================================================================
        # INÍCIO DA SEÇÃO DE ATUALIZAÇÃO (LÓGICA CORRIGIDA)
        # =========================================================================
        
        processo_bem_sucedido = False
        urls_com_updates = set() # Usamos um set para consulta rápida

        if not df_updates.empty:
            urls_com_updates = set(df_updates['__gs_url'].unique())
            try:
                logging.info(f"Sinalizando {len(urls_com_updates)} planilha(s) Google como 'em atualização'...")
                for url in urls_com_updates:
                    gs_client.update_status_banner(url, Config.GOOGLE_SHEETS_CONFIG["sheet_name_to_read"], "Atualizando...", "RED")
                
                time.sleep(1)
                
                logging.info(f"Iniciando atualização de {len(df_updates)} registros no Google Sheets...")
                for url in urls_com_updates:
                    updates_neste_arquivo = df_updates[df_updates['__gs_url'] == url]
                    gs_client.batch_update_data(url, Config.GOOGLE_SHEETS_CONFIG["sheet_name_to_read"], updates_neste_arquivo, Config.GOOGLE_SHEETS_CONFIG["header"])
                
                logging.info("✅ SUCESSO! Atualizações de dados no Google Sheets concluídas.")
                processo_bem_sucedido = True
            except Exception as e:
                logging.critical(f"❌ ERRO DURANTE ATUALIZAÇÃO NO GOOGLE SHEETS: {e}", exc_info=True)
                for url in urls_com_updates:
                    gs_client.update_status_banner(url, Config.GOOGLE_SHEETS_CONFIG["sheet_name_to_read"], f"Falha na atualização. Detalhe: {str(e)[:150]}", "YELLOW")
                raise
        else:
            logging.info("Nenhuma atualização de status necessária.")
            processo_bem_sucedido = True # Se não há updates, o processo foi 'bem-sucedido'

        # Lógica final para atualizar TODOS os banners, tratando cada caso
        if processo_bem_sucedido:
            timestamp = datetime.now().strftime('%d/%m/%Y às %H:%M:%S')
            # Percorre TODAS as planilhas configuradas
            for url in Config.GOOGLE_SHEETS_CONFIG["sheet_urls"]:
                # Verifica se a planilha atual ESTÁ na lista daquelas que foram modificadas
                if url in urls_com_updates:
                    # Calcula os totais de alteração APENAS para esta planilha
                    updates_nesta_planilha = df_updates[df_updates['__gs_url'] == url]
                    
                    total_t_ad = (updates_nesta_planilha['status'] == 'AGUARDANDO DESCARGA').sum()
                    total_desc = (updates_nesta_planilha['status'] == 'DESCARREGADO').sum()

                    msg_sucesso = f"Atualizado em {timestamp}. Status alterados: {total_t_ad} (Trânsito -> Aguardando), {total_desc} (-> Descarregado)"
                    gs_client.update_status_banner(url, Config.GOOGLE_SHEETS_CONFIG["sheet_name_to_read"], msg_sucesso, "GREEN")
                else:
                    # Se a planilha não foi modificada, envia a mensagem de "sem alterações"
                    msg_sem_alteracao = f"Atualizado em {timestamp}. Nenhuma alteração de status nesta execução."
                    gs_client.update_status_banner(url, Config.GOOGLE_SHEETS_CONFIG["sheet_name_to_read"], msg_sem_alteracao, "GREEN")

        # =========================================================================
        # FIM DA SEÇÃO DE ATUALIZAÇÃO (LÓGICA CORRIGIDA)
        # =========================================================================
        
        # =========================================================================
        # INÍCIO DA SEÇÃO DE RELATÓRIO DE DIVERGÊNCIAS (COM LÓGICA ANTI-FALSO POSITIVO)
        # =========================================================================
        logging.info("Iniciando a geração do relatório de divergências...")
        if not df_descargas.empty:
            df_descargas_nao_usadas = df_descargas.drop(index=list(indices_usados))
            
            # PASSO 1: Criar a "lista de exceções" com base nos transportes já descarregados hoje
            hoje_str = datetime.now().strftime('%Y-%m-%d')
            raw_transportes['data_descarga'] = pd.to_datetime(raw_transportes['data_descarga'], errors='coerce', dayfirst=True)

            df_descarregados_hoje = raw_transportes[
                (raw_transportes['status'].str.lower() == 'descarregado') &
                (raw_transportes['data_descarga'].dt.strftime('%Y-%m-%d') == hoje_str)
            ].copy()

            chaves_excecao_primaria = set()
            chaves_excecao_placa = set()

            if not df_descarregados_hoje.empty:
                # --- INÍCIO DA MELHORIA 1: Usa a nova chave primária para a lista de exceções ---
                df_descarregados_hoje['chave_primaria'] = (_normalizar_texto_para_chave(df_descarregados_hoje['nfe']) + '_' +
                                                            _normalizar_texto_para_chave(df_descarregados_hoje['produto']) + '_' +
                                                            _normalizar_texto_para_chave(df_descarregados_hoje['recebedor']))
                chaves_excecao_primaria = set(df_descarregados_hoje['chave_primaria'])
                # --- FIM DA MELHORIA 1 ---
                
                # Cria a chave de exceção por PLACA (secundária)
                df_descarregados_hoje['chave_placa_recebedor_produto'] = (_normalizar_texto_para_chave(df_descarregados_hoje['cavalo']) + '_' +
                                                                            _normalizar_texto_para_chave(df_descarregados_hoje['produto']) + '_' +
                                                                            _normalizar_texto_para_chave(df_descarregados_hoje['recebedor']))
                chaves_excecao_placa = set(df_descarregados_hoje['chave_placa_recebedor_produto'])
                
                logging.info(f"Encontradas {len(chaves_excecao_primaria)} chaves de exceção primárias e {len(chaves_excecao_placa)} por placa para evitar falsos positivos.")
            else:
                logging.info("Nenhuma viagem previamente descarregada hoje. Nenhuma exceção será aplicada.")

            # PASSO 2: Filtragem das divergências, aplicando a exceção
            df_descargas_nao_usadas['data_de_descarga'] = pd.to_datetime(df_descargas_nao_usadas['data_de_descarga'], errors='coerce', dayfirst=True)
            hoje = datetime.now().date()
            
            filtro_data = (df_descargas_nao_usadas['data_de_descarga'].isna()) | (df_descargas_nao_usadas['data_de_descarga'].dt.date == hoje)
            df_relatorio = df_descargas_nao_usadas[filtro_data].copy()

            # --- INÍCIO DA MELHORIA 1: Aplica o filtro de exceção usando a nova chave primária ---
            if not df_relatorio.empty and (chaves_excecao_primaria or chaves_excecao_placa):
                logging.info("Aplicando filtro de exceção duplo (chave primária e placa) para remover falsos positivos...")
                
                # Compara a chave primária do relatório com a lista de exceção
                filtro_chave_primaria_valida = ~df_relatorio['chave_primaria'].isin(chaves_excecao_primaria)
                
                # Compara a chave de placa do relatório (fonte) com a lista de exceção de placas (recebedor)
                filtro_placa_valida = ~df_relatorio['chave_placa_fonte_produto'].isin(chaves_excecao_placa)
                
                # Uma divergência só é real se não for encontrada por NENHUMA das chaves de exceção
                df_relatorio = df_relatorio[filtro_chave_primaria_valida & filtro_placa_valida]
            # --- FIM DA MELHORIA 1 ---
            
            # PASSO 3: Filtro final de produto e escrita do relatório
            df_relatorio_final = pd.DataFrame()
            if not df_relatorio.empty:
                filtro_produto = (df_relatorio['produto'].str.contains('gasolina', case=False, na=False)) | \
                                 (df_relatorio['produto'].str.contains('diesel ', case=False, na=False))
                df_relatorio_final = df_relatorio[filtro_produto]

            if not df_relatorio_final.empty:
                logging.info(f"Encontradas {len(df_relatorio_final)} linhas para o relatório de divergências após todos os filtros.")
                colunas_relatorio = {
                    'data': 'Data Chegada', 'placa': 'Placa', 'motorista': 'Motorista', 'nota': 'Nota Fiscal', 
                    'produto': 'Produto', 'Fonte Padronizada': 'Origem', 'status': 'Status Descarga',
                    'data_de_descarga': 'Data de Descarga'
                }
                df_para_escrever = df_relatorio_final[list(colunas_relatorio.keys())].rename(columns=colunas_relatorio)
                gs_client.clear_and_write_dataframe(
                    Config.RELATORIO_DIVERGENCIA_CONFIG['url'],
                    Config.RELATORIO_DIVERGENCIA_CONFIG['sheet_name'],
                    df_para_escrever
                )
            else:
                logging.info("Nenhuma divergência encontrada para o relatório após todos os filtros.")
                gs_client.clear_and_write_dataframe(
                    Config.RELATORIO_DIVERGENCIA_CONFIG['url'],
                    Config.RELATORIO_DIVERGENCIA_CONFIG['sheet_name'],
                    pd.DataFrame(columns=[f"Nenhuma divergência encontrada na execução de {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}."])
                )
        else:
            logging.info("Fonte de Descargas vazia, pulando relatório de divergências.")
            
    except Exception as e:
        logging.critical(f"❌ PROCESSO INTERROMPIDO POR ERRO CRÍTICO: {e}", exc_info=False)
        sys.exit(1)

if __name__ == "__main__":
    main()