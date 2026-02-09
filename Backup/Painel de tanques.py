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
# ### [ALTERADO] Importando datetime para pegar o horário também
from datetime import date, datetime 

# --- BIBLIOTECAS PARA O GOOGLE SHEETS ---
import gspread
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from google.oauth2.service_account import Credentials

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
load_dotenv()

# ==============================================================================
# 2. AUTENTICAÇÃO GOOGLE
# ==============================================================================

def autenticar_google_sheets():
    """Autentica com a API do Google usando o arquivo de credenciais e retorna o cliente."""
    logging.info("--- Autenticando com a API do Google Sheets ---")
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        script_dir = os.path.dirname(os.path.abspath(__file__))
        creds_path = os.path.join(script_dir, 'credenciais.json')
        
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        client = gspread.authorize(creds)
        logging.info("✅ Autenticação com Google Sheets bem-sucedida.")
        return client
    except FileNotFoundError:
        logging.error(f"❌ ERRO: Arquivo de credenciais 'credenciais.json' não encontrado no diretório do script.")
        return None
    except Exception as e:
        logging.error(f"❌ ERRO durante a autenticação com Google: {e}")
        return None

# ==============================================================================
# 3. CONFIGURAÇÃO GERAL (SHAREPOINT E PARÂMETROS)
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
        "site_path": "/sites/DataLake",   # <--- Site de Origem
        "drive_name": "Documentos",
        "folder_path": "Bases",
        "sheet_name": "PAINEL DE TANQUES",
    }
    
    # --- CONFIGURAÇÃO DE ESCRITA (DESTINO: Site Transportes / Folder Disponibilidade) ---
    DESTINATION_CONFIG: Dict[str, Any] = {
        "name": "Site Transportes Escrita",
        "site_path": "/sites/Transportes", # <--- Site de Destino
        "drive_name": "Documentos",
        "folder_path": "Disponibilidade",  # <--- Pasta de Destino
        "sheet_name": "Base_Consolidada",  # Apenas referência
    }
    
    # Nome do arquivo que será salvo dentro da pasta 'Disponibilidade' no site 'Transportes'
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
    
    # --- [NOVO] LISTA DE PRODUTOS PARA EXCLUIR ---
    PRODUTOS_EXCLUIDOS: List[str] = ["GAS C", "B100", "AS10", "AS500","GAS, C"]

    @staticmethod
    def validate():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET]):
            raise ValueError("❌ Faltam credenciais no arquivo .env.")
        logging.info("Credenciais de ambiente para SharePoint carregadas com sucesso.")

# ==============================================================================
# 4. CLIENTE SHAREPOINT (COM CAPACIDADE DE ESCRITA ADICIONADA)
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
        # Pega o site_path da configuração específica (DataLake ou Transportes)
        url = f"https://graph.microsoft.com/v1.0/sites/{self.config.HOSTNAME}:{self.site_config['site_path']}"
        return self._api_request('get', url)["id"]

    def _get_drive_id(self) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        drives = self._api_request('get', url).get("value", [])
        drive_name_lower = self.site_config['drive_name'].lower()
        for drive in drives:
            if drive['name'].lower() == drive_name_lower: return drive['id']
        raise FileNotFoundError(f"Biblioteca '{self.site_config['drive_name']}' não encontrada no site '{self.site_config['site_path']}'.")

    # --- MÉTODOS DE LEITURA ---

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
            response_content.raise_for_status()
            
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
                
                if start_row_index == -1:
                    logging.warning(f"Texto inicial '{search_text}' não encontrado no arquivo '{file_name}'.")
                    return None

                df_data = df_full.iloc[start_row_index + 1:].copy()
                if df_data.empty: return None
                
                new_header = df_data.iloc[0]
                df_data = df_data[1:]
                df_data.columns = new_header
                df_data.reset_index(drop=True, inplace=True)

                if not df_data.columns.is_unique:
                    cols, counts, new_cols = list(df_data.columns), {}, []
                    for i, col in enumerate(cols):
                        col_name = str(col) if pd.notna(col) else f'Unnamed_{i}'
                        if col_name in counts:
                            counts[col_name] += 1
                            new_cols.append(f"{col_name}_{counts[col_name]}")
                        else:
                            counts[col_name] = 1
                            new_cols.append(col_name)
                    df_data.columns = new_cols
                
                stop_row_index = next((index for index, row in df_data.iterrows() if any('disponivel para' in str(cell).lower() or 'pedidos em tela' in str(cell).lower() for cell in row)), -1)
                
                df_final = df_data.iloc[:stop_row_index] if stop_row_index != -1 else df_data
                return df_final.dropna(axis=1, how='all')
            return None
        except Exception as e:
            logging.error(f"Falha ao ler o arquivo {file_name}. Erro: {e}")
            return None

    # --- MÉTODOS DE ESCRITA ---

    def _convert_to_excel_col(self, n: int) -> str:
        """Converte um índice de coluna (ex: 0) para letra (ex: 'A')."""
        result = ''
        while n >= 0:
            result = chr(n % 26 + ord('A')) + result
            n = n // 26 - 1
        return result

    def get_item_by_path(self, item_path: str) -> Dict:
        """Obtém os metadados de um item pelo seu caminho relativo à raiz da biblioteca."""
        try:
            folder_prefix = f"/{self.site_config['folder_path']}" if self.site_config.get('folder_path') else ""
            full_path = f"/root:{folder_prefix}/{item_path}"
            
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}{full_path}"
            logging.debug(f"Buscando item por caminho: {url}")
            return self._api_request('get', url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.error(f"Não foi possível encontrar o arquivo destino: {item_path} na pasta {self.site_config.get('folder_path')}")
                raise FileNotFoundError(f"❌ Arquivo '{item_path}' não encontrado. Crie um arquivo Excel vazio com este nome na pasta '{self.site_config.get('folder_path')}' do site '{self.site_config['site_path']}'.") from e
            raise

    def overwrite_sheet_with_dataframe(self, file_path: str, sheet_name: str, df: pd.DataFrame):
        """
        Limpa uma aba e escreve o DataFrame nela no SharePoint.
        """
        logging.info(f"--- Iniciando gravação no SharePoint: {file_path} (Aba: {sheet_name}) ---")
        
        COLUNAS_CRITICAS = {'cnpj', 'chave de acesso', 'empresa'}
        
        try:
            # 1. Encontrar o arquivo
            item = self.get_item_by_path(file_path)
            file_id = item['id']
            
            # 2. Verificar/Criar aba
            url_sheets = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets"
            existing_sheets = self._api_request('get', url_sheets).get('value', [])
            
            target_sheet_exists = any(s['name'].lower() == sheet_name.lower() for s in existing_sheets)
            
            if not target_sheet_exists:
                logging.info(f"Aba '{sheet_name}' não encontrada. Criando nova aba...")
                self._api_request('post', url_sheets, json={'name': sheet_name})
            else:
                logging.debug(f"Aba '{sheet_name}' encontrada. Limpando conteúdo antigo...")
                url_clear = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range/clear"
                self._api_request('post', url_clear, json={'applyTo': 'contents'})

            # 3. Preparar DataFrame
            if df.empty:
                logging.warning("DataFrame vazio. Aba limpa, mas sem dados novos.")
                return

            df_clean = df.copy()

            # Força cabeçalhos para string
            df_clean.columns = [str(c) if pd.notna(c) else "" for c in df_clean.columns]
            
            # Lista de colunas para usar no loop
            header_list = list(df_clean.columns)
            cleaned_columns = []

            # --- ITERAR POR POSIÇÃO (ILOC) PARA EVITAR ERRO COM DUPLICATAS ---
            for i in range(len(df_clean.columns)):
                col_name = header_list[i]
                col_norm = col_name.lower().strip()
                
                # Seleciona a coluna pela POSIÇÃO (garante que vem uma Series e não um DataFrame)
                val = df_clean.iloc[:, i].astype(str)
                
                # Limpezas
                val = val.replace(['nan', 'NaN', 'NaT', 'None', '<NA>'], '')
                val = val.str.replace(r'\.0$', '', regex=True)
                
                if col_norm in COLUNAS_CRITICAS:
                      val = val.apply(lambda x: f"'{x}" if x else x)
                else:
                    val = val.str.replace('.', ',', regex=False)
                
                cleaned_columns.append(val)
            
            # Reconstrói o DataFrame limpo
            df_final_clean = pd.concat(cleaned_columns, axis=1)
            # Garante que os nomes das colunas estão certos
            df_final_clean.columns = header_list

            values = [header_list] + df_final_clean.values.tolist()
            
            # 4. Escrever dados
            num_rows = len(values)
            num_cols = len(values[0])
            end_col_letter = self._convert_to_excel_col(num_cols - 1)
            address = f"A1:{end_col_letter}{num_rows}"

            url_write = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range(address='{address}')"
            self._api_request('patch', url_write, json={'values': values})
            
            logging.info(f"✅ Sucesso! {num_rows} linhas salvas na aba '{sheet_name}'.")

        except Exception as e:
            logging.error(f"❌ Erro ao sobrescrever a aba no SharePoint: {e}", exc_info=True)

# ==============================================================================
# 5. FUNÇÕES DE DADOS E DE-PARA
# ==============================================================================

def coletar_dados_do_datalake(source_config: Dict[str, Any], general_config: Config) -> pd.DataFrame:
    """Conecta ao SharePoint, lê arquivos e consolida."""
    logging.info(f"--- Iniciando coleta da fonte: {source_config['name']} (Site: {source_config['site_path']}) ---")
    sp_client = SharePointClient(source_config, general_config)
    all_items = sp_client.get_files_in_folder()
    
    if not all_items:
        logging.warning(f"Nenhum arquivo encontrado para a fonte '{source_config['name']}'.")
        return pd.DataFrame()
        
    list_of_dataframes, success_count = [], 0
    files_to_process = [item for item in all_items if "file" in item and not any(k in item['name'].lower() for k in general_config.KEYWORDS_TO_EXCLUDE)]
    total_files = len(files_to_process)
    logging.info(f"Encontrados {total_files} arquivos para processar.")

    for item in files_to_process:
        file_name = item['name']
        df = sp_client.read_excel_sheet(item['id'], file_name)
        
        if df is not None and not df.empty:
            mapped_name = next((value for key, value in general_config.FILENAME_MAP.items() if key.lower() in file_name.lower()), file_name)
            df['Origem'] = mapped_name
            list_of_dataframes.append(df)
            success_count += 1
            
    logging.info(f"Processamento concluído: {success_count} de {total_files} arquivos lidos com sucesso.")
            
    if not list_of_dataframes:
        logging.warning(f"Nenhum dado válido extraído.")
        return pd.DataFrame()
        
    consolidated_df = pd.concat(list_of_dataframes, ignore_index=True)
    return consolidated_df

def salvar_no_sheets(client, df, url_planilha, nome_aba):
    """Salva DataFrame no Google Sheets."""
    try:
        logging.info(f"Salvando no Google Sheets (Aba: {nome_aba})...")
        spreadsheet = client.open_by_url(url_planilha)
        worksheet = spreadsheet.worksheet(nome_aba)
        worksheet.clear()
        df_str = df.astype(str)
        set_with_dataframe(worksheet, df_str, include_index=False, include_column_header=True, resize=True)
        logging.info(f"✅ Salvo no Google Sheets com sucesso.")
    except gspread.exceptions.WorksheetNotFound:
        logging.error(f"❌ Aba '{nome_aba}' não encontrada no Google Sheets.")
    except Exception as e:
        logging.error(f"❌ Erro ao salvar no Google Sheets: {e}")

def aplicar_de_para_empresa(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica o DE->PARA na coluna EMPRESA."""
    logging.info("--- Aplicando DE->PARA (Empresa) ---")
    if 'EMPRESA' not in df.columns: return df

    try:
        home_dir = os.path.expanduser('~')
        caminho_de_para = os.path.join(home_dir, 'Documentos', 'De Para', 'Empresa.csv')
        df_de_para = pd.read_csv(caminho_de_para, sep=';', encoding='latin-1')
        df_de_para.columns = df_de_para.columns.str.strip()

        if 'De' not in df_de_para.columns or '2_EMPRESA' not in df_de_para.columns:
            return df

        df_merged = pd.merge(df, df_de_para[['De', '2_EMPRESA']], left_on='EMPRESA', right_on='De', how='left')
        df_merged['EMPRESA'] = df_merged['2_EMPRESA'].fillna(df_merged['EMPRESA'])
        return df_merged.drop(columns=['De', '2_EMPRESA'])
    except Exception as e:
        logging.warning(f"Não foi possível aplicar DE-PARA: {e}")
        return df

# ==============================================================================
# 6. EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    try:
        logging.info("--- INICIANDO PROCESSO COMPLETO ---")
        Config.validate()
        
        # 1. Autenticação Google
        google_client = autenticar_google_sheets()
        if not google_client:
            raise ConnectionError("Falha crítica no Google. Abortando.")
            
        url_sheets = "https://docs.google.com/spreadsheets/d/19mc4J3oIm5oO_6oz5fjyNjgKE3lqgiw1H3rwyt8FuPE/edit?usp=sharing"
        nome_aba_destino = "Base"

        # 2. Coleta SharePoint (Leitura)
        df_painel_tanque = coletar_dados_do_datalake(Config.DATALAKE_CONFIG, Config)
        
        if df_painel_tanque.empty:
            logging.warning("Nenhum dado novo coletado. Encerrando.")
            return

        # ---------------------------------------------------------
        # 3. TRATAMENTO INICIAL E PADRONIZAÇÃO (CORRIGIDA)
        # ---------------------------------------------------------
        logging.info("Tratando dados e resolvendo duplicatas de colunas...")
        
        # Converte para maiúsculo e remove colunas duplicadas (fica apenas com a primeira ocorrência)
        df_painel_tanque.columns = [str(c).upper().strip() for c in df_painel_tanque.columns]
        df_painel_tanque = df_painel_tanque.loc[:, ~df_painel_tanque.columns.duplicated()]

        cols_to_fill = ['TANQUE', 'PRODUTO']
        for col in cols_to_fill:
            if col in df_painel_tanque.columns:
                df_painel_tanque[col] = df_painel_tanque[col].replace(r'^\s*(-)?\s*$', np.nan, regex=True).ffill()
        
        if 'EMPRESA' in df_painel_tanque.columns:
            df_painel_tanque['EMPRESA'] = df_painel_tanque['EMPRESA'].astype(str)
            df_painel_tanque = df_painel_tanque[~df_painel_tanque['EMPRESA'].str.strip().isin(['-', '', 'nan'])]
        
        df_painel_tanque.drop(columns=['TANQUE'], inplace=True, errors='ignore')

        today_str = date.today().strftime('%Y-%m-%d')
        df_painel_tanque['DATA_ATUALIZACAO'] = today_str
        df_painel_tanque['DATA_HORA_EXECUCAO'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # ---------------------------------------------------------
        # 4. HISTÓRICO (GOOGLE SHEETS) - TAMBÉM COM REMOÇÃO DE DUPLICATAS
        # ---------------------------------------------------------
        df_historico_preservado = pd.DataFrame()
        try:
            spreadsheet = google_client.open_by_url(url_sheets)
            worksheet = spreadsheet.worksheet(nome_aba_destino)
            df_historico = get_as_dataframe(worksheet, evaluate_formulas=False)
            df_historico.dropna(how='all', inplace=True)
            
            if not df_historico.empty:
                # Padroniza e remove duplicatas no histórico
                df_historico.columns = [str(c).upper().strip() for c in df_historico.columns]
                df_historico = df_historico.loc[:, ~df_historico.columns.duplicated()]
                
                if 'DATA_ATUALIZACAO' in df_historico.columns:
                    df_historico['DATA_ATUALIZACAO'] = pd.to_datetime(df_historico['DATA_ATUALIZACAO']).dt.strftime('%Y-%m-%d')
                    df_historico_preservado = df_historico[df_historico['DATA_ATUALIZACAO'] != today_str]
                else:
                    df_historico_preservado = df_historico
                
                logging.info(f"📊 Histórico preservado: {len(df_historico_preservado)} linhas.")

        except Exception as e:
            logging.warning(f"Erro ao obter histórico: {e}")

        # UNIÃO SEGURA: Agora as colunas são únicas em ambos os lados
        df_final_para_salvar = pd.concat([df_historico_preservado, df_painel_tanque], ignore_index=True)
        
        # ---------------------------------------------------------
        # 5. FILTROS FINAIS
        # ---------------------------------------------------------
        # Remove colunas Unnamed indesejadas
        df_final_para_salvar = df_final_para_salvar.loc[:, ~df_final_para_salvar.columns.str.contains('^UNNAMED', case=False, na=False)]
        
        if 'PRODUTO' in df_final_para_salvar.columns:
            df_final_para_salvar = df_final_para_salvar[
                ~df_final_para_salvar['PRODUTO'].astype(str).str.strip().str.upper().isin(Config.PRODUTOS_EXCLUIDOS)
            ]
        
        df_final_para_salvar = aplicar_de_para_empresa(df_final_para_salvar)

        # ---------------------------------------------------------
        # 6. SALVAMENTO
        # ---------------------------------------------------------
        salvar_no_sheets(google_client, df_final_para_salvar, url_sheets, nome_aba_destino)

        sp_client_writer = SharePointClient(Config.DESTINATION_CONFIG, Config)
        sp_client_writer.overwrite_sheet_with_dataframe(
            file_path=Config.SHAREPOINT_DEST_FILE,
            sheet_name=Config.SHAREPOINT_DEST_SHEET,
            df=df_final_para_salvar
        )

        # Aba Atual
        df_atual = df_final_para_salvar[df_final_para_salvar['DATA_ATUALIZACAO'] == today_str].copy()
        if not df_atual.empty:
            sp_client_writer.overwrite_sheet_with_dataframe(
                file_path=Config.SHAREPOINT_DEST_FILE,
                sheet_name="Atual",
                df=df_atual
            )
            logging.info(f"✅ Sucesso total!")

    except Exception as e:
        logging.critical(f"❌ ERRO CRÍTICO: {e}", exc_info=True)

if __name__ == "__main__":
    main()