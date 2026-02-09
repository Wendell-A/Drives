# -*- coding: utf-8 -*-
import os
import io
import logging
import pandas as pd
import requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from typing import List, Dict, Any
import warnings
from datetime import datetime
from pathlib import Path

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E LOGGING
# ==============================================================================
SCRIPT_DIR = Path(__file__).resolve().parent
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
load_dotenv(dotenv_path=SCRIPT_DIR / ".env")

class Config:
    """⚙️ Centraliza todas as configurações e parâmetros da aplicação."""
    TENANT_ID: str = os.getenv("TENANT_ID")
    CLIENT_ID: str = os.getenv("CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
    HOSTNAME: str = os.getenv("HOSTNAME")

    # --- LEITURA DO SHAREPOINT ---
    TRANSPORTES_SHAREPOINT_CONFIG: Dict[str, Any] = {
        "name": "Transportes SharePoint (Leitura)", 
        "site_path": "/sites/Transportes", 
        "drive_name": "Documentos", 
        "folder_path": "", 
        "sheet_name": "Base",
        "header": ["sm", "data_prev_carregamento", "expedidor", "cidade_origem", "ufo", "destinatario_venda", "destinatario", "recebedor", "cidade_destino", "ufd", "produto", "motorista", "cavalo", "carreta1", "carreta2", "transportadora", "nfe", "volume_l", "data_de_carregamento", "horario_de_carregamento", "data_chegada", "data_descarga", "status"],
        "arquivos_para_ler": ["FORM-PPL-000 - Fitplan Anidro - RJ.xlsx", "FORM-PPL-000 - Fitplan Anidro - SP.xlsx", "FORM-PPL-000 - Fitplan Biodiesel.xlsx", "FORM-PPL-000 - Fitplan Hidratado - RJ.xlsx", "FORM-PPL-000 - Fitplan Hidratado - SP.xlsx"]
    }

    # --- ESCRITA NO SHAREPOINT ---
    DESTINO_SHAREPOINT: Dict[str, Any] = {
        "name": "Transportes SharePoint (Escrita)",
        "site_path": "/sites/Transportes",
        "drive_name": "Documentos",
        "folder_path": "Disponibilidade", 
        "file_name": "Base_Consolidada_Transportes.xlsx"
    }

    # --- GOOGLE SHEETS ---
    TRANSPORTES_SHEETS_CONFIG: Dict[str, Any] = {
        "name": "Transportes Google Sheets",
        "credentials_path": SCRIPT_DIR / "credenciais.json",
        "sheet_urls": ["https://docs.google.com/spreadsheets/d/1bu3CR46-D62laZyUcxuTlookn0kjEsJjvv0frzjJ03c/edit?usp=sharing", "https://docs.google.com/spreadsheets/d/1K22quWCg2XTpfgenx-n5nQ5efDr6_ntcyX3iCExJ958/edit?usp=sharing"],
        "sheet_name_to_read": "Base",
        "header": ["sm", "data_prev_carregamento", "expedidor", "cidade_origem", "ufo", "destinatario", "recebedor", "cidade_destino", "ufd", "produto", "motorista", "cavalo", "carreta1", "carreta2", "transportadora", "nfe", "volume_l", "data_de_carregamento", "horario_de_carregamento", "data_chegada", "data_descarga", "status"]
    }
    OUTPUT_SHEET_CONFIG: Dict[str, Any] = {
        "url": "https://docs.google.com/spreadsheets/d/1HsCqPy5UUxeNo4VyR2eXbIXVyEWiLcgKtRyAZlqWlOk/edit?usp=sharing",
        "sheet_name": "Base",
        "timestamp_sheet_name": "Data Atualização"
    }
    KEYWORDS_TO_EXCLUDE: List[str] = ["backup", "modelo", "corrompida", "corrompido", "dinamica"]
    
    @staticmethod
    def validate():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET]): raise ValueError("❌ Faltam credenciais do SharePoint no .env")
        if not os.path.exists(Config.TRANSPORTES_SHEETS_CONFIG["credentials_path"]): raise ValueError("❌ Arquivo 'credenciais.json' não encontrado na pasta do script.")
        logging.info("Configurações validadas com sucesso.")

# ==============================================================================
# 2. CLASSES DE CLIENTE E FUNÇÕES DE LEITURA
# ==============================================================================
class SharePointClient:
    def __init__(self, site_config: Dict[str, Any]): 
        self.site_config = site_config
        self.access_token = self._get_access_token()
        self.site_id = self._get_site_id()
        self.drive_id = self._get_drive_id()

    def _api_request(self, method: str, url: str, data=None, headers=None) -> Dict[str, Any] | None: 
        if headers is None:
            headers = {"Authorization": f"Bearer {self.access_token}"}
        
        response = requests.request(method, url, headers=headers, data=data)
        response.raise_for_status()
        
        if 'application/json' in response.headers.get('Content-Type', ''):
            return response.json()
        return None

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
            if drive['name'].lower() == drive_name_lower: return drive['id']
        raise FileNotFoundError(f"Biblioteca '{self.site_config['drive_name']}' não encontrada.")

    def get_files_in_folder(self) -> List[Dict[str, Any]]: 
        path = f"/root:/{requests.utils.quote(self.site_config['folder_path'])}:" if self.site_config['folder_path'] else "/root"
        url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}{path}/children"
        return self._api_request('get', url).get("value", [])

    def read_excel_sheet(self, file_id: str, file_name: str) -> pd.DataFrame | None:
        try:
            url_item = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}"
            download_url = self._api_request('get', url_item).get('@microsoft.graph.downloadUrl')
            if not download_url: return None
            response = requests.get(download_url, timeout=60)
            response.raise_for_status()
            xls = pd.ExcelFile(io.BytesIO(response.content))
            sheet_name = next((s for s in xls.sheet_names if s.lower() == self.site_config['sheet_name'].lower()), None)
            if sheet_name:
                df = pd.read_excel(xls, sheet_name=sheet_name, header=None, dtype=str)
                header_list = self.site_config['header']
                for i, row in df.head(15).iterrows():
                    if any(str(cell).strip().lower() == 'sm' for cell in row):
                        df_data = df.iloc[i + 1:, :len(header_list)]
                        df_data.columns = header_list
                        return df_data.reset_index(drop=True)
            return None
        except Exception as e: 
            logging.error(f"Falha ao ler {file_name}: {e}")
            return None

    def upload_dataframes_as_excel(self, sheets_dict: Dict[str, pd.DataFrame], file_name: str):
        """
        Recebe um dicionário onde a chave é o nome da aba e o valor é o DataFrame.
        """
        logging.info(f"Iniciando upload para SharePoint: {file_name} com {len(sheets_dict)} abas.")
        try:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                for sheet_name, df in sheets_dict.items():
                    logging.info(f"Preparando aba: {sheet_name} ({len(df)} linhas)")
                    df.to_excel(writer, index=False, sheet_name=sheet_name)
            
            output.seek(0)
            file_content = output.getvalue()

            folder_path = self.site_config.get('folder_path', '')
            path_url = f"/root:/{requests.utils.quote(folder_path)}/{requests.utils.quote(file_name)}:" if folder_path else f"/root:/{requests.utils.quote(file_name)}:"
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}{path_url}/content"

            headers = {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
            
            response = requests.put(url, headers=headers, data=file_content)
            response.raise_for_status()
            logging.info(f"✅ Arquivo '{file_name}' salvo com sucesso no SharePoint (Abas: {list(sheets_dict.keys())}).")

        except Exception as e:
            logging.error(f"❌ Erro ao salvar arquivo no SharePoint: {e}")
            raise

class GoogleSheetsClient:
    def __init__(self, credentials_path: str):
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
        self.client = gspread.authorize(creds)
    def get_data_as_dataframe(self, url: str, sheet_name: str, header_config: list) -> pd.DataFrame:
        try:
            sheet = self.client.open_by_url(url).worksheet(sheet_name); all_data = sheet.get_all_values()
            for i, row in enumerate(all_data[:15]):
                if 'sm' in [str(cell).strip().lower() for cell in row]:
                    df = pd.DataFrame(all_data[i + 1:]); df = df.iloc[:, :len(header_config)]; df.columns = header_config; return df
            return pd.DataFrame()
        except Exception as e: logging.error(f"Falha ao ler Google Sheet '{url}': {e}"); return pd.DataFrame()
    def clear_and_write_dataframe(self, url: str, sheet_name: str, df_to_write: pd.DataFrame):
        try:
            logging.info(f"Abrindo a planilha de destino: {url}"); spreadsheet = self.client.open_by_url(url)
            try: sheet = spreadsheet.worksheet(sheet_name); logging.info(f"Limpando a aba '{sheet_name}'..."); sheet.clear()
            except gspread.exceptions.WorksheetNotFound: logging.warning(f"Aba '{sheet_name}' não encontrada. Criando uma nova..."); sheet = spreadsheet.add_worksheet(title=sheet_name, rows="1", cols="1")
            logging.info(f"Escrevendo {len(df_to_write)} linhas na planilha...")
            data_to_write = [df_to_write.columns.values.tolist()] + df_to_write.values.tolist()
            sheet.update(values=data_to_write, value_input_option='USER_ENTERED'); logging.info("Dados escritos no Google Sheets com sucesso.")
        except Exception as e: logging.error(f"Falha ao escrever no Google Sheets: {e}"); raise
    def update_timestamp(self, url: str, sheet_name: str, message: str):
        try:
            spreadsheet = self.client.open_by_url(url); sheet = spreadsheet.worksheet(sheet_name)
            sheet.update(range_name='A1', values=[[message]])
            logging.info(f"Timestamp '{message}' atualizado na aba '{sheet_name}'.")
        except gspread.exceptions.WorksheetNotFound: logging.error(f"Aba de timestamp '{sheet_name}' não foi encontrada! Verifique o nome na planilha.")
        except Exception as e: logging.error(f"Falha ao atualizar o timestamp: {e}")

def carregar_dados_sharepoint(config: Dict[str, Any]) -> pd.DataFrame:
    logging.info(f"--- Coletando dados de: {config['name']} ---"); sp_client = SharePointClient(config); all_files = sp_client.get_files_in_folder(); dataframes = []; files_to_read = config.get("arquivos_para_ler", [])
    for item in all_files:
        if "folder" in item: continue
        file_name = item['name']
        if file_name in files_to_read and not any(k in file_name.lower() for k in Config.KEYWORDS_TO_EXCLUDE):
            df = sp_client.read_excel_sheet(item['id'], file_name)
            if df is not None: df['Fonte'] = file_name; dataframes.append(df)
    if not dataframes: return pd.DataFrame()
    return pd.concat(dataframes, ignore_index=True)
def carregar_dados_google_sheets(config: Dict[str, Any]) -> pd.DataFrame:
    logging.info(f"--- Coletando dados de: {config['name']} ---"); gs_client = GoogleSheetsClient(config["credentials_path"]); dataframes = []
    for url in config["sheet_urls"]:
        df = gs_client.get_data_as_dataframe(url, config["sheet_name_to_read"], config["header"])
        if not df.empty: df.insert(5, 'destinatario_venda', ''); df['Fonte'] = url.split('/d/')[1].split('/')[0]; dataframes.append(df)
    if not dataframes: return pd.DataFrame()
    return pd.concat(dataframes, ignore_index=True)

if __name__ == "__main__":
    try:
        logging.info("--- INICIANDO PROCESSO DE COLETA DE DADOS DE TRANSPORTE ---"); Config.validate()
        
        # 1. Coleta
        df_transportes_sp = carregar_dados_sharepoint(Config.TRANSPORTES_SHAREPOINT_CONFIG)
        df_transportes_gs = carregar_dados_google_sheets(Config.TRANSPORTES_SHEETS_CONFIG)
        
        logging.info("--- INICIANDO CONSOLIDAÇÃO ---")
        if df_transportes_sp.empty and df_transportes_gs.empty: 
            logging.warning("Nenhum dado foi coletado. Nenhum arquivo será gerado ou atualizado.")
        else:
            # 2. Consolidação
            df_consolidado = pd.concat([df_transportes_sp, df_transportes_gs], ignore_index=True).copy()
            logging.info(f"Dados consolidados com sucesso. Total de {len(df_consolidado)} linhas antes do filtro.")
            
            # Inserir Data e Hora da Atualização
            data_atual = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            if 'Fonte' in df_consolidado.columns:
                loc_index = df_consolidado.columns.get_loc('Fonte') + 1
                df_consolidado.insert(loc_index, 'Data_Atualizacao', data_atual)
            else:
                df_consolidado['Data_Atualizacao'] = data_atual

            # 3. Filtros
            status_para_manter = ['Descarregado', 'Aguardando', 'Trânsito', 'Programado']
            filtro_regex = '|'.join(status_para_manter)
            df_filtrado = df_consolidado[df_consolidado['status'].str.contains(filtro_regex, case=False, na=False)].copy()
            logging.info(f"Filtro aplicado. Total de {len(df_filtrado)} linhas após o filtro.")
            df_filtrado.fillna('', inplace=True)
            
            # ---------------------------------------------------------
            # LOGICA DE ABAS EXTRAS
            # ---------------------------------------------------------
            
            # Tratamento prévio para garantir comparação de strings
            df_filtrado['data_descarga'] = df_filtrado['data_descarga'].astype(str).str.strip()
            df_filtrado['data_chegada'] = df_filtrado['data_chegada'].astype(str).str.strip()

            # --- 1. Aba 'atuais' (Descarga Hoje ou Vazio) ---
            hoje_br = datetime.now().strftime('%d/%m/%Y')
            hoje_iso = datetime.now().strftime('%Y-%m-%d')
            
            mask_atuais = (
                (df_filtrado['data_descarga'] == '') | 
                (df_filtrado['data_descarga'] == 'nan') | 
                (df_filtrado['data_descarga'] == 'NaT') | 
                (df_filtrado['data_descarga'].str.contains(hoje_br, na=False)) |
                (df_filtrado['data_descarga'].str.contains(hoje_iso, na=False))
            )
            df_atuais = df_filtrado[mask_atuais].copy()
            logging.info(f"Aba 'atuais' gerada com {len(df_atuais)} linhas.")

            # --- 2. Aba 'Aguardando Descarga' (Chegada PREENCHIDA e Descarga VAZIA) ---
            mask_aguardando = (
                # Chegada TEM que ter valor
                (df_filtrado['data_chegada'] != '') & 
                (df_filtrado['data_chegada'] != 'nan') & 
                (df_filtrado['data_chegada'] != 'NaT') &
                # Descarga TEM que ser vazia
                (
                    (df_filtrado['data_descarga'] == '') | 
                    (df_filtrado['data_descarga'] == 'nan') | 
                    (df_filtrado['data_descarga'] == 'NaT')
                )
            )
            df_aguardando = df_filtrado[mask_aguardando].copy()
            
            # --- 🍒 A CEREJA DO BOLO: CÁLCULO DE DIAS DE ESPERA (AJUSTADO) ---
            if not df_aguardando.empty:
                # 1. Limpeza e garantia de String
                col_dates = df_aguardando['data_chegada'].astype(str).str.strip()

                # 2. TENTATIVA 1: Forçar formato Brasileiro (SharePoint geralmente é dd/mm/yyyy)
                # dayfirst=True instrui: "se for ambíguo como 05/01, entenda como 05 de Jan"
                dates_temp = pd.to_datetime(col_dates, dayfirst=True, errors='coerce')

                # 3. TENTATIVA 2: Recuperar o que falhou (NaT) usando formato genérico/US
                # O parâmetro errors='coerce' transformou o formato do sheets (yyyy-mm-dd ou mm/dd/yyyy) em NaT
                # Agora filtramos só esses erros e tentamos converter SEM o dayfirst=True
                mask_erros = dates_temp.isna()
                if mask_erros.any():
                    dates_temp[mask_erros] = pd.to_datetime(col_dates[mask_erros], dayfirst=False, errors='coerce')
                
                # 4. Pegar a data de agora e calcular
                now = datetime.now()
                df_aguardando['Dias_Aguardando'] = (now - dates_temp).dt.days
                
                # 5. Tratar remanescentes (casos irremediáveis viram 0)
                df_aguardando['Dias_Aguardando'] = df_aguardando['Dias_Aguardando'].fillna(0).astype(int)
            else:
                df_aguardando['Dias_Aguardando'] = 0

            logging.info(f"Aba 'Aguardando Descarga' gerada com {len(df_aguardando)} linhas e coluna de espera calculada.")

            # ---------------------------------------------------------
            # UPLOADS
            # ---------------------------------------------------------

            # 4. Salvar no Google Sheets (Apenas a Base filtrada)
            logging.info("--- INICIANDO UPLOAD PARA O GOOGLE SHEETS ---")
            gs_client_writer = GoogleSheetsClient(Config.TRANSPORTES_SHEETS_CONFIG["credentials_path"])
            gs_client_writer.clear_and_write_dataframe(url=Config.OUTPUT_SHEET_CONFIG["url"], sheet_name=Config.OUTPUT_SHEET_CONFIG["sheet_name"], df_to_write=df_filtrado)
            
            timestamp_msg = f"Última atualização em: {datetime.now().strftime('%d/%m/%Y às %H:%M:%S')}"
            gs_client_writer.update_timestamp(url=Config.OUTPUT_SHEET_CONFIG["url"], sheet_name=Config.OUTPUT_SHEET_CONFIG["timestamp_sheet_name"], message=timestamp_msg)
            
            # 5. Salvar no SharePoint (Base + Atuais + Aguardando Descarga)
            logging.info("--- INICIANDO UPLOAD PARA O SHAREPOINT ---")
            sp_writer = SharePointClient(Config.DESTINO_SHAREPOINT)
            
            # Dicionário com as 3 abas
            dict_abas = {
                'Base': df_filtrado,
                'atuais': df_atuais,
                'Aguardando Descarga': df_aguardando
            }
            
            sp_writer.upload_dataframes_as_excel(
                sheets_dict=dict_abas, 
                file_name=Config.DESTINO_SHAREPOINT['file_name']
            )

            print("\n" + "*"*50)
            print("📈 DADOS ATUALIZADOS NO GOOGLE SHEETS E SHAREPOINT COM SUCESSO!")
            print(f"Google Sheets: {Config.OUTPUT_SHEET_CONFIG['url']}")
            print(f"SharePoint: {Config.DESTINO_SHAREPOINT['folder_path']}/{Config.DESTINO_SHAREPOINT['file_name']}")
            print(f"Abas salvas: {list(dict_abas.keys())}")
            print("*"*50)

    except Exception as e:
        logging.critical(f"❌ PROCESSO INTERROMPIDO POR ERRO CRÍTICO: {e}", exc_info=True)