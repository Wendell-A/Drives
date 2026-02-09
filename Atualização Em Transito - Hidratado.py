import os
import logging
import pandas as pd
import requests
import unicodedata 
from dotenv import load_dotenv
from datetime import date, timedelta, datetime
from typing import List, Dict, Any, Tuple

# ==============================================================================
# CONFIGURAÇÃO E LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,  # <--- Mude de INFO para DEBUG aqui
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

load_dotenv()

class Config:
    """
    ⚙️ Centraliza todas as configurações e variáveis de ambiente da aplicação.
    """
    TENANT_ID: str = os.getenv("TENANT_ID")
    CLIENT_ID: str = os.getenv("CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
    HOSTNAME: str = os.getenv("HOSTNAME")
    
    SITE_PATH: str = "sites/Transportes" 
    
    # TARGET_FOLDER_NAME removido pois agora lemos da raiz
    TARGET_SHEET_NAME: str = "Base" 
    
    Qive_FILENAME: str = "Relatório de NF Qive.xlsx"
    Qive_SHEET_NAME: str = "Sheet1"
    
    # --- ARQUIVO DE DIVERGÊNCIA NO SHAREPOINT ---
    REPORT_DIVERGENCIA_FILENAME: str = "Relatório de divergência Hidratado.xlsx"
    REPORT_DIVERGENCIA_SHEET: str = "Divergencia NF"
    
    PRODUTOS_BIO: List[str] = ["Hidratado","Hidratado "]
    
    # --- LISTA DE PRODUTOS PARA FILTRO DE EXCEÇÃO (NOVOS) ---
    PRODUTOS_FILTRO_EXCECAO: List[str] = ["Hidratado", "Hidratado "]
    VOLUME_MAXIMO_EXCECAO: int = 66000 
    
    # --- LISTA DE ARQUIVOS PERMITIDOS ---
    # O script só lerá estes arquivos, ignorando outros documentos na raiz
    ARQUIVOS_PERMITIDOS: List[str] = [
        "FORM-PPL-000 - Fitplan Hidratado - RJ.xlsx",
        "FORM-PPL-000 - Fitplan Hidratado - SP.xlsx"
    ]

    COLUNAS_TRANSPORTE: List[str] = [
        "sm", "data_prev_carregamento", "expedidor", "cidade_origem", "ufo",
        "destinatario_venda", "destinatario", "recebedor", "cidade_destino", "ufd",
        "produto", "motorista", "cavalo", "carreta1", "carreta2", "transportadora",
        "nfe", "volume_l", "data_de_carregamento", "horario_de_carregamento",
        "data_chegada", "data_descarga", "status"
    ]

    # --- ★★★ NOVO: COLUNAS QUE DEVEM SER FORÇADAS COMO TEXTO NO EXCEL ★★★ ---
    COLS_PARA_FORCAR_TEXTO: List[str] = [
        'CNPJ DO CLIENTE', 'CHAVE DE ACESSO', 'NÚMERO DA NOTA FISCAL'
    ]

    @staticmethod
    def validar_configuracoes():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET, Config.HOSTNAME]):
            raise ValueError("❌ Faltam variáveis de ambiente essenciais (TENANT_ID, CLIENT_ID, CLIENT_SECRET, HOSTNAME) no arquivo .env.")
        
        if not Config.SITE_PATH:
            raise ValueError("❌ A variável SITE_PATH está vazia no script.")
            
        logging.info("Configurações de ambiente carregadas com sucesso.")

# ==============================================================================
# CLASSE PARA INTERAÇÃO COM SHAREPOINT
# ==============================================================================
class SharePointClient:
    """
    Gerencia toda a comunicação com a API Microsoft Graph.
    """
    def __init__(self, config: Config):
        self.config = config
        self.access_token = self._get_access_token()
        self.api_site_path = f"{self.config.HOSTNAME}:/{self.config.SITE_PATH}"
        
        logging.info(f"Tentando acessar o site: {self.api_site_path}")
        self.site_id = self._get_id('sites', self.api_site_path)
        self.drive_id = self._get_main_drive_id()

    def _get_access_token(self) -> str:
        """Obtém o token de acesso para autenticação."""
        url = f"https://login.microsoftonline.com/{self.config.TENANT_ID}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.CLIENT_ID,
            "client_secret": self.config.CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default"
        }
        response = requests.post(url, data=data)
        response.raise_for_status()
        logging.info("Token de acesso obtido com sucesso.")
        return response.json()["access_token"]

    def _api_request(self, method: str, url: str, params: Dict = None, json: Dict = None) -> Any:
        """Centraliza e trata requisições à API do Microsoft Graph."""
        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            response = requests.request(method, url, headers=headers, params=params, json=json)
            response.raise_for_status()
            return response.json() if response.content else None
        except requests.exceptions.HTTPError as e:
            logging.error(f"Erro na requisição da API ({method} {url}): {e.response.text}")
            raise

    def _get_id(self, resource: str, path: str) -> str:
        """Obtém o ID de um recurso do SharePoint."""
        try:
            url = f"https://graph.microsoft.com/v1.0/{resource}/{path}"
            logging.debug(f"Buscando ID para: {url}")
            return self._api_request('get', url)['id']
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.error(f"Não foi possível encontrar o recurso: {path}")
                raise FileNotFoundError(f"❌ Recurso '{resource}' em '{path}' não encontrado.") from e
            raise

    def _get_main_drive_id(self) -> str:
        """Obtém o ID da biblioteca de documentos principal ('Documentos')."""
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        logging.info(f"Buscando drives (bibliotecas) no site_id: {self.site_id}")
        for drive in self._api_request('get', url).get("value", []):
            if drive.get('name') == 'Documentos':
                logging.info("Biblioteca 'Documentos' encontrada.")
                return drive['id']
        raise FileNotFoundError("❌ Biblioteca 'Documentos' não encontrada.")

    # --- NOVO MÉTODO: LER DIRETO DA RAIZ ---
    def get_root_items(self) -> List[Dict]:
        """Obtém a lista de arquivos e pastas na RAIZ da biblioteca Documents."""
        try:
            logging.debug(f"Listando arquivos da raiz da biblioteca (ID: {self.drive_id})...")
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root/children"
            return self._api_request('get', url).get("value", [])
        except Exception as e:
            logging.error(f"Erro ao listar arquivos da raiz: {e}")
            return []

    # Método mantido para compatibilidade, caso precise no futuro, mas não usado agora para transporte
    def get_items_in_folder(self, folder_name: str) -> List[Dict]:
        """Obtém a lista de arquivos e pastas em uma pasta específica."""
        try:
            folder_id = self._get_id('drives', f"{self.drive_id}/root:/{folder_name}:")
            logging.debug(f"Pasta '{folder_name}' (ID: {folder_id}) encontrada. Listando arquivos...")
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{folder_id}/children"
            return self._api_request('get', url).get("value", [])
        except FileNotFoundError:
            logging.error(f"Pasta '{folder_name}' não encontrada no SharePoint.")
            return []
    
    def get_item_by_path(self, item_path: str) -> Dict:
        """Obtém os metadados de um item (arquivo/pasta) pelo seu caminho na raiz do drive."""
        try:
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root:/{item_path}"
            logging.debug(f"Buscando item por caminho: {url}")
            return self._api_request('get', url)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.error(f"Não foi possível encontrar o item no caminho: {item_path}")
                raise FileNotFoundError(f"❌ Item em '{item_path}' não encontrado.") from e
            raise
    
    def format_header_color(self, file_id: str, sheet_name: str, color_hex: str):
        """Pinta o cabeçalho (A1:W1) de uma cor."""
        try:
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range(address='A1:W1')/format/fill"
            payload = {"color": color_hex}
            self._api_request('patch', url, json=payload)
            logging.debug(f"Cabeçalho do arquivo ID {file_id} pintado (Cor: {color_hex}).")
        except Exception as e:
            logging.warning(f"Não foi possível pintar o cabeçalho do arquivo ID {file_id}: {e}")

    def _convert_to_excel_col(self, n: int) -> str:
        """Converte um índice de coluna (ex: 0) para letra (ex: 'A')."""
        result = ''
        while n >= 0:
            result = chr(n % 26 + ord('A')) + result
            n = n // 26 - 1
        return result
        
    def read_sheet_data(self, item_id: str, sheet_name: str) -> Tuple[pd.DataFrame, str]:
            """Lê os dados de uma planilha (Transporte) no SharePoint, com fallback para leitura em blocos."""
            actual_sheet_name = None
            try:
                url_sheets = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets"
                worksheets = self._api_request('get', url_sheets).get("value", [])
                actual_sheet_name = next((ws['name'] for ws in worksheets if ws['name'].strip().lower() == sheet_name.strip().lower()), None)
                if not actual_sheet_name:
                    return None, f"Aba '{sheet_name}' não encontrada."
                
                col_end_char = 'BA' # Coluna fixa para leitura
                
                try:
                    # TENTA LER TUDO DE UMA VEZ
                    url_used_range = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{actual_sheet_name}/usedRange"
                    response_range = self._api_request('get', url_used_range)
                    address = response_range.get('address')
                    
                    if not address:
                        return None, "Aba está vazia ou não tem um usedRange válido."
                    
                    logging.debug(f"Leitura direta usando usedRange: {address}")
                    
                    url_range = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{actual_sheet_name}/range(address='{address}')"
                    data = self._api_request('get', url_range).get('values', [])
                    if not data or len(data) < 2:
                        return None, "Aba está vazia ou contém apenas cabeçalho."

                    df = pd.DataFrame(data[1:])
                    df.columns = data[0]

                    df = df.iloc[:, :len(Config.COLUNAS_TRANSPORTE)]
                    df.columns = self.config.COLUNAS_TRANSPORTE
                    df['__ms_file_id'] = item_id
                    df['__ms_row_index'] = range(response_range.get('rowIndex', 0) + 2, len(df) + response_range.get('rowIndex', 0) + 2)
                    
                    return df, actual_sheet_name
                
                except requests.exceptions.HTTPError as e:
                    # SE FALHAR (ARQUIVO MUITO GRANDE OU SUJO), ENTRA AQUI
                    if e.response.status_code == 400 and "RangeExceedsLimit" in str(e.response.text):
                        logging.warning("O usedRange excedeu o limite. Iniciando leitura em blocos.")
                        
                        url_header = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{actual_sheet_name}/range(address='A1:{col_end_char}1')"
                        header_data = self._api_request('get', url_header).get('values', [])
                        if not header_data:
                                return None, "Falha ao ler o cabeçalho. Planilha inválida ou inacessível."
                        
                        full_data = []
                        chunk_size = 2000
                        current_row = 2
                        LIMITE_MAXIMO_LINHAS = 15000  # <--- NOVA TRAVA DE SEGURANÇA
                        
                        while True:
                            # --- VERIFICAÇÃO DE LIMITE ---
                            if current_row > LIMITE_MAXIMO_LINHAS:
                                logging.warning(f"⚠️ Leitura interrompida: Limite de {LIMITE_MAXIMO_LINHAS} linhas atingido para evitar loop infinito.")
                                break
                            # -----------------------------

                            end_row = current_row + chunk_size - 1
                            chunk_address = f"A{current_row}:{col_end_char}{end_row}"
                            logging.debug(f"Lendo bloco de dados fixo: {chunk_address}")
                            
                            try:
                                url_chunk = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{actual_sheet_name}/range(address='{chunk_address}')"
                                chunk_data = self._api_request('get', url_chunk).get('values', [])
                            except requests.exceptions.HTTPError as e:
                                # Se der erro 400 aqui, provavelmente acabou a planilha real
                                break

                            if not chunk_data:
                                break
                            
                            full_data.extend(chunk_data)
                            
                            # Se o bloco veio menor que o pedido, acabou os dados
                            if len(chunk_data) < chunk_size:
                                break
                                
                            current_row = end_row + 1

                        full_data_with_header = header_data + full_data
                        if len(full_data_with_header) < 2:
                            return None, "Aba está vazia após a leitura em blocos."

                        df = pd.DataFrame(full_data_with_header[1:])
                        df.columns = full_data_with_header[0]

                        df = df.iloc[:, :len(Config.COLUNAS_TRANSPORTE)]
                        df.columns = self.config.COLUNAS_TRANSPORTE
                        df['__ms_file_id'] = item_id
                        df['__ms_row_index'] = range(2, len(df) + 2)
                        
                        logging.info(f"Leitura em blocos concluída. Total de linhas lidas: {len(df)}")
                        return df, actual_sheet_name

                    else:
                        raise e
            except Exception as e:
                logging.error(f"Erro ao ler dados da planilha do SharePoint: {e}")
                return None, "Erro ao processar a planilha."

    def read_generic_sheet_data(self, item_id: str, sheet_name: str) -> Tuple[pd.DataFrame, str]:
        """Lê os dados de uma planilha genérica no SharePoint, usando apenas o usedRange."""
        actual_sheet_name = None
        try:
            url_sheets = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets"
            worksheets = self._api_request('get', url_sheets).get("value", [])
            actual_sheet_name = next((ws['name'] for ws in worksheets if ws['name'].strip().lower() == sheet_name.strip().lower()), None)
            
            if not actual_sheet_name and sheet_name.lower() == 'sheet1' and worksheets:
                    actual_sheet_name = worksheets[0]['name']
                    logging.warning(f"Aba 'Sheet1' não encontrada. Usando a primeira aba disponível: '{actual_sheet_name}'")
            elif not actual_sheet_name:
                return None, f"Aba '{sheet_name}' não encontrada."
            
            url_used_range = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{actual_sheet_name}/usedRange"
            response_range = self._api_request('get', url_used_range)
            address = response_range.get('address')
            
            if not address:
                return None, "Aba está vazia ou não tem um usedRange válido."
            
            logging.debug(f"Leitura genérica direta usando usedRange: {address}")
            
            url_range = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{actual_sheet_name}/range(address='{address}')"
            data = self._api_request('get', url_range).get('values', [])
            if not data or len(data) < 2:
                return None, "Aba está vazia ou contém apenas cabeçalho."

            df = pd.DataFrame(data[1:])
            df.columns = data[0]
            df['__ms_file_id'] = item_id
            
            return df, actual_sheet_name
        
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and "RangeExceedsLimit" in str(e.response.text):
                    logging.error(f"Erro ao ler planilha genérica: O usedRange ({address}) excedeu o limite da API...")
                    return None, "Arquivo muito grande (RangeExceedsLimit)."
            raise
        except Exception as e:
            logging.error(f"Erro ao ler dados da planilha genérica do SharePoint: {e}")
            return None, "Erro ao processar a planilha genérica."

    def update_cell(self, file_id: str, sheet_name: str, row_index: int, col_name: str, value: Any):
        """Atualiza o valor de uma única célula na planilha."""
        try:
            col_idx = self.config.COLUNAS_TRANSPORTE.index(col_name)
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/cell(row={row_index-1},column={col_idx})"
            self._api_request('patch', url, json={'values': [[value]]})
            logging.debug(f"Célula atualizada: {col_name}='{value}' na linha {row_index}.")
        except ValueError:
            logging.error(f"A coluna '{col_name}' não foi encontrada na lista de colunas de transporte.")
        except Exception as e:
            logging.error(f"Erro ao atualizar a célula na linha {row_index}, coluna '{col_name}': {e}")
    
    def add_rows(self, file_id: str, sheet_name: str, rows_data: List[List[Any]]):
        """Adiciona múltiplas linhas no final de uma planilha."""
        try:
            url_range = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/usedRange"
            data = self._api_request('get', url_range)
            last_row = data.get('rowIndex', 0) + data.get('rowCount', 0)
            
            num_new_rows = len(rows_data)
            num_cols = len(rows_data[0])
            col_letter = self._convert_to_excel_col(num_cols - 1)
            
            address = f"A{last_row + 1}:{col_letter}{last_row + num_new_rows}"
            url_update = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range(address='{address}')"
            self._api_request('patch', url_update, json={'values': rows_data})
            logging.debug(f"Adicionadas {num_new_rows} novas linhas no SharePoint (ID: {file_id}).")
        except Exception as e:
            logging.error(f"Erro ao adicionar novas linhas no SharePoint: {e}")

    # --- FUNÇÃO PARA SOBRESCREVER ABA (COM FORÇA BRUTA TEXTO) ---
    def overwrite_sheet_with_dataframe(self, file_path: str, sheet_name: str, df: pd.DataFrame):
            """
            Limpa uma aba e escreve o DataFrame nela.
            SE A ABA NÃO EXISTIR, ELA SERÁ CRIADA AUTOMATICAMENTE.
            """
            
            # Mapeamento das colunas críticas
            COLUNAS_CRITICAS = {
                'cnpj do cliente': True,
                'chave de acesso': True,
                'número': True
            }
            
            try:
                # 1. Encontrar o arquivo
                item = self.get_item_by_path(file_path)
                file_id = item['id']
                
                # --- NOVA LÓGICA: VERIFICAR SE A ABA EXISTE E CRIAR SE NECESSÁRIO ---
                url_sheets = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets"
                existing_sheets = self._api_request('get', url_sheets).get('value', [])
                
                # Verifica se o nome da aba existe (ignorando maiúsculas/minúsculas)
                target_sheet_exists = any(s['name'].lower() == sheet_name.lower() for s in existing_sheets)
                
                if not target_sheet_exists:
                    logging.info(f"Aba '{sheet_name}' não encontrada. Criando nova aba...")
                    self._api_request('post', url_sheets, json={'name': sheet_name})
                else:
                    # Se já existe, limpamos o conteúdo antes de escrever
                    logging.debug(f"Aba '{sheet_name}' encontrada. Limpando conteúdo antigo...")
                    url_clear = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range/clear"
                    self._api_request('post', url_clear, json={'applyTo': 'contents'})

                # 2. Preparar os dados (mantendo sua lógica de força bruta de texto)
                df_clean = df.copy()
                header = [str(col) for col in df_clean.columns]
                header_norm = [str(col).lower().strip() for col in header]

                for i, col_name in enumerate(header):
                    col_norm = header_norm[i]
                    
                    df_clean[col_name] = df_clean[col_name].astype(str)
                    df_clean[col_name] = df_clean[col_name].str.replace(r'\.0$', '', regex=True)
                    df_clean[col_name] = df_clean[col_name].replace(['nan', 'NaT', 'None'], '')
                    
                    if col_norm in COLUNAS_CRITICAS:
                        df_clean[col_name] = df_clean[col_name].apply(lambda x: f"'{x}" if x else x)
                    else:
                        df_clean[col_name] = df_clean[col_name].str.replace(',', '.')

                values = [header] + df_clean.values.tolist()
                
                if not values:
                    logging.warning("DataFrame vazio. Nada foi escrito, mas a aba foi garantida.")
                    return

                # 3. Escrever os novos dados
                num_rows = len(values)
                num_cols = len(values[0])
                end_col_letter = self._convert_to_excel_col(num_cols - 1)
                address = f"A1:{end_col_letter}{num_rows}"

                url_write = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range(address='{address}')"
                self._api_request('patch', url_write, json={'values': values})
                
                logging.info(f"Aba '{sheet_name}' atualizada com sucesso no arquivo '{file_path}'. ({num_rows} linhas)")

            except Exception as e:
                logging.error(f"Erro ao sobrescrever/criar a aba no SharePoint: {e}")
# ==============================================================================
# CLASSE PARA PROCESSAMENTO DE DADOS
# ==============================================================================
class DataProcessor:
    """
    📊 Contém a lógica de negócio e manipulação de dados.
    """
    def __init__(self, config: Config):
        self.config = config

    @staticmethod
    def _normalizar_texto(series: pd.Series) -> pd.Series:
        """Normaliza strings removendo acentos e convertendo para maiúsculas."""
        if series is None:
            return pd.Series(dtype='object')
        return series.astype(str).str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8').str.strip().str.upper()

    @staticmethod
    def _tratar_data_excel(series: pd.Series) -> pd.Series:
        """Converte colunas de data de forma inteligente."""
        datas_numericas = pd.to_numeric(series.astype(str).str.replace(',', '.'), errors='coerce')
        datas_convertidas = pd.to_datetime(datas_numericas, unit='D', origin='1899-12-30', errors='coerce')
        datas_texto = pd.to_datetime(series, dayfirst=True, errors='coerce')
        return datas_convertidas.fillna(datas_texto)

    @staticmethod
    def _limpar_placa(series: pd.Series) -> pd.Series:
        """Limpa uma série de placas, mantendo apenas alfanuméricos."""
        if series is None:
            return pd.Series(dtype='object')
        return series.astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)

    def _criar_chaves(self, df: pd.DataFrame, is_faturado: bool = False, mapa_recebedores: pd.DataFrame = None) -> pd.DataFrame:
        """Cria as chaves de cruzamento para a reconciliação."""
        
        if is_faturado:
            logging.info("Criando chaves para o Qive (Faturados)...")
            df.columns = [str(col).lower().strip() for col in df.columns]
            df = df.loc[:, ~df.columns.duplicated(keep='first')]
            
            col_produto = next((c for c in ['[item] descrição', 'produto'] if c in df.columns), None)
            col_p1 = next((c for c in ['placa1', 'placa do veículo'] if c in df.columns), None)
            col_p2 = next((c for c in ['placa2'] if c in df.columns), None)
            col_p3 = next((c for c in ['placa3'] if c in df.columns), None)

            if not col_produto:
                logging.error("❌ Coluna de Produto (ex: '[item] descrição') não encontrada no Qive.")
                return pd.DataFrame()
            
            if not col_p1:
                logging.warning("⚠️ Coluna de Placa1 (ex: 'placa1' ou 'placa do veículo') não encontrada no Qive.")

            df['produto_norm'] = self._normalizar_texto(df[col_produto])
            df['chave_base_Qive'] = df['produto_norm']
            df['placa1_norm'] = self._limpar_placa(df[col_p1]) if col_p1 else ''
            df['placa2_norm'] = self._limpar_placa(df[col_p2]) if col_p2 else ''
            df['placa3_norm'] = self._limpar_placa(df[col_p3]) if col_p3 else ''
            
            df['data_emissao_faturado'] = self._tratar_data_excel(df.get('data emissão'))
            df['número'] = df.get('número', '').astype(str).str.strip()
            
        else:
            logging.info("Criando chaves para o Fitplan (Transporte)...")
            
            df['chave_grupo'] = "SM_" + df['sm'].astype(str).str.strip()
            
            df['produto_norm'] = self._normalizar_texto(df['produto'])
            df['cavalo_norm'] = self._limpar_placa(df['cavalo'])
            
            df['chave_base_fitplan'] = df['produto_norm']
            
            if 'nfe' in df.columns:
                df['nfe'] = df['nfe'].astype(str).str.strip()

        return df
    
    def carregar_dados_transporte(self, sp_client: SharePointClient) -> pd.DataFrame:
        """Carrega e processa os dados de transporte (FORM-PPL) da pasta RAIZ (Documentos)."""
        
        # ALTERAÇÃO: Agora lê direto da raiz, e não mais de uma subpasta
        items = sp_client.get_root_items()
        
        all_dfs = []
        for item in items:
            filename = item.get('name', '')
            
            # Filtro importante: só processa arquivos que estão na lista permitida
            if filename not in self.config.ARQUIVOS_PERMITIDOS:
                continue

            if 'file' in item and filename.lower().endswith(('.xlsx', '.xls')) and not filename.startswith('~') and filename != self.config.Qive_FILENAME:
                logging.debug(f"Lendo arquivo de Transporte do SharePoint: '{filename}'")
                df, sheet_name = sp_client.read_sheet_data(item['id'], self.config.TARGET_SHEET_NAME)
                if df is not None:
                    df['__ms_file_name'] = filename
                    df['__ms_sheet_name'] = sheet_name
                    all_dfs.append(df)
                else: 
                    logging.warning(f"Pulando '{filename}': {sheet_name}")
        
        if not all_dfs:
            logging.info("Nenhum arquivo de transporte válido encontrado na raiz. Retornando DataFrame vazio.")
            return pd.DataFrame()
        
        df_transporte = pd.concat(all_dfs, ignore_index=True)
        logging.info(f"Carregados {len(df_transporte)} registros de {len(all_dfs)} arquivos de transporte.")
        return self._criar_chaves(df_transporte)

    def carregar_dados_faturados(self, sp_client: SharePointClient) -> pd.DataFrame:
        """Carrega e processa os dados faturados (Qive) da raiz do SharePoint."""
        logging.info(f"Procurando arquivo de faturados (Qive) na raiz: '{self.config.Qive_FILENAME}'")
        
        try:
            Qive_item = sp_client.get_item_by_path(self.config.Qive_FILENAME)
            
            if not Qive_item or 'file' not in Qive_item:
                logging.error(f"Caminho '{self.config.Qive_FILENAME}' encontrado, mas não é um arquivo.")
                return pd.DataFrame()
            
            logging.info(f"Arquivo Qive encontrado (ID: {Qive_item['id']}). Lendo dados...")
            
            df, sheet_name = sp_client.read_generic_sheet_data(Qive_item['id'], self.config.Qive_SHEET_NAME)
            
            if df is None:
                logging.error(f"Falha ao ler o arquivo Qive: {sheet_name}")
                return pd.DataFrame()

            logging.info(f"Arquivo Qive lido. Total de linhas brutas: {len(df)}")

            col_names_map = {str(col).lower().strip(): col for col in df.columns}

            try:
                if len(df.columns) > 13: 
                    col_n_name = df.columns[13] 
                    logging.info(f"Aplicando filtro de 'Cancelamento' na coluna N (detectada como: '{col_n_name}')")
                    col_n_normalized = df[col_n_name].astype(str).str.strip().str.upper()
                    
                    linhas_antes = len(df)
                    df = df[col_n_normalized != 'CANCELAMENTO']
                    linhas_depois = len(df)
                    logging.info(f"Filtro 'Cancelamento' removeu {linhas_antes - linhas_depois} linhas.")
                else:
                    logging.warning(f"Não foi possível aplicar o filtro de 'Cancelamento'. O arquivo Qive tem menos de 14 colunas.")
            except Exception as e:
                logging.error(f"Erro ao aplicar filtro de 'Cancelamento' na coluna N: {e}")

            try:
                col_data_nome_original = col_names_map.get('data emissão')
                
                if col_data_nome_original:
                    logging.info(f"Aplicando filtro de data D-3 na coluna G (detectada como: '{col_data_nome_original}')")
                    
                    df[col_data_nome_original] = self._tratar_data_excel(df[col_data_nome_original])
                    
                    cutoff_date = pd.to_datetime(date.today() - timedelta(days=3)).normalize()
                    
                    linhas_antes = len(df)
                    df = df[df[col_data_nome_original].notna() & (df[col_data_nome_original] >= cutoff_date)]
                    linhas_depois = len(df)
                    logging.info(f"Filtro de data (D-3) removeu {linhas_antes - linhas_depois} linhas.")
                else:
                    logging.warning(f"Coluna 'data emissão' (esperada na Coluna G) não encontrada. Filtro de data D-3 não foi aplicado.")
            except Exception as e:
                logging.error(f"Erro ao aplicar filtro de data D-3: {e}")

            logging.info(f"Total de linhas do Qive após filtros: {len(df)}")
            
            if '[item] quantidade' in df.columns:
                df['[item] quantidade'] = df['[item] quantidade'].astype(str)
            
            col_numero_nome_original = col_names_map.get('número')
            if col_numero_nome_original:
                df[col_numero_nome_original] = df[col_numero_nome_original].astype(str)
            
            return self._criar_chaves(df, is_faturado=True) 
            
        except FileNotFoundError:
            logging.error(f"Arquivo de faturados '{self.config.Qive_FILENAME}' não encontrado na raiz da biblioteca 'Documentos'.")
            return pd.DataFrame()
        except Exception as e:
            logging.error(f"Erro ao carregar o arquivo de faturados do SharePoint: {e}", exc_info=True)
            return pd.DataFrame()

# ==============================================================================
# ORQUESTRADOR PRINCIPAL
# ==============================================================================
def get_updates_from_faturado(faturado_row: Dict) -> Dict:
    """Extrai e formata os dados de atualização de uma linha de faturado."""
    updates = {}
    try:
        raw_str = str(faturado_row.get('[item] quantidade', '')).strip()
        py_str = raw_str.replace(',', '.')
        cleaned_str = ''.join(c for c in py_str if c.isdigit() or c == '.')
        volume = float(cleaned_str) if cleaned_str else None
        
        updates["status"] = "EM TRÂNSITO" 
        updates["nfe"] = str(faturado_row['número'])
        updates["volume_l"] = volume
        updates["horario_de_carregamento"] = "Pend. OC." 
        
        if pd.notna(faturado_row['data_emissao_faturado']):
            updates["data_de_carregamento"] = faturado_row['data_emissao_faturado'].strftime("%d/%m/%Y")
        
    except Exception as e:
        logging.warning(f"Erro ao processar linha faturada (NFe: {faturado_row.get('número')}, Qtd: {faturado_row.get('[item] quantidade')}): {e}")
    
    return updates

def main():
    try:
        Config.validar_configuracoes()
        sp_client = SharePointClient(Config)
        processor = DataProcessor(Config)

        logging.info("--- Fase 1: Carregamento e Preparação dos Dados ---")
        df_transporte = processor.carregar_dados_transporte(sp_client)
        if df_transporte.empty:
            logging.info("Nenhum dado de transporte válido encontrado. Encerrando.")
            return
        
        df_faturados = processor.carregar_dados_faturados(sp_client) 
        
        if df_faturados.empty:
            logging.info("Nenhum dado faturado válido encontrado. Encerrando.")
            return

        nfs_ja_registradas = set(df_transporte['nfe'].dropna().astype(str).str.strip().tolist())
        df_faturados = df_faturados[~df_faturados['número'].isin(nfs_ja_registradas)].copy()
        
        logging.info(f"Carregadas {len(df_faturados)} NFs novas (pós-filtros) para reconciliação.")


        df_programados = df_transporte[DataProcessor._normalizar_texto(df_transporte['status']) == 'PROGRAMADO'].copy()
        if df_programados.empty:
            logging.info("Nenhum registro 'PROGRAMADO' encontrado. Encerrando.")
            return
    
        logging.info("--- Fase 2: Reconciliação das Viagens ---")
        
        file_ids_to_update = df_programados['__ms_file_id'].unique()
        sheet_map = df_programados.drop_duplicates('__ms_file_id').set_index('__ms_file_id')['__ms_sheet_name'].to_dict()
        
        logging.info(f"Iniciando atualização. Marcando {len(file_ids_to_update)} arquivos com cabeçalho VERMELHO...")
        for file_id in file_ids_to_update:
            sp_client.format_header_color(file_id, sheet_map[file_id], "#FF0000") # Vermelho
        
        grupos_a_processar = df_programados['chave_grupo'].unique()
        logging.info(f"Encontrados {len(grupos_a_processar)} grupos (SMs) 'PROGRAMADOS' para processar.")
        
        updates_count = 0
        report_data = [] 
        file_update_summary = {} 

        df_faturados = df_faturados.reset_index(drop=True)
        
        for grupo in grupos_a_processar:
            sp_row_info = df_programados[df_programados['chave_grupo'] == grupo].iloc[0]
            logging.info(f"Processando Grupo: {grupo} (Arquivo: {sp_row_info['__ms_file_name']})")
            
            if 'DATA_INVALIDA' in grupo:
                logging.warning(f"Pulando grupo '{grupo}' por conter data inválida no planejamento.")
                continue

            sp_rows = df_programados[df_programados['chave_grupo'] == grupo].copy().reset_index(drop=True)
            produto_grupo = DataProcessor._normalizar_texto(sp_rows['produto']).iloc[0]

            chave_base_grupo = sp_rows['chave_base_fitplan'].iloc[0] 
            cavalo_do_grupo = sp_rows['cavalo_norm'].iloc[0]

            entry = {
                "GrupoTransporte": grupo, "ChaveBaseTransporte": chave_base_grupo,
                "CavaloTransporte": cavalo_do_grupo, "Status": "", "QtdCandidatosQive": 0,
                "QiveP1_Candidatos": "", "QiveP2_Candidatos": "", "QiveP3_Candidatos": "",
                "NFs_Combinadas": ""
            }

            faturado_candidatos = df_faturados[df_faturados['chave_base_Qive'] == chave_base_grupo].copy()
            
            entry["QtdCandidatosQive"] = len(faturado_candidatos)
            
            if faturado_candidatos.empty:
                entry["Status"] = "FALHA: Produto nao bateu"
                report_data.append(entry)
                continue 

            entry["QiveP1_Candidatos"] = ", ".join(faturado_candidatos['placa1_norm'].unique())
            entry["QiveP2_Candidatos"] = ", ".join(faturado_candidatos['placa2_norm'].unique())
            entry["QiveP3_Candidatos"] = ", ".join(faturado_candidatos['placa3_norm'].unique())

            matches_p1 = faturado_candidatos[faturado_candidatos['placa1_norm'] == cavalo_do_grupo]
            
            Qive_restante_p1 = faturado_candidatos.drop(matches_p1.index)
            if Qive_restante_p1.empty:
                faturado_rows = matches_p1
            else:
                matches_p2 = Qive_restante_p1[Qive_restante_p1['placa2_norm'] == cavalo_do_grupo]
                Qive_restante_p2 = Qive_restante_p1.drop(matches_p2.index)
                if Qive_restante_p2.empty:
                    faturado_rows = pd.concat([matches_p1, matches_p2])
                else:
                    matches_p3 = Qive_restante_p2[Qive_restante_p2['placa3_norm'] == cavalo_do_grupo]
                    faturado_rows = pd.concat([matches_p1, matches_p2, matches_p3])
            
            faturado_rows.sort_values(by='data_emissao_faturado', ascending=True, inplace=True)
            faturado_rows_list = faturado_rows.to_dict('records')

            if not faturado_rows_list:
                entry["Status"] = "FALHA: Placa nao bateu"
                report_data.append(entry)
                continue

            entry["Status"] = "SUCESSO"
            entry["NFs_Combinadas"] = ", ".join(faturado_rows['número'].astype(str).unique())
            
            if produto_grupo in Config.PRODUTOS_BIO:
                logging.debug(f"  > Aplicando lógica BIO (N vs M). Planejadas: {len(sp_rows)}, Faturadas: {len(faturado_rows_list)}.")
                
                min_len = min(len(sp_rows), len(faturado_rows_list))
                for i in range(min_len):
                    sp_row = sp_rows.iloc[i]
                    fat_row = faturado_rows_list[i]
                    updates = get_updates_from_faturado(fat_row)
                    
                    file_name = sp_row['__ms_file_name']
                    file_update_summary.setdefault(file_name, {'updated': 0, 'added': 0})
                    file_update_summary[file_name]['updated'] += 1
                    
                    for col, val in updates.items():
                        sp_client.update_cell(sp_row['__ms_file_id'], sp_row['__ms_sheet_name'], sp_row['__ms_row_index'], col, val)
                    updates_count += 1
                
                if len(sp_rows) > len(faturado_rows_list):
                    for i in range(len(faturado_rows_list), len(sp_rows)):
                        sp_row = sp_rows.iloc[i]
                        file_name = sp_row['__ms_file_name']
                        file_update_summary.setdefault(file_name, {'updated': 0, 'added': 0})
                        file_update_summary[file_name]['updated'] += 1
                        sp_client.update_cell(sp_row['__ms_file_id'], sp_row['__ms_sheet_name'], sp_row['__ms_row_index'], 'status', 'PMM não Utilizada')
                
                elif len(faturado_rows_list) > len(sp_rows):
                    sp_row_base = sp_rows.iloc[0]
                    file_name = sp_row_base['__ms_file_name']
                    file_update_summary.setdefault(file_name, {'updated': 0, 'added': 0})
                    
                    novas_linhas_dados = []
                    for i in range(len(sp_rows), len(faturado_rows_list)):
                        fat_row = faturado_rows_list[i]
                        updates = get_updates_from_faturado(fat_row)
                        nova_linha = sp_row_base.copy()
                        for col, val in updates.items():
                            nova_linha[col] = val
                        novas_linhas_dados.append(nova_linha[Config.COLUNAS_TRANSPORTE].values.tolist())
                        file_update_summary[file_name]['added'] += 1 
                    
                    if novas_linhas_dados:
                        sp_client.add_rows(sp_row_base['__ms_file_id'], sp_row_base['__ms_sheet_name'], novas_linhas_dados)
            
            else: 
                logging.debug(f"  > Aplicando lógica simples (1x1).")
                sp_row = sp_rows.iloc[0]
                fat_row = faturado_rows_list[0]
                updates = get_updates_from_faturado(fat_row)
                
                file_name = sp_row['__ms_file_name']
                file_update_summary.setdefault(file_name, {'updated': 0, 'added': 0})
                file_update_summary[file_name]['updated'] += 1
                
                for col, val in updates.items():
                    sp_client.update_cell(sp_row['__ms_file_id'], sp_row['__ms_sheet_name'], sp_row['__ms_row_index'], col, val)
                updates_count += 1
            
            report_data.append(entry)

            nfs_usadas = [str(fr['número']) for fr in faturado_rows_list]
            df_faturados = df_faturados[~df_faturados['número'].isin(nfs_usadas)].copy()

        logging.info("--- Fase 3: Gerando Relatório de Tentativas ---")
        if report_data:
            report_df = pd.DataFrame(report_data)
            col_order = [
                "GrupoTransporte", "ChaveBaseTransporte", "CavaloTransporte", "Status",
                "QtdCandidatosQive", "QiveP1_Candidatos", "QiveP2_Candidatos", "QiveP3_Candidatos",
                "NFs_Combinadas"
            ]
            report_df = report_df[col_order]
            report_filename = "relatorio_tentativas_match.csv"
            
            try:
                full_report_path = os.path.abspath(report_filename)
                report_df.to_csv(report_filename, index=False, sep=';', encoding='utf-8-sig')
                logging.info(f"✅ Relatório de tentativas salvo em: {full_report_path}")
            except Exception as e:
                logging.error(f"Falha ao salvar relatório de tentativas: {e}")
        else:
            logging.info("Nenhuma tentativa de match foi processada para gerar relatório.")
        
        # --- FASE 4 ATUALIZADA (COM MULTIPLOS FILTROS) ---
        logging.info("--- Fase 4: Gerando Relatório de NFs não Utilizadas (Exceções) ---")
        
        PRODUTOS_FILTRO_EXCECAO = Config.PRODUTOS_FILTRO_EXCECAO
        VOLUME_MAXIMO_EXCECAO = Config.VOLUME_MAXIMO_EXCECAO

        try:
            if not df_faturados.empty:
                
                # Cria a máscara de filtro para todos os produtos na lista
                produto_mask = df_faturados['produto_norm'].apply(
                    lambda x: any(p in x for p in PRODUTOS_FILTRO_EXCECAO) if isinstance(x, str) else False
                )
                
                df_excecoes_filtrado = df_faturados[produto_mask].copy()

                def limpar_volume_interno(val):
                    try:
                        raw = str(val).strip().replace(',', '.')
                        clean = ''.join(c for c in raw if c.isdigit() or c == '.')
                        return float(clean)
                    except:
                        return 0.0

                df_excecoes_filtrado['__vol_temp'] = df_excecoes_filtrado['[item] quantidade'].apply(limpar_volume_interno)
                df_excecoes_filtrado = df_excecoes_filtrado[df_excecoes_filtrado['__vol_temp'] <= VOLUME_MAXIMO_EXCECAO]

                qtd_excecoes = len(df_excecoes_filtrado)
                logging.warning(f"Encontradas {qtd_excecoes} NFs de produtos elegíveis (Vol <= {VOLUME_MAXIMO_EXCECAO}) não utilizadas.")
                
                if qtd_excecoes > 0:
                    # Remove colunas internas
                    cols_internas = [c for c in df_excecoes_filtrado.columns if c.startswith('__') or c.endswith('_norm') or c.startswith('chave_')]
                    df_final_salvar = df_excecoes_filtrado.drop(columns=cols_internas, errors='ignore')

                    logging.info(f"Sobrescrevendo arquivo '{Config.REPORT_DIVERGENCIA_FILENAME}' no SharePoint...")
                    sp_client.overwrite_sheet_with_dataframe(
                        Config.REPORT_DIVERGENCIA_FILENAME, 
                        Config.REPORT_DIVERGENCIA_SHEET, 
                        df_final_salvar
                    )
                else:
                    logging.info(f"Nenhuma exceção encontrada para os produtos elegíveis com volume <= {VOLUME_MAXIMO_EXCECAO}.")
            
            else:
                logging.info("✅ Nenhuma NF sobrando. Todas as NFs elegíveis foram reconciliadas.")
                
        except Exception as e:
            logging.error(f"Falha ao atualizar relatório de exceções no SharePoint: {e}")

        logging.info("--- Resumo das Atualizações por Arquivo ---")
        if not file_update_summary:
            logging.info("Nenhuma atualização realizada.")
        for file_name, counts in file_update_summary.items():
            logging.info(f"  > {file_name}: {counts['updated']} linha(s) atualizada(s), {counts['added']} linha(s) adicionada(s).")

        logging.info(f"Finalizando. Marcando {len(file_ids_to_update)} arquivos com cabeçalho VERDE...")
        for file_id in file_ids_to_update:
            sp_client.format_header_color(file_id, sheet_map[file_id], "#00FF00") # Verde

        logging.info(f"✅ Processo concluído. Total de {updates_count} viagens foram atualizadas para 'EM TRÂNSITO'.")

    except Exception as e:
        logging.critical(f"Ocorreu um erro fatal na aplicação: {e}", exc_info=True)

if __name__ == "__main__":
    main()