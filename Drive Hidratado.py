# -*- coding: utf-8 -*-
"""
Este script executa um processo de três fases:
1. COLETA: Conecta-se a dois sites do SharePoint, coleta e limpa os dados.
2. CRUZAMENTO E ATUALIZAÇÃO: Executa uma lógica de cruzamento "inteligente".
3. ATUALIZAÇÃO E RELATÓRIO: Atualiza as linhas modificadas e gera um relatório de divergências salvo em um arquivo Excel no SharePoint.
"""

import re
import os
import io
import sys
import time
import logging
import pandas as pd
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple, Set
import warnings

# ==============================================================================
# 1. CONFIGURAÇÃO INICIAL E LOGGING
# ==============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')
# Correção para compatibilidade futura do Pandas
pd.set_option('future.no_silent_downcasting', True)

load_dotenv()

class Config:
    """⚙️ Centraliza todas as configurações e parâmetros da aplicação."""
    TENANT_ID: str = os.getenv("TENANT_ID"); CLIENT_ID: str = os.getenv("CLIENT_ID"); CLIENT_SECRET: str = os.getenv("CLIENT_SECRET"); HOSTNAME: str = os.getenv("HOSTNAME")
    DESCARGAS_CONFIG: Dict[str, Any] = {
        "name": "Descargas DataLake", "site_path": "/sites/DataLake", "drive_name": "Documentos", "folder_path": "Bases", "sheet_name": "Descarga",
        "header": ['faturista', 'produto', 'origem', 'empresa', 'data', 'hora', 'placa', 'motorista', 'nota', 'quantidade_nf', 'op_tanque', 'aditivar', 'aditivo', 'dias_em_espera', 'status', 'data_de_descarga', 'hr_entrada'],
        "products_to_include": ['anidro', 'hidratado', 'biodiesel', 'gasolina a', 'gasolina c', 'diesel a s10', 'diesel b s10', 'diesel a s500', 'diesel b s500', 'mgo']
    }
    
    TRANSPORTES_CONFIG: Dict[str, Any] = {
        "name": "Transportes", "site_path": "/sites/Transportes", "drive_name": "Documentos", 
        "folder_path": "",
        "sheet_name": "Base",
        "header": ["sm", "data_prev_carregamento", "expedidor", "cidade_origem", "ufo", "destinatario_venda", "destinatario", "recebedor", "cidade_destino", "ufd", "produto", "motorista", "cavalo", "carreta1", "carreta2", "transportadora", "nfe", "volume_l", "data_de_carregamento", "horario_de_carregamento", "data_chegada", "data_descarga", "status"],
        "status_para_incluir": ["Em Trânsito", "Aguardando Descarga", "Em Trânsito By Pass", "Aguardando By Pass"],
        "arquivos_para_ler": [
            "FORM-PPL-000 - Fitplan Hidratado - RJ.xlsx",
            "FORM-PPL-000 - Fitplan Hidratado - SP.xlsx"
        ]
    }
    
    RELATORIO_DIVERGENCIA_CONFIG: Dict[str, Any] = {
        "site_path": "/sites/Transportes", 
        "drive_name": "Documentos",
        "folder_path": "", # Salvar na pasta raiz
        "file_name": "Relatório de divergência Hidratado.xlsx"
    }
    
    KEYWORDS_TO_EXCLUDE: List[str] = ["backup", "modelo", "corrompida", "corrompido", "dinamica"]
    FILENAME_MAP: Dict[str, str] = {
        "ARUJA": "Aruja", "BARRA_MANSA": "Barra Mansa", "BCAG": "BCAG",
        "CAVALINI": "Cavalini", "CROSS": "Cross Terminais", "DIRECIONAL_FILIAL": "Direcional Filial",
        "DIRECIONAL_MATRIZ": "Direcional Matriz", "FLAG": "Flag", "GRANEL_QUIMICA": "Granel Química",
        "MANGUINHOS": "Caxias", "PETRONORTE": "Petronorte", "REFIT_BASE": "Refit", "RODOPETRO_CAPIVARI": "Capivari",
        "SANTOS_BRASIL": "Santos Brasil", "SGP": "SGP", "STOCK": "Stock",
        "STOCKMAT": "Stockmat", "TIF": "TIF", "TLIQ": "Tliq", "TRANSO": "Transo",
        "TRR_AB": "Americo", "TRR_CATANDUVA": "Catanduva", "VAISHIA": "Vaishia"
    }

    @staticmethod
    def validate():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET]):
            raise ValueError("❌ Faltam credenciais no arquivo .env."); logging.info("Configurações de ambiente carregadas.")

class SharePointClient:
    def __init__(self, site_config: Dict[str, Any], config: Config):
        self.site_config = site_config
        self.config = config
        self.access_token = self._get_access_token()
        self.site_id = self._get_site_id()
        self.drive_id = self._get_drive_id()

    def _api_request(self, method: str, url: str, json: Dict = None, data=None) -> Dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        if data:
            headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        
        response = requests.request(method, url, headers=headers, json=json, data=data)
        response.raise_for_status()
        
        is_json_response = 'application/json' in response.headers.get('Content-Type', '')
        if response.content and is_json_response:
            return response.json()
        return None

    def _get_access_token(self) -> str:
        url = f"https://login.microsoftonline.com/{self.config.TENANT_ID}/oauth2/v2.0/token"
        data = {"client_id": self.config.CLIENT_ID, "scope": "https://graph.microsoft.com/.default", "client_secret": self.config.CLIENT_SECRET, "grant_type": "client_credentials"}
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
            response_content.raise_for_status()
            
            xls = pd.ExcelFile(io.BytesIO(response_content.content))
            sheet_name_to_find = self.site_config['sheet_name'].lower()
            actual_sheet_name = next((s for s in xls.sheet_names if s.lower() == sheet_name_to_find), None)
            
            if actual_sheet_name:
                df = pd.read_excel(xls, sheet_name=actual_sheet_name, header=None)
                
                header_list = self.site_config['header']
                header_keyword = 'sm' if self.site_config['name'] == 'Transportes' else 'produto'

                for i, row in df.head(15).iterrows():
                    if any(str(cell).strip().lower() == header_keyword for cell in row):
                        df_data = df.iloc[i + 1:].copy()
                        num_expected_cols = len(header_list)
                        df_data = df_data.iloc[:, :num_expected_cols]
                        df_data.columns = header_list
                        
                        df_final = df_data.reset_index(drop=True)
                        df_final['__ms_file_id'] = file_id
                        df_final['__ms_sheet_name'] = actual_sheet_name
                        df_final['__ms_row_index'] = df_final.index + i + 2
                        
                        return df_final
            return None
        except Exception as e:
            logging.error(f"Falha ao ler o arquivo {file_name} (ID: {file_id}). Erro: {e}")
            return None

    def update_cell(self, file_id: str, sheet_name: str, row_index: int, col_name: str, value: Any):
        try:
            col_list = Config.TRANSPORTES_CONFIG['header']
            col_idx = col_list.index(col_name)
            
            api_row = row_index - 1
            api_col = col_idx
            
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/cell(row={api_row},column={api_col})"
            payload = {'values': [[value]]}
            
            self._api_request('patch', url, json=payload)
        except ValueError:
            logging.error(f"A coluna '{col_name}' não foi encontrada na lista de colunas de configuração.")
        except Exception as e:
            logging.error(f"Erro ao atualizar a célula na Linha {row_index}, Coluna '{col_name}': {e}")

    def _get_range_url(self, file_id: str, sheet_name: str, range_address: str) -> str:
        return f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name}/range(address='{range_address}')"

    def format_range(self, file_id: str, sheet_name: str, range_address: str, format_payload: Dict[str, Any]):
        try:
            url = f"{self._get_range_url(file_id, sheet_name, range_address)}/format/fill"
            self._api_request('patch', url, json=format_payload)
            logging.info(f"Cor de preenchimento aplicada ao range '{range_address}' no arquivo {file_id}.")
        except Exception as e:
            logging.warning(f"Não foi possível aplicar cor ao range '{range_address}': {e}")
            
    def update_range_value(self, file_id: str, sheet_name: str, cell_address: str, value: Any):
        try:
            url = self._get_range_url(file_id, sheet_name, cell_address)
            payload = {'values': [[value]]}
            self._api_request('patch', url, json=payload)
        except Exception as e:
            logging.error(f"Não foi possível atualizar valor da célula '{cell_address}': {e}")

    # ==============================================================================
    # NOVO MÉTODO DE OUTPUT DO RELATÓRIO (IGUAL AO SCRIPT PARALELO)
    # ==============================================================================

    def _convert_to_excel_col(self, n: int) -> str:
        """Converte índice numérico (0, 1, 27) para letra (A, B, AB)."""
        string = ""
        while n >= 0:
            string = chr(n % 26 + 65) + string
            n = n // 26 - 1
        return string

    def get_file_id_by_name(self, file_name: str) -> str:
        """Busca o ID de um arquivo específico na pasta configurada."""
        try:
            folder_path = self.site_config.get('folder_path', '')
            if folder_path:
                # Evita duplicação de /root/root se houver subpasta
                encoded_path = f"{requests.utils.quote(folder_path)}/{requests.utils.quote(file_name)}"
            else:
                encoded_path = requests.utils.quote(file_name)

            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root:/{encoded_path}"
            
            response = self._api_request('get', url)
            return response['id'] if response else None
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise e
        except Exception:
            return None

    def upload_and_overwrite_excel(self, df_to_write: pd.DataFrame):
        """Método legado (usado apenas para criar arquivo do zero se não existir)."""
        config_relatorio = self.config.RELATORIO_DIVERGENCIA_CONFIG
        try:
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df_safe = df_to_write.fillna('').astype(str)
                df_safe.to_excel(writer, index=False, sheet_name='Divergencias')
            excel_data = output.getvalue()
            
            folder_path = config_relatorio['folder_path']
            file_name = config_relatorio['file_name']
            
            path_segment = f"/{requests.utils.quote(folder_path)}" if folder_path else ""
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root:{path_segment}/{requests.utils.quote(file_name)}:/content"
            
            logging.info(f"Criando novo arquivo no SharePoint: '{file_name}'...")
            self._api_request('put', url, data=excel_data)
            logging.info("Arquivo criado com sucesso.")
        except Exception as e:
            logging.error(f"Falha ao criar arquivo: {e}", exc_info=True)

    def update_specific_sheet(self, df_to_write: pd.DataFrame, sheet_name_target: str = 'Divergencias'):
        """
        Atualiza uma aba específica (Divergencias) preservando as demais.
        Usa a lógica robusta de limpar conteúdo e escrever com range calculado.
        """
        config_relatorio = self.config.RELATORIO_DIVERGENCIA_CONFIG
        file_name = config_relatorio['file_name']
        
        # 1. Preparar os dados
        df_clean = df_to_write.fillna('').astype(str)
        
        # Limpar '.0' de strings numéricas
        for col in df_clean.columns:
            df_clean[col] = df_clean[col].str.replace(r'\.0$', '', regex=True)

        header = [str(col) for col in df_clean.columns]
        dados_lista = [header] + df_clean.values.tolist()
        
        num_rows = len(dados_lista)
        num_cols = len(dados_lista[0]) if num_rows > 0 else 0
        
        try:
            # 2. Obter ID do Arquivo
            file_id = self.get_file_id_by_name(file_name)
            
            if not file_id:
                logging.info(f"Arquivo '{file_name}' não encontrado. Criando novo arquivo...")
                self.upload_and_overwrite_excel(df_to_write)
                return

            logging.info(f"Arquivo '{file_name}' encontrado. Atualizando aba '{sheet_name_target}'...")

            # 3. Verificar se a aba existe
            url_worksheets = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets"
            response_sheets = self._api_request('get', url_worksheets)
            existing_sheets = [sheet['name'] for sheet in response_sheets.get('value', [])]

            if sheet_name_target in existing_sheets:
                # Se existe, limpa o conteúdo (mantendo formatação, igual ao script paralelo)
                url_clear = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name_target}/range/clear"
                self._api_request('post', url_clear, json={'applyTo': 'contents'})
                logging.debug(f"Conteúdo da aba '{sheet_name_target}' limpo.")
            else:
                # Se não existe, cria a aba
                self._api_request('post', url_worksheets, json={'name': sheet_name_target})
                logging.info(f"Aba '{sheet_name_target}' criada com sucesso.")

            # 4. Escrever dados com Range Calculado (Evita erro 400)
            if num_rows > 0 and num_cols > 0:
                col_letter = self._convert_to_excel_col(num_cols - 1)
                address_range = f"A1:{col_letter}{num_rows}"
                
                url_write = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{file_id}/workbook/worksheets/{sheet_name_target}/range(address='{address_range}')"
                self._api_request('patch', url_write, json={'values': dados_lista})
            
            logging.info(f"Sucesso! {num_rows} linhas escritas na aba '{sheet_name_target}'.")

        except Exception as e:
            logging.error(f"Erro ao atualizar a aba específica: {e}", exc_info=True)
            raise

def _normalizar_texto_para_chave(series: Any) -> Any:
    if isinstance(series, pd.Series):
        return series.astype(str).str.strip().str.lower().str.replace('[^a-z0-9]', '', regex=True)
    else:
        texto_str = str(series).strip().lower()
        return re.sub('[^a-z0-9]', '', texto_str)

def carregar_e_consolidar_fonte(source_config: Dict[str, Any], general_config: Config) -> pd.DataFrame:
    logging.info(f"--- Iniciando coleta da fonte: {source_config['name']} ---")
    sp_client = SharePointClient(source_config, general_config)
    all_items = sp_client.get_files_in_folder()
    
    if not all_items:
        logging.warning(f"Nenhum arquivo encontrado para a fonte '{source_config['name']}'.")
        return pd.DataFrame()
        
    list_of_dataframes = []
    
    arquivos_permitidos = source_config.get("arquivos_para_ler")

    for item in all_items:
        if "folder" in item: continue
        file_name = item['name']

        if arquivos_permitidos and file_name not in arquivos_permitidos:
            continue
        
        if any(keyword in file_name.lower() for keyword in general_config.KEYWORDS_TO_EXCLUDE): continue
        
        df = sp_client.read_excel_sheet(item['id'], file_name)
        
        if df is not None and not df.empty:
            df['Fonte do Arquivo'] = file_name
            list_of_dataframes.append(df)
            logging.info(f"Arquivo '{file_name}' lido e adicionado.")
            
    if not list_of_dataframes:
        logging.warning(f"Nenhum dado válido extraído dos arquivos da fonte '{source_config['name']}'.")
        return pd.DataFrame()
        
    consolidated_df = pd.concat(list_of_dataframes, ignore_index=True)
    logging.info(f"Fonte '{source_config['name']}' consolidada. Total de {len(consolidated_df)} linhas brutas.")
    return consolidated_df

def processar_dados_descargas(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    if df.empty: return df
    logging.info("Processando dados de Descargas (Filtros e Chaves)...")
    cfg = config.DESCARGAS_CONFIG
    
    logging.info("Verificando e tratando notas fiscais múltiplas...")
    df['nota'] = df['nota'].astype(str)
    df['nota'] = df['nota'].str.split('/')
    df = df.explode('nota')
    df['nota'] = df['nota'].str.strip()
    
    df = df[df['produto'].astype(str).str.lower().isin(cfg['products_to_include'])]
    data_limite = datetime.now() - timedelta(days=20)
    df['data'] = pd.to_datetime(df['data'], errors='coerce')
    df = df[df['data'].notna() & (df['data'] > data_limite)]
    
    ### ALTERAÇÃO 1: INÍCIO - Excluir registros de devolução/cancelamento ###
    logging.info("Aplicando filtros para desconsiderar devoluções e cancelamentos...")
    palavras_a_excluir = ['devolução', 'cancelado', 'devolvido', 'cancelada']
    regex_exclusao = '|'.join(palavras_a_excluir)
    
    df['produto'] = df['produto'].astype(str)
    df['status'] = df['status'].astype(str)

    mask_produto = ~df['produto'].str.contains(regex_exclusao, case=False, na=False)
    mask_status = ~df['status'].str.contains(regex_exclusao, case=False, na=False)
    
    df = df[mask_produto & mask_status].copy()
    logging.info(f"DataFrame de descargas filtrado. {len(df)} linhas restantes.")
    ### ALTERAÇÃO 1: FIM ###

    logging.info("Aplicando regra 'de-para' na fonte do arquivo para padronização.")
    df['Fonte Padronizada'] = df['Fonte do Arquivo']
    for key, value in config.FILENAME_MAP.items():
        mask = df['Fonte do Arquivo'].astype(str).str.contains(key, case=False, na=False)
        df.loc[mask, 'Fonte Padronizada'] = value
    
    ### ALTERAÇÃO 2: INÍCIO - Criação das novas chaves ###
    # Chave Primária: nota + produto + Fonte Padronizada
    df['chave_primaria'] = (_normalizar_texto_para_chave(df['nota']) + '_' +
                            _normalizar_texto_para_chave(df['produto']) + '_' +
                            _normalizar_texto_para_chave(df['Fonte Padronizada']))
    
    # Chave Secundária (Fallback): placa + produto + Fonte Padronizada
    df['chave_secundaria_placa'] = (_normalizar_texto_para_chave(df['placa']) + '_' +
                                   _normalizar_texto_para_chave(df['produto']) + '_' +
                                   _normalizar_texto_para_chave(df['Fonte Padronizada']))
    ### ALTERAÇÃO 2: FIM ###
    
    return df

def processar_dados_transportes(df: pd.DataFrame, config: Config) -> pd.DataFrame:
    if df.empty: return df
    logging.info("Processando dados de Transportes (Filtros e Chaves)...")
    cfg = config.TRANSPORTES_CONFIG
    
    df['status'] = df['status'].astype(str)
    status_validos = [s.lower() for s in cfg['status_para_incluir']]
    df = df[df['status'].str.lower().isin(status_validos)].copy()

    ### ALTERAÇÃO 3: INÍCIO - Criação das novas chaves ###
    # Chave Primária: nfe + produto + recebedor
    df['chave_primaria'] = (_normalizar_texto_para_chave(df['nfe']) + '_' +
                            _normalizar_texto_para_chave(df['produto']) + '_' +
                            _normalizar_texto_para_chave(df['recebedor']))

    # Chave Secundária (Fallback): cavalo + produto + recebedor
    df['chave_secundaria_placa'] = (_normalizar_texto_para_chave(df['cavalo']) + '_' +
                                   _normalizar_texto_para_chave(df['produto']) + '_' +
                                   _normalizar_texto_para_chave(df['recebedor']))
    ### ALTERAÇÃO 3: FIM ###

    df['data_de_carregamento'] = pd.to_datetime(df['data_de_carregamento'], errors='coerce')
    
    return df

def cruzar_e_atualizar_transportes(df_transportes: pd.DataFrame, df_descargas: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int], Set[int]]:
    if df_transportes.empty or df_descargas.empty:
        logging.warning("Um dos DataFrames está vazio, pulando a etapa de cruzamento.")
        return df_transportes, {}, set()

    transportes_atualizado = df_transportes.copy()
    transportes_atualizado['fonte_atualizacao'] = 'Não Atualizado'
    
    indices_descargas_usados = set()
    
    contadores = {
        "etapa1_transito_para_aguardando": 0, "etapa1_transito_para_descarregado": 0, "etapa1_aguardando_para_descarregado": 0,
        "etapa2_transito_para_aguardando": 0, "etapa2_transito_para_descarregado": 0, "etapa2_aguardando_para_descarregado": 0,
    }

    descargas_com_indice = df_descargas.copy()
    descargas_com_indice['original_index'] = descargas_com_indice.index

    ### ALTERAÇÃO 4: INÍCIO - Usar a nova chave_primaria para o cruzamento ###
    descargas_unicas_primaria = descargas_com_indice.drop_duplicates(subset=['chave_primaria'], keep='last')
    descargas_map_primaria = descargas_unicas_primaria.set_index('chave_primaria').to_dict('index')
    
    logging.info("Iniciando Etapa 1 de cruzamento (por Chave Primária: Nota + Produto + Origem/Recebedor)...")
    for index, transporte in transportes_atualizado.iterrows():
        match = descargas_map_primaria.get(transporte['chave_primaria'])
        if match:
            indices_descargas_usados.add(match['original_index'])
            status_original = str(transporte['status']).strip().lower()
            novo_status = 'aguardando descarga' if pd.isna(match.get('data_de_descarga')) else 'descarregado'

            if novo_status != status_original:
                transportes_atualizado.loc[index, 'data_chegada'] = match['data']
                transportes_atualizado.loc[index, 'fonte_atualizacao'] = 'Etapa 1 - Chave Primária'

                if 'trânsito' in status_original and novo_status == 'aguardando descarga':
                    transportes_atualizado.loc[index, 'status'] = 'AGUARDANDO DESCARGA'
                    contadores['etapa1_transito_para_aguardando'] += 1
                elif 'trânsito' in status_original and novo_status == 'descarregado':
                    transportes_atualizado.loc[index, 'data_descarga'] = match['data_de_descarga']
                    transportes_atualizado.loc[index, 'status'] = 'DESCARREGADO'
                    contadores['etapa1_transito_para_descarregado'] += 1
                elif 'aguardando' in status_original and novo_status == 'descarregado':
                    transportes_atualizado.loc[index, 'data_descarga'] = match['data_de_descarga']
                    transportes_atualizado.loc[index, 'status'] = 'DESCARREGADO'
                    contadores['etapa1_aguardando_para_descarregado'] += 1

    logging.info("Iniciando Etapa 2 de cruzamento (por Chave Secundária: Placa + Produto + Origem/Recebedor)...")
    descargas_unicas_secundaria = descargas_com_indice.drop_duplicates(subset=['chave_secundaria_placa'], keep='last')
    descargas_map_secundaria = descargas_unicas_secundaria.set_index('chave_secundaria_placa').to_dict('index')
    
    transportes_para_etapa_2 = transportes_atualizado[transportes_atualizado['fonte_atualizacao'] == 'Não Atualizado']
    for index, transporte in transportes_para_etapa_2.iterrows():
        chave_busca_placa = transporte['chave_secundaria_placa']
        match = descargas_map_secundaria.get(chave_busca_placa)
        
        if match:
            if pd.notna(transporte['data_de_carregamento']) and match['data'] >= transporte['data_de_carregamento']:
                indices_descargas_usados.add(match['original_index'])
                status_original = str(transporte['status']).strip().lower()
                novo_status = 'aguardando descarga' if pd.isna(match.get('data_de_descarga')) else 'descarregado'

                if novo_status != status_original:
                    transportes_atualizado.loc[index, 'data_chegada'] = match['data']
                    transportes_atualizado.loc[index, 'fonte_atualizacao'] = 'Etapa 2 - Chave Placa'
                    
                    if 'trânsito' in status_original and novo_status == 'aguardando descarga':
                        transportes_atualizado.loc[index, 'status'] = 'AGUARDANDO DESCARGA'
                        contadores['etapa2_transito_para_aguardando'] += 1
                    elif 'trânsito' in status_original and novo_status == 'descarregado':
                        transportes_atualizado.loc[index, 'data_descarga'] = match['data_de_descarga']
                        transportes_atualizado.loc[index, 'status'] = 'DESCARREGADO'
                        contadores['etapa2_transito_para_descarregado'] += 1
                    elif 'aguardando' in status_original and novo_status == 'descarregado':
                        transportes_atualizado.loc[index, 'data_descarga'] = match['data_de_descarga']
                        transportes_atualizado.loc[index, 'status'] = 'DESCARREGADO'
                        contadores['etapa2_aguardando_para_descarregado'] += 1
    ### ALTERAÇÃO 4: FIM ###
            
    return transportes_atualizado, contadores, indices_descargas_usados

# ==============================================================================
# 4. ORQUESTRADOR PRINCIPAL DA EXECUÇÃO
# ==============================================================================
def main():
    try:
        logging.info("--- INICIANDO PROCESSO COMPLETO DE COLETA E ATUALIZAÇÃO ---")
        Config.validate()
        
        raw_descargas = carregar_e_consolidar_fonte(Config.DESCARGAS_CONFIG, Config)
        df_descargas = processar_dados_descargas(raw_descargas, Config) if not raw_descargas.empty else pd.DataFrame()
        
        raw_transportes = carregar_e_consolidar_fonte(Config.TRANSPORTES_CONFIG, Config)

        # --- Contagem de linhas lidas por arquivo (Transportes) ---
        if not raw_transportes.empty:
            contagem_por_arquivo = raw_transportes['Fonte do Arquivo'].value_counts()
            print("\nLinhas lidas por arquivo de Transporte:")
            for arquivo, qtd in contagem_por_arquivo.items():
                print(f"  - {arquivo}: {qtd} linhas lidas")
            print(f"Total lido (Transportes): {len(raw_transportes)} linhas\n")
        else:
            print("\nNenhum arquivo de Transportes foi lido.\n")
        # --------------------------------------------------------
    

        df_transportes = processar_dados_transportes(raw_transportes, Config) if not raw_transportes.empty else pd.DataFrame()

        df_transportes_final, contadores_detalhados, indices_usados = cruzar_e_atualizar_transportes(df_transportes, df_descargas)
        
        sp_client_transportes = SharePointClient(Config.TRANSPORTES_CONFIG, Config)
        
        df_updates = df_transportes_final[df_transportes_final['fonte_atualizacao'] != 'Não Atualizado'].copy()

        status_range = 'A1:W1'
        COR_VERMELHA = {'color': "#C70C21"}
        COR_AMARELA = {'color': '#FFEB9C'}
        COR_VERDE = {'color': "#069422"}

        if df_updates.empty:
            logging.info("Nenhuma atualização de status necessária. Sinalizando arquivos como 'verificados'.")
            
            if not raw_transportes.empty:
                file_ids_para_sinalizar = set(raw_transportes['__ms_file_id'])
                sheet_name_para_aviso = raw_transportes['__ms_sheet_name'].iloc[0] 
                
                timestamp = datetime.now().strftime('%d/%m/%Y às %H:%M:%S')
                msg_sem_alteracao = f"Atualizado em {timestamp}. Nenhuma alteração de status nesta execução."

                logging.info(f"Sinalizando {len(file_ids_para_sinalizar)} arquivo(s) com mensagem de 'sem alterações'.")
                for file_id in file_ids_para_sinalizar:
                    sp_client_transportes.format_range(file_id, sheet_name_para_aviso, status_range, COR_VERDE)
                    sp_client_transportes.update_range_value(file_id, sheet_name_para_aviso, 'A1', msg_sem_alteracao)
                    time.sleep(1) 
            else:
                logging.info("Nenhum arquivo de Transportes foi lido, então não há onde sinalizar.")
        
        else:
            file_ids_para_atualizar = set(df_updates['__ms_file_id'])
            sheet_name_para_aviso = df_updates['__ms_sheet_name'].iloc[0]
            
            processo_bem_sucedido = False

            try:
                logging.info(f"Sinalizando {len(file_ids_para_atualizar)} arquivo(s) como 'em atualização'...")
                for file_id in file_ids_para_atualizar:
                    sp_client_transportes.format_range(file_id, sheet_name_para_aviso, status_range, COR_VERMELHA)
                    sp_client_transportes.update_range_value(file_id, sheet_name_para_aviso, 'A1', "Atualizando...")
                    time.sleep(1)

                logging.info(f"Iniciando atualização de {len(df_updates)} registros no SharePoint...")
                for _, row in df_updates.iterrows():
                    file_id, sheet_name, row_idx = row['__ms_file_id'], row['__ms_sheet_name'], int(row['__ms_row_index'])
                    
                    sp_client_transportes.update_cell(file_id, sheet_name, row_idx, 'status', row['status'])
                    if pd.notna(row['data_chegada']):
                        sp_client_transportes.update_cell(file_id, sheet_name, row_idx, 'data_chegada', row['data_chegada'].strftime('%Y-%m-%d'))
                    if pd.notna(row['data_descarga']):
                        if isinstance(row['data_descarga'], (datetime, pd.Timestamp)):
                            sp_client_transportes.update_cell(file_id, sheet_name, row_idx, 'data_descarga', row['data_descarga'].strftime('%Y-%m-%d'))
                        else:
                            sp_client_transportes.update_cell(file_id, sheet_name, row_idx, 'data_descarga', str(row['data_descarga']))
                
                logging.info("✅ SUCESSO! Atualizações no SharePoint concluídas.")
                processo_bem_sucedido = True

            except Exception as e:
                logging.critical(f"❌ UM ERRO OCORREU DURANTE AS ATUALIZAÇÕES: {e}", exc_info=True)
                for file_id in file_ids_para_atualizar:
                    sp_client_transportes.format_range(file_id, sheet_name_para_aviso, status_range, COR_AMARELA)
                    msg_erro = f"Falha na atualização. Verifique os logs. Detalhe: {str(e)[:150]}"
                    sp_client_transportes.update_range_value(file_id, sheet_name_para_aviso, 'A1', msg_erro)
                    time.sleep(1)
                raise

            finally:
                if processo_bem_sucedido:
                    logging.info("Sinalizando arquivos como 'atualização concluída'.")
                    
                    total_t_ad = sum(v for k, v in contadores_detalhados.items() if 'transito_para_aguardando' in k)
                    total_ad_d = sum(v for k, v in contadores_detalhados.items() if 'aguardando_para_descarregado' in k)
                    total_t_d = sum(v for k, v in contadores_detalhados.items() if 'transito_para_descarregado' in k)
                    
                    timestamp = datetime.now().strftime('%d/%m/%Y às %H:%M:%S')
                    msg_sucesso = (f"Atualizado em {timestamp}. "
                                   f"Status alterados: {total_t_ad} (Trânsito -> Aguardando), "
                                   f"{total_ad_d + total_t_d} (-> Descarregado)")

                    for file_id in file_ids_para_atualizar:
                        sp_client_transportes.format_range(file_id, sheet_name_para_aviso, status_range, COR_VERDE)
                        sp_client_transportes.update_range_value(file_id, sheet_name_para_aviso, 'A1', msg_sucesso)
                        time.sleep(1)

        os.system('cls' if os.name == 'nt' else 'clear')
        
        print("\n" + "="*80)
        print("                        RESULTADO FINAL - DADOS DE TRANSPORTE ATUALIZADOS")
        print("="*80)
        print(f"\nTotal de registros de Transporte (pós-filtro): {len(df_transportes)}")
        print(f"Total de registros de Descarga para consulta: {len(df_descargas)}")
        
        if contadores_detalhados:
            print("-" * 80)
            print("                                        RELATÓRIO DE MUDANÇAS DE STATUS REAIS")
            print("-" * 80)
            total_etapa1 = sum(v for k, v in contadores_detalhados.items() if k.startswith('etapa1'))
            total_etapa2 = sum(v for k, v in contadores_detalhados.items() if k.startswith('etapa2'))
            print(f"Etapa 1 (Chave Primária): {total_etapa1} atualizações reais")
            print(f"  - 'Em Trânsito' -> 'Aguardando Descarga': {contadores_detalhados.get('etapa1_transito_para_aguardando', 0)}")
            print(f"  - 'Em Trânsito' -> 'Descarregado': {contadores_detalhados.get('etapa1_transito_para_descarregado', 0)}")
            print(f"  - 'Aguardando Descarga' -> 'Descarregado': {contadores_detalhados.get('etapa1_aguardando_para_descarregado', 0)}")
            print("-" * 80)
            print(f"Etapa 2 (Chave Placa): {total_etapa2} atualizações reais")
            print(f"  - 'Em Trânsito' -> 'Aguardando Descarga': {contadores_detalhados.get('etapa2_transito_para_aguardando', 0)}")
            print(f"  - 'Em Trânsito' -> 'Descarregado': {contadores_detalhados.get('etapa2_transito_para_descarregado', 0)}")
            print(f"  - 'Aguardando Descarga' -> 'Descarregado': {contadores_detalhados.get('etapa2_aguardando_para_descarregado', 0)}")
            print("-" * 80)
            print(f"Total de ATUALIZAÇÕES REAIS: {total_etapa1 + total_etapa2}")

        # ===== SEÇÃO DO RELATÓRIO DE DIVERGÊNCIAS =====
        logging.info("Iniciando a geração do relatório de divergências...")
        sp_client_relatorio = SharePointClient(Config.RELATORIO_DIVERGENCIA_CONFIG, Config)

        if not df_descargas.empty:
            df_descargas_nao_usadas = df_descargas.drop(index=list(indices_usados))
            
            hoje_str = datetime.now().strftime('%Y-%m-%d')
            raw_transportes['data_descarga'] = pd.to_datetime(raw_transportes['data_descarga'], errors='coerce')

            df_descarregados_hoje = raw_transportes[
                (raw_transportes['status'].str.lower() == 'descarregado') &
                (raw_transportes['data_descarga'].dt.strftime('%Y-%m-%d') == hoje_str)
            ].copy()

            chaves_descarregadas_hoje_primaria = set()
            chaves_descarregadas_hoje_secundaria = set()

            ### ALTERAÇÃO 5: INÍCIO - Usar novas chaves para o relatório de divergência ###
            if not df_descarregados_hoje.empty:
                df_descarregados_hoje['chave_primaria'] = (_normalizar_texto_para_chave(df_descarregados_hoje['nfe']) + '_' +
                                                          _normalizar_texto_para_chave(df_descarregados_hoje['produto']) + '_' +
                                                          _normalizar_texto_para_chave(df_descarregados_hoje['recebedor']))
                chaves_descarregadas_hoje_primaria = set(df_descarregados_hoje['chave_primaria'])
                
                df_descarregados_hoje['chave_secundaria_placa'] = (_normalizar_texto_para_chave(df_descarregados_hoje['cavalo']) + '_' +
                                                                   _normalizar_texto_para_chave(df_descarregados_hoje['produto']) + '_' +
                                                                   _normalizar_texto_para_chave(df_descarregados_hoje['recebedor']))
                chaves_descarregadas_hoje_secundaria = set(df_descarregados_hoje['chave_secundaria_placa'])

            df_descargas_nao_usadas['data_de_descarga'] = pd.to_datetime(df_descargas_nao_usadas['data_de_descarga'], errors='coerce')
            hoje = datetime.now().date()
            
            filtro_data = (df_descargas_nao_usadas['data_de_descarga'].isna()) | (df_descargas_nao_usadas['data_de_descarga'].dt.date == hoje)
            df_relatorio = df_descargas_nao_usadas[filtro_data].copy()

            if not df_relatorio.empty:
                filtro_primaria_nao_encontrada = ~df_relatorio['chave_primaria'].isin(chaves_descarregadas_hoje_primaria)
                filtro_secundaria_nao_encontrada = ~df_relatorio['chave_secundaria_placa'].isin(chaves_descarregadas_hoje_secundaria)
                df_relatorio = df_relatorio[filtro_primaria_nao_encontrada & filtro_secundaria_nao_encontrada]
            ### ALTERAÇÃO 5: FIM ###

            df_relatorio_final = pd.DataFrame()
            if not df_relatorio.empty:
                # FILTRO: Hidratado (Case Insensitive)
                termo_busca = 'hidratado'
                filtro_produto = df_relatorio['produto'].astype(str).str.contains(termo_busca, case=False, na=False)
                df_relatorio_final = df_relatorio[filtro_produto]
            
            if not df_relatorio_final.empty:
                logging.info(f"Encontradas {len(df_relatorio_final)} linhas para o relatório de divergências.")
                colunas_relatorio = {
                    'data': 'Data Chegada', 'placa': 'Placa', 'motorista': 'Motorista', 'quantidade_nf':'Quantidade', 'nota': 'Nota Fiscal', 
                    'produto': 'Produto', 'Fonte Padronizada': 'Origem Descarga', 'status': 'Status Descarga',
                    'data_de_descarga': 'Data de Descarga','empresa': 'empresa'
                }
                df_para_escrever = df_relatorio_final[list(colunas_relatorio.keys())].rename(columns=colunas_relatorio)
                
                # USO DO NOVO MÉTODO ROBUSTO (BASEADO NO SCRIPT PARALELO)
                sp_client_relatorio.update_specific_sheet(df_para_escrever, sheet_name_target='Divergencias')

            else:
                logging.info("Nenhuma divergência encontrada para o relatório.")
                df_vazio = pd.DataFrame(columns=[f"Nenhuma divergência de Hidratado encontrada na execução de {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}."])
                sp_client_relatorio.update_specific_sheet(df_vazio, sheet_name_target='Divergencias')
        else:
            logging.info("Fonte de Descargas vazia, pulando relatório de divergências.")
            df_vazio = pd.DataFrame(columns=[f"Fonte de descargas vazia. Execução de {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}."])
            sp_client_relatorio.update_specific_sheet(df_vazio, sheet_name_target='Divergencias')
        
        print("="*80)
        print("                RELATÓRIO DE DIVERGÊNCIAS GERADO NO SHAREPOINT")
        print("="*80)
            
    except Exception as e:
        logging.critical(f"❌ PROCESSO INTERROMPIDO POR ERRO CRÍTICO: {e}", exc_info=False)
        sys.exit(1)

if __name__ == "__main__":
    main()