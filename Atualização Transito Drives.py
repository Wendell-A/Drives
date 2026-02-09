import os
import logging
import pandas as pd
import requests
import unicodedata 
import time
from dotenv import load_dotenv
from datetime import date, timedelta
from typing import List, Dict, Any

# ==============================================================================
# CONFIGURAÇÃO E LOGGING
# ==============================================================================
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

load_dotenv()

class Config:
    # Credenciais
    TENANT_ID: str = os.getenv("TENANT_ID")
    CLIENT_ID: str = os.getenv("CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
    HOSTNAME: str = os.getenv("HOSTNAME")
    
    # Caminhos
    SITE_PATH: str = "sites/Transportes" 
    TARGET_SHEET_NAME: str = "Base" 
    
    Bsoft_FILENAME: str = "Relatório de NF Bsoft.xlsx"
    Bsoft_SHEET_NAME: str = "Sheet1"
    
    # Arquivos de Transporte Permitidos
    ARQUIVOS_PERMITIDOS: List[str] = [
        "FORM-PPL-000 - Fitplan Hidratado - RJ.xlsx",
        "FORM-PPL-000 - Fitplan Hidratado - SP.xlsx",
        "FORM-PPL-000 - Fitplan Anidro - SP.xlsx",
        "FORM-PPL-000 - Fitplan Anidro - RJ.xlsx",
        'FORM-PPL-000 - Fitplan Biodiesel.xlsx',
        "FORM-PPL-000 - Gasolina.xlsx",
        "FORM-PPL-000 - Diesel e Insumos.xlsx"
    ]

    # Mapeamento de Colunas
    COLUNAS_TRANSPORTE: List[str] = [
        "sm", "data_prev_carregamento", "expedidor", "cidade_origem", "ufo",
        "destinatario_venda", "destinatario", "recebedor", "cidade_destino", "ufd",
        "produto", "motorista", "cavalo", "carreta1", "carreta2", "transportadora",
        "nfe", "volume_l", "data_de_carregamento", "horario_de_carregamento",
        "data_chegada", "data_descarga", "status"
    ]

    @staticmethod
    def validar():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET, Config.HOSTNAME]):
            raise ValueError("❌ Variáveis de ambiente (.env) incompletas.")

    @staticmethod
    def get_col_letter(col_name: str) -> str:
        try:
            idx = Config.COLUNAS_TRANSPORTE.index(col_name)
            return chr(65 + idx)
        except ValueError:
            return None

# ==============================================================================
# CLIENTE SHAREPOINT
# ==============================================================================
class SharePointClient:
    def __init__(self, config: Config):
        self.config = config
        self.access_token = self._get_token()
        self.api_site = f"{self.config.HOSTNAME}:/{self.config.SITE_PATH}"
        logging.info("🔑 Autenticando no SharePoint...")
        self.site_id = self._get_id('sites', self.api_site)
        self.drive_id = self._get_main_drive_id()

    def _get_token(self) -> str:
        url = f"https://login.microsoftonline.com/{self.config.TENANT_ID}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.config.CLIENT_ID,
            "client_secret": self.config.CLIENT_SECRET,
            "scope": "https://graph.microsoft.com/.default"
        }
        r = requests.post(url, data=data)
        r.raise_for_status()
        return r.json()["access_token"]

    def _api_get(self, url: str) -> Any:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        return r.json()

    def _api_patch(self, url: str, json_data: Dict) -> Any:
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        r = requests.patch(url, headers=headers, json=json_data)
        r.raise_for_status()
        return r.json()

    def _get_id(self, resource: str, path: str) -> str:
        return self._api_get(f"https://graph.microsoft.com/v1.0/{resource}/{path}")['id']

    def _get_main_drive_id(self) -> str:
        drives = self._api_get(f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives")["value"]
        for d in drives:
            if d.get('name') == 'Documentos': return d['id']
        raise Exception("Biblioteca 'Documentos' não encontrada.")

    def get_root_items(self) -> List[Dict]:
        return self._api_get(f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root/children")["value"]

    def get_item_id_by_path(self, path: str) -> str:
        return self._api_get(f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root:/{path}")['id']

    def read_excel(self, item_id: str, sheet_name: str, colunas_esperadas: List[str] = None) -> pd.DataFrame:
        try:
            sheets = self._api_get(f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets")["value"]
            actual_sheet = next((s['name'] for s in sheets if s['name'].lower() == sheet_name.lower()), None)
            if not actual_sheet and sheet_name.lower() == 'sheet1' and sheets: actual_sheet = sheets[0]['name'] 
            if not actual_sheet: return None

            # TENTATIVA OTIMIZADA: Range A1:Z8000 para evitar Gateway Timeout (504)
            try:
                url_range = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{actual_sheet}/range(address='A1:Z8000')"
                data_json = self._api_get(url_range)
                values = data_json.get('values', [])
            except requests.exceptions.HTTPError as e:
                # Se ainda der erro de limite ou timeout, tenta por blocos
                if e.response.status_code in [504, 502, 429] or "RangeExceedsLimit" in str(e):
                    logging.warning(f"⚠️ Timeout ou limite atingido em {item_id}. Lendo em blocos...")
                    values = self._read_in_chunks(item_id, actual_sheet)
                else: raise e

            if not values or len(values) < 2: return None
            
            # Converte para DataFrame e garante apenas colunas até Z (26 colunas)
            df = pd.DataFrame(values[1:], columns=values[0])
            df = df.iloc[:, :26] 
            
            # Limpeza: remove linhas totalmente vazias que o range fixo pode trazer
            df = df.dropna(how='all').reset_index(drop=True)
            
            if colunas_esperadas:
                if len(df.columns) >= len(colunas_esperadas):
                    df = df.iloc[:, :len(colunas_esperadas)]
                    df.columns = colunas_esperadas

            df['__ms_file_id'] = item_id
            df['__ms_sheet_name'] = actual_sheet
            df['__excel_row_num'] = range(2, len(df) + 2)
            return df
        except Exception as e:
            logging.error(f"Erro ao ler Excel ({item_id}): {e}")
            return None

    def _read_in_chunks(self, item_id: str, sheet_name: str) -> List[List]:
        full_data = []; chunk_size = 2000; row = 1
        # Lê até 8000 linhas em blocos de 2000
        while row <= 8001:
            addr = f"A{row}:Z{row + chunk_size - 1}"
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{sheet_name}/range(address='{addr}')"
            try:
                res = self._api_get(url); vals = res.get('values', [])
                if not vals: break
                
                # Verifica se o bloco tem conteúdo real
                has_content = any(any(str(c).strip() for c in v) for v in vals)
                if not has_content: break
                
                # Adiciona dados (pula cabeçalho se não for a primeira linha)
                full_data.extend(vals if row == 1 else vals[1:])
                row += chunk_size
                time.sleep(0.2)
            except: break
        return full_data

    def update_excel_row(self, item_id: str, sheet: str, row_num: int, updates: Dict[str, Any]):
        for col_name, value in updates.items():
            col_letter = Config.get_col_letter(col_name)
            if not col_letter: continue
            address = f"{col_letter}{row_num}"
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{sheet}/range(address='{address}')"
            payload = { "values": [[value]] }
            
            # Lógica de Tentativas (Retry) para evitar o erro 504 no Update
            for tentativa in range(3):
                try:
                    self._api_patch(url, payload)
                    time.sleep(0.5) # Aumentado levemente para dar fôlego ao Excel
                    break 
                except Exception as e:
                    if tentativa < 2:
                        logging.warning(f"⚠️ Falha ao atualizar {address} (Tentativa {tentativa+1}/3). Aguardando...")
                        time.sleep(2) # Espera 2 segundos antes de tentar de novo
                    else:
                        logging.error(f"❌ Erro persistente ao atualizar {address}: {e}")

# ==============================================================================
# PROCESSADOR DE DADOS
# ==============================================================================
class DataProcessor:
    @staticmethod
    def limpar_nf(series: pd.Series) -> pd.Series:
        if series is None: return pd.Series(dtype='object')
        return (
            series.astype(str)
            .str.replace(r'\.0$', '', regex=True)
            .str.strip()
            .replace(['nan', 'None', 'NAN'], '')
        )

    @staticmethod
    def normalizar_txt(series: pd.Series) -> pd.Series:
        if series is None: return pd.Series(dtype='object')
        return series.astype(str).str.normalize('NFKD').str.encode('ascii', 'ignore').str.decode('utf-8').str.strip().str.upper()

    @staticmethod
    def limpar_placa(series: pd.Series) -> pd.Series:
        if series is None: return pd.Series(dtype='object')
        return series.astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)

    @staticmethod
    def _tratar_data_excel(series: pd.Series) -> pd.Series:
        if series is None: return pd.Series(dtype='object')
        
        # 1. Tenta converter valores numéricos do Excel (ex: 45322.0)
        datas_numericas = pd.to_numeric(series.astype(str).str.replace(',', '.'), errors='coerce')
        datas_convertidas = pd.to_datetime(datas_numericas, unit='D', origin='1899-12-30', errors='coerce')
        
        # 2. Tenta converter texto com formato fixo (evita o UserWarning)
        datas_texto = pd.to_datetime(series, format='%d/%m/%Y', errors='coerce')
        
        # 3. Se ainda houver NaT (falha no formato fixo), tenta o modo flexível
        mask_faltante = datas_texto.isna() & series.notna()
        if mask_faltante.any():
            datas_flexiveis = pd.to_datetime(series[mask_faltante], dayfirst=True, errors='coerce')
            datas_texto = datas_texto.fillna(datas_flexiveis)
            
        return datas_convertidas.fillna(datas_texto)

    @staticmethod
    def preparar_transporte(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("🔧 Preparando Transporte...")
        df['__data_temp'] = DataProcessor._tratar_data_excel(df['data_de_carregamento'])
        df['__data_prev_temp'] = DataProcessor._tratar_data_excel(df['data_prev_carregamento'])
        
        ontem = pd.to_datetime(date.today() - timedelta(days=1)).normalize()
        df = df[(df['__data_temp'].isna()) | (df['__data_temp'] >= ontem)].copy()
        
        df['produto_norm'] = DataProcessor.normalizar_txt(df['produto'])
        df['nfe'] = DataProcessor.limpar_nf(df['nfe'])
        df['chave_dedup'] = df['produto_norm'] + "_" + df['nfe']
        return df

    @staticmethod
    def preparar_bsoft(df: pd.DataFrame) -> pd.DataFrame:
        logging.info("🔧 Preparando Bsoft...")
        df.columns = [str(c).lower().strip() for c in df.columns]

        col_prod = next((c for c in df.columns if c in ['[item] descrição', 'produto']), None)
        col_p1   = next((c for c in df.columns if c in ['placa1', 'placa do veículo']), None)
        col_nf   = next((c for c in df.columns if c in ['número', 'notas fiscais', 'numero']), None)
        col_data = next((c for c in df.columns if c in ['data emissão', 'data de emissão']), None)
        col_hora = next((c for c in df.columns if c in ['horario de carregamento']), None)
        col_vol  = next((c for c in df.columns if c in ['[item] quantidade', 'volume', 'peso']), None)

        if not col_prod: return pd.DataFrame()

        ontem = pd.to_datetime(date.today() - timedelta(days=1)).normalize()

        if col_data:
            df['__data_emissao'] = DataProcessor._tratar_data_excel(df[col_data]).dt.normalize()
            df = df[df['__data_emissao'].notna() & (df['__data_emissao'] >= ontem)].copy()
        else:
            df['__data_emissao'] = pd.NaT

        if df.empty: return pd.DataFrame()

        df['produto_norm'] = DataProcessor.normalizar_txt(df[col_prod])
        df['número'] = DataProcessor.limpar_nf(df[col_nf]) if col_nf else ""
        df['chave_dedup'] = df['produto_norm'] + "_" + df['número']
        df['placa1_norm'] = DataProcessor.limpar_placa(df[col_p1]) if col_p1 else ""
        df['chave_match_bsoft'] = df['produto_norm'] + "_" + df['placa1_norm']

        df['bsoft_data'] = df['__data_emissao'].dt.strftime('%d/%m/%Y').fillna('')
        df['bsoft_hora'] = df[col_hora].astype(str).replace('nan', '') if col_hora else ""
        df['bsoft_vol']  = df[col_vol] if col_vol else ""
        return df

# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    try:
        Config.validar(); sp = SharePointClient(Config)

        # 1. LER TRANSPORTE
        logging.info("📂 Lendo arquivos de Transporte...")
        arquivos = sp.get_root_items()
        lista_dfs = []
        for arq in arquivos:
            if arq.get('name') in Config.ARQUIVOS_PERMITIDOS:
                df = sp.read_excel(arq['id'], Config.TARGET_SHEET_NAME, Config.COLUNAS_TRANSPORTE)
                if df is not None:
                    df['__arquivo'] = arq['name']
                    lista_dfs.append(df)

        if not lista_dfs: 
            logging.info("ℹ️ Nenhum dado de transporte encontrado.")
            return

        df_transporte = pd.concat(lista_dfs, ignore_index=True)
        df_transporte = DataProcessor.preparar_transporte(df_transporte)

        # 2. BLOQUEIO GLOBAL NF
        nfs_ja_usadas = set(df_transporte['nfe'].loc[lambda x: x != ''])

        # 3. LER BSOFT
        logging.info("📄 Lendo Bsoft...")
        bsoft_id = sp.get_item_id_by_path(Config.Bsoft_FILENAME)
        df_bsoft = sp.read_excel(bsoft_id, Config.Bsoft_SHEET_NAME)
        if df_bsoft is None: return
        df_bsoft = DataProcessor.preparar_bsoft(df_bsoft)

        if df_bsoft.empty:
            logging.info("ℹ️ Bsoft sem dados novos para processar.")
            return

        # 4. FILTRAR SOBRAS BSOFT
        chaves_existentes = set(df_transporte['chave_dedup'].unique())
        df_bsoft_sobras = df_bsoft[
            (~df_bsoft['chave_dedup'].isin(chaves_existentes)) &
            (~df_bsoft['número'].isin(nfs_ja_usadas))
        ].copy()

        # 5. FILTRAR E PRIORIZAR PROGRAMADOS
        status_norm = DataProcessor.normalizar_txt(df_transporte['status'])
        df_programados = df_transporte[
            (status_norm == 'PROGRAMADO') & (df_transporte['nfe'] == '')
        ].copy()

        df_programados['cavalo_norm'] = DataProcessor.limpar_placa(df_programados['cavalo'])
        df_programados['chave_match_transp'] = df_programados['produto_norm'] + "_" + df_programados['cavalo_norm']

        df_programados = df_programados.sort_values(
            by=['__data_prev_temp', '__excel_row_num'], 
            ascending=[True, True]
        )

        df_programados_unicos = df_programados.drop_duplicates(
            subset=['chave_match_transp'], 
            keep='first'
        )

        # 6. MATCH TRANSPORTE × BSOFT
        df_match = pd.merge(
            df_programados_unicos,
            df_bsoft_sobras,
            left_on='chave_match_transp',
            right_on='chave_match_bsoft',
            how='inner',
            suffixes=('_transp', '_bsoft')
        )

        logging.info(f"🎯 Matches únicos encontrados: {len(df_match)}")

        # 7. ATUALIZAÇÃO
        if df_match.empty: return
        count = 0
        for _, row in df_match.iterrows():
            file_id = row['__ms_file_id_transp']
            sheet_name = row['__ms_sheet_name_transp']
            row_num = row['__excel_row_num_transp']
            nova_nfe = row['número']

            if file_id == bsoft_id or nova_nfe in nfs_ja_usadas: continue

            updates = {
                "nfe": nova_nfe,
                "volume_l": row['bsoft_vol'],
                "data_de_carregamento": row['bsoft_data'],
                "horario_de_carregamento": row['bsoft_hora'],
                "status": "EM TRÂNSITO"
            }

            logging.info(f"💾 Atualizando {row['__arquivo']} | Linha {row_num} | NF {nova_nfe}")
            sp.update_excel_row(file_id, sheet_name, row_num, updates)
            nfs_ja_usadas.add(nova_nfe)
            count += 1

        logging.info(f"✅ Finalizado: {count} viagens atualizadas.")

    except Exception as e:
        logging.critical(f"🔥 Erro fatal: {e}")

if __name__ == "__main__":
    main()