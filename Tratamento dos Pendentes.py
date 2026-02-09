# -*- coding: utf-8 -*-

# --- Bibliotecas necessárias ---
import pandas as pd
import os
import io
import numpy as np
import requests
from datetime import datetime
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

# ==============================================================================
# 1. CONFIGURAÇÃO SHAREPOINT (CLASSES DE UPLOAD)
# ==============================================================================
class Config:
    """⚙️ Configurações do SharePoint e Azure AD."""
    TENANT_ID: str = os.getenv("TENANT_ID")
    CLIENT_ID: str = os.getenv("CLIENT_ID")
    CLIENT_SECRET: str = os.getenv("CLIENT_SECRET")
    HOSTNAME: str = os.getenv("HOSTNAME")
    
    # Configuração do Destino
    SITE_PATH: str = "/sites/Transportes"
    DRIVE_NAME: str = "Documentos"
    TARGET_FOLDER: str = "Disponibilidade"

    @staticmethod
    def validate():
        if not all([Config.TENANT_ID, Config.CLIENT_ID, Config.CLIENT_SECRET, Config.HOSTNAME]):
            print("⚠️ AVISO: Credenciais do SharePoint (Env) não encontradas. O upload será cancelado.")
            return False
        return True

class SharePointUploader:
    """Gerencia o upload de arquivos para o SharePoint."""
    def __init__(self, config: Config):
        self.config = config
        self.access_token = self._get_access_token()
        if self.access_token:
            self.site_id = self._get_site_id()
            self.drive_id = self._get_drive_id()

    def _api_request(self, method: str, url: str, json: dict = None, data=None, headers: dict = None):
        if not headers:
            headers = {}
        headers["Authorization"] = f"Bearer {self.access_token}"
        
        response = requests.request(method, url, headers=headers, json=json, data=data)
        if response.status_code not in [200, 201]:
            raise Exception(f"Erro API {response.status_code}: {response.text}")
        return response.json()

    def _get_access_token(self) -> str:
        try:
            url = f"https://login.microsoftonline.com/{self.config.TENANT_ID}/oauth2/v2.0/token"
            data = {
                "client_id": self.config.CLIENT_ID, 
                "scope": "https://graph.microsoft.com/.default",
                "client_secret": self.config.CLIENT_SECRET, 
                "grant_type": "client_credentials"
            }
            response = requests.post(url, data=data)
            response.raise_for_status()
            return response.json()["access_token"]
        except Exception as e:
            print(f"❌ Erro ao obter token SharePoint: {e}")
            return None

    def _get_site_id(self) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{self.config.HOSTNAME}:{self.config.SITE_PATH}"
        return self._api_request('get', url)["id"]

    def _get_drive_id(self) -> str:
        url = f"https://graph.microsoft.com/v1.0/sites/{self.site_id}/drives"
        drives = self._api_request('get', url).get("value", [])
        for drive in drives:
            if drive['name'].lower() == self.config.DRIVE_NAME.lower(): return drive['id']
        raise FileNotFoundError(f"Biblioteca '{self.config.DRIVE_NAME}' não encontrada.")

    def upload_dataframe(self, df: pd.DataFrame, filename: str):
        if not self.access_token: return

        print(f"--- Iniciando Upload SharePoint: {filename} ---")
        try:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Base')
            buffer.seek(0)
            file_content = buffer.getvalue()

            folder_path = f"{self.config.TARGET_FOLDER}/{filename}"
            url_path = requests.utils.quote(folder_path)
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root:/{url_path}:/content"

            headers = {"Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}
            self._api_request('put', url, data=file_content, headers=headers)
            print(f"✅ Arquivo '{filename}' salvo com sucesso no SharePoint.")

        except Exception as e:
            print(f"❌ Falha no upload para o SharePoint: {e}")

# ==============================================================================
# 2. FUNÇÕES DE ETL
# ==============================================================================

def carregar_arquivos_sense(home_dir):
    print("--- 1. Carregando todos os arquivos (versão Qlik Sense) ---")
    
    def ler_csv(nome_arquivo, pasta='De Para', encoding='latin-1'):
        caminho = os.path.join(home_dir, 'Documentos', pasta, nome_arquivo)
        df = pd.read_csv(caminho, sep=';', encoding=encoding)
        df.columns = df.columns.str.strip()
        print(f"✅ Arquivo {nome_arquivo} carregado.")
        return df

    dfs = {
        'vendas': pd.read_excel(os.path.join(home_dir, 'Documentos', 'qlik_sense.xlsx')),
        'comprador': ler_csv('Comprador_De_Para.csv'),
        'deposito': ler_csv('Deposito_De_Para.csv'),
        'empresa': ler_csv('Empresa.csv'),
        'segmentos': ler_csv('Segmento.csv', encoding='utf-8-sig'),
        'produto_acabado': ler_csv('Bases Produto Acabado.csv')
    }
    dfs['vendas'].columns = dfs['vendas'].columns.str.strip()
    
    # Removendo duplicatas
    dfs['produto_acabado']['chave_estoque'] = dfs['produto_acabado']['Produto'].astype(str).str.upper() + '&' + dfs['produto_acabado']['Base'].astype(str).str.upper()
    dfs['comprador'] = dfs['comprador'].drop_duplicates(subset=['CNPJ'], keep='first')
    dfs['deposito'] = dfs['deposito'].drop_duplicates(subset=['De'], keep='first')
    dfs['empresa'] = dfs['empresa'].drop_duplicates(subset=['De'], keep='first')
    dfs['segmentos'] = dfs['segmentos'].drop_duplicates(subset=['CNPJ'], keep='first')
    dfs['produto_acabado'] = dfs['produto_acabado'].drop_duplicates(subset=['chave_estoque'], keep='first')
    
    return dfs

def limpar_colunas_numericas_sense(df):
    print("--- 2. Ajustando tipagem da coluna Quantidade ---")
    if 'Quantidade' in df.columns:
        df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)
    return df

def mesclar_dados_sense(dfs):
    print("--- 2. Cruzando dados (PROCVs) ---")
    df_vendas = dfs['vendas'].copy()
    
    df_vendas = pd.merge(df_vendas, dfs['empresa'][['De', '2_EMPRESA']], left_on='Empresa', right_on='De', how='left').drop(columns=['De'])
    df_vendas = pd.merge(df_vendas, dfs['deposito'][['De', '2_EXPEDIDOR']], left_on='Cod Deposito', right_on='De', how='left').drop(columns=['De'])
    
    df_vendas['CNPJ Comprador'] = df_vendas['CNPJ Comprador'].astype(str)
    dfs['comprador']['CNPJ'] = dfs['comprador']['CNPJ'].astype(str)
    df_vendas = pd.merge(df_vendas, dfs['comprador'][['CNPJ', '2_DESTINATÁRIO', '2_DESTINATÁRIO TIPO']], left_on='CNPJ Comprador', right_on='CNPJ', how='left').drop(columns='CNPJ')
    
    df_vendas['2_DESTINATÁRIO TIPO'] = df_vendas['2_DESTINATÁRIO TIPO'].fillna('CLIENTES')
    df_vendas['2_DESTINATÁRIO'] = df_vendas['2_DESTINATÁRIO'].fillna('CLIENTES')
    return df_vendas

def aplicar_regras_de_negocio_sense(df_vendas, dfs):
    print("--- 3. Aplicando regras de negócio ---")
    df_vendas = pd.merge(df_vendas, dfs['segmentos'][['CNPJ', 'Segmento']], left_on='CNPJ Comprador', right_on='CNPJ', how='left').drop(columns='CNPJ')
    
    # Regras de Segmento
    df_vendas.loc[df_vendas['RzSocial Comprador'].str.contains('Marangoni', case=False, na=False), 'Segmento'] = 'GOOIL'
    df_vendas.loc[(df_vendas['Segmento'].isna()) & (df_vendas['2_DESTINATÁRIO'] != 'CLIENTES'), 'Segmento'] = 'Intercompany'
    df_vendas.loc[(df_vendas['Segmento'].isna()) & (df_vendas['RzSocial Comprador'].str.contains('Posto', case=False, na=False)), 'Segmento'] = 'Posto'
    df_vendas.loc[(df_vendas['Segmento'].isna()) & (df_vendas['Empresa'] == 'Maragoni'), 'Segmento'] = 'GOOIL'
    df_vendas.loc[(df_vendas['Segmento'].isna()) & (df_vendas['Descricao Produto'].str.contains('Gasolina A|Diesel A', case=False, na=False)), 'Segmento'] = 'congerenere'
    df_vendas['Segmento'] = df_vendas['Segmento'].fillna('B2B')

    # Regras de Produto_1
    conditions = [
        df_vendas['Descricao Produto'].str.contains('Gasolina C', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Diesel B S10', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Gasolina A', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Diesel A S10', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Diesel B S500', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Diesel A S500', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Anidro', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Biodiesel', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Hidratado', case=False, na=False),
        df_vendas['Descricao Produto'].str.contains('Maritimo', case=False, na=False)
    ]
    choices = ['Gasolina C', 'Diesel B S10', 'Gasolina A', 'Diesel A S10', 'Diesel B S500', 'Diesel A S500', 'Anidro', 'Biodiesel', 'Hidratado', 'MGO']
    df_vendas['Produto_1'] = np.select(conditions, choices, default=None)

    # Armazena Acabado
    df_vendas['chave_vendas'] = df_vendas['Produto_1'].astype(str).str.upper() + '&' + df_vendas['2_EXPEDIDOR'].astype(str).str.upper()
    df_temp = pd.merge(df_vendas, dfs['produto_acabado'][['chave_estoque']], left_on='chave_vendas', right_on='chave_estoque', how='left')
    df_vendas['Armazena Acabado'] = np.where(df_temp['chave_estoque'].notna(), 'Sim', 'Não')
    
    lista_override = ['Hidratado', 'MGO', 'Gasolina A', 'Diesel A S10', 'Diesel A S500', 'Anidro', 'Biodiesel']
    df_vendas.loc[df_vendas['Produto_1'].isin(lista_override), 'Armazena Acabado'] = 'Sim'
    
    # Tipo de Vendas
    cond_congenere_produto = df_vendas['Descricao Produto'].str.contains('Gasolina A|Diesel A S10|Diesel A S500|Anidro|Biodiesel', case=False, na=False)
    df_vendas['Tipo de vendas'] = np.where(cond_congenere_produto, 'Venda Congênere', 'Produto Acabado')
    df_vendas.loc[df_vendas['2_DESTINATÁRIO TIPO'].str.upper() == 'INTERCOMPANY', 'Tipo de vendas'] = 'INTERCOMPANY'
    
    return df_vendas.drop(columns=['chave_vendas'])

def criar_df_convertido_sense(df_tratado):
    print("--- 4. Criando a base de dados 'Convertido' ---")
    df_sim = df_tratado[df_tratado['Armazena Acabado'] == 'Sim'].copy()
    df_nao = df_tratado[df_tratado['Armazena Acabado'] != 'Sim'].copy()
    
    df_gc_a_converter = df_nao[df_nao['Produto_1'] == 'Gasolina C'].copy()
    df_db_a_converter = df_nao[df_nao['Produto_1'].str.contains('Diesel B', na=False)].copy()

    lista_cestas = []
    if not df_gc_a_converter.empty:
        g_a = df_gc_a_converter.copy(); g_a['Produto_1'] = 'Gasolina A'; g_a['Quantidade'] *= 0.70; lista_cestas.append(g_a)
        ani = df_gc_a_converter.copy(); ani['Produto_1'] = 'Anidro'; ani['Quantidade'] *= 0.30; lista_cestas.append(ani)

    if not df_db_a_converter.empty:
        d_a = df_db_a_converter.copy(); d_a['Produto_1'] = d_a['Produto_1'].str.replace('B', 'A', regex=False); d_a['Quantidade'] *= 0.85; lista_cestas.append(d_a)
        bio = df_db_a_converter.copy(); bio['Produto_1'] = 'Biodiesel'; bio['Quantidade'] *= 0.15; lista_cestas.append(bio)
    
    return pd.concat([df_sim] + lista_cestas, ignore_index=True)

# ==============================================================================
# 3. EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    try:
        home_dir = os.path.expanduser('~')
        caminho_vendas_original = os.path.join(home_dir, 'Documentos', 'qlik_sense.xlsx')
        
        # Metadata
        mod_time = os.path.getmtime(caminho_vendas_original)
        data_hora_modificacao = datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
        
        # ETL
        dataframes = carregar_arquivos_sense(home_dir)
        dataframes['vendas'] = limpar_colunas_numericas_sense(dataframes['vendas'])
        df_tratado = mesclar_dados_sense(dataframes)
        df_tratado = aplicar_regras_de_negocio_sense(df_tratado, dataframes)
        df_tratado['Data_Hora_Atualizacao'] = data_hora_modificacao
        
        df_convertido = criar_df_convertido_sense(df_tratado)
        df_convertido['Data_Hora_Atualizacao'] = data_hora_modificacao
        
        # Filtro de Colunas
        colunas_finais = [
            'Quantidade', 'Numero Pedido', 'Data Prevista Fat', 'RzSocial Comprador', 
            'CNPJ Comprador', '2_EMPRESA', '2_EXPEDIDOR', '2_DESTINATÁRIO', 
            '2_DESTINATÁRIO TIPO', 'Segmento', 'Produto_1', 'Armazena Acabado', 'Tipo de vendas',
            'Data_Hora_Atualizacao'
        ]

        df_acabado_final = df_tratado[[c for c in colunas_finais if c in df_tratado.columns]].copy()
        df_convertido_final = df_convertido[[c for c in colunas_finais if c in df_convertido.columns]].copy()

        # Upload SharePoint
        if Config.validate():
            sp_uploader = SharePointUploader(Config)
            sp_uploader.upload_dataframe(df_acabado_final, "Base_Acabado_Sense.xlsx")
            sp_uploader.upload_dataframe(df_convertido_final, "Base_Convertido_Sense.xlsx")

    except Exception as e:
        print(f"❌ Ocorreu um erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()