# -*- coding: utf-8 -*-

# --- Bibliotecas necessárias ---
import pandas as pd
import os
import io
import numpy as np
import requests
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
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
            print("⚠️ AVISO: Credenciais do SharePoint (Env) não encontradas. O upload para o SharePoint será pulado.")
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
        """
        Salva o DataFrame como Excel no SharePoint.
        """
        if not self.access_token: return

        print(f"--- Iniciando Upload SharePoint: {filename} ---")
        try:
            # 1. Converter DataFrame para buffer binário (Excel)
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Base')
            buffer.seek(0)
            file_content = buffer.getvalue()

            # 2. Construir URL de Upload
            folder_path = f"{self.config.TARGET_FOLDER}/{filename}"
            url_path = requests.utils.quote(folder_path)
            
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/root:/{url_path}:/content"

            # 3. Enviar
            headers = {
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            }
            self._api_request('put', url, data=file_content, headers=headers)
            print(f"✅ Arquivo '{filename}' salvo com sucesso na pasta '{self.config.TARGET_FOLDER}'.")

        except Exception as e:
            print(f"❌ Falha no upload para o SharePoint: {e}")

# ==============================================================================
# 2. FUNÇÕES ORIGINAIS DE ETL
# ==============================================================================

def autenticar_e_conectar():
    print("--- Autenticando com o Google Sheets ---")
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
        except NameError:
            script_dir = os.getcwd()
            
        creds_path = os.path.join(script_dir, 'credenciais.json')
        
        creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(creds)
        print("✅ Autenticação bem-sucedida.\n")
        return gc
    except FileNotFoundError:
        print(f"❌ ERRO: Arquivo de credenciais '{creds_path}' não encontrado.")
        return None
    except Exception as e:
        print(f"❌ ERRO durante a autenticação: {e}")
        return None

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
    print(f"✅ Arquivo de vendas (qlik_sense.xlsx) carregado com {dfs['vendas'].shape[0]} linhas e {dfs['vendas'].shape[1]} colunas.\n")
    
    print("--- 1.5. Removendo duplicatas das bases De/Para ---")
    dfs['produto_acabado']['chave_estoque'] = dfs['produto_acabado']['Produto'].astype(str).str.upper() + '&' + dfs['produto_acabado']['Base'].astype(str).str.upper()
    
    dfs['comprador'] = dfs['comprador'].drop_duplicates(subset=['CNPJ'], keep='first')
    dfs['deposito'] = dfs['deposito'].drop_duplicates(subset=['De'], keep='first')
    dfs['empresa'] = dfs['empresa'].drop_duplicates(subset=['De'], keep='first')
    dfs['segmentos'] = dfs['segmentos'].drop_duplicates(subset=['CNPJ'], keep='first')
    dfs['produto_acabado'] = dfs['produto_acabado'].drop_duplicates(subset=['chave_estoque'], keep='first')
    
    print("✅ Duplicatas das chaves 'De Para' removidas.\n")
    return dfs

def limpar_colunas_numericas_sense(df):
    print("--- 2. Limpando colunas numéricas ---")
    if 'Quantidade' in df.columns:
        df['Quantidade'] = df['Quantidade'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
        df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)
        print("✅ Coluna 'Quantidade' convertida para número.\n")
    return df

def mesclar_dados_sense(dfs):
    print("--- 3. Cruzando dados (PROCVs) ---")
    df_vendas = dfs['vendas'].copy()
    
    df_vendas = pd.merge(df_vendas, dfs['empresa'][['De', '2_EMPRESA']], left_on='Empresa', right_on='De', how='left').drop(columns=['De'])
    df_vendas = pd.merge(df_vendas, dfs['deposito'][['De', '2_EXPEDIDOR']], left_on='Cod Deposito', right_on='De', how='left').drop(columns=['De'])
    
    df_vendas['CNPJ Comprador'] = df_vendas['CNPJ Comprador'].astype(str)
    dfs['comprador']['CNPJ'] = dfs['comprador']['CNPJ'].astype(str)
    colunas_para_trazer = ['CNPJ', '2_DESTINATÁRIO', '2_DESTINATÁRIO TIPO']
    df_vendas = pd.merge(df_vendas, dfs['comprador'][colunas_para_trazer], left_on='CNPJ Comprador', right_on='CNPJ', how='left').drop(columns='CNPJ')
    
    df_vendas['2_DESTINATÁRIO TIPO'] = df_vendas['2_DESTINATÁRIO TIPO'].fillna('CLIENTES')
    df_vendas['2_DESTINATÁRIO'] = df_vendas['2_DESTINATÁRIO'].fillna('CLIENTES')
    
    print("✅ Todos os merges foram concluídos.\n")
    return df_vendas

def aplicar_regras_de_negocio_sense(df_vendas, dfs):
    print("--- 4. Aplicando regras de negócio ---")
    df_vendas = pd.merge(df_vendas, dfs['segmentos'][['CNPJ', 'Segmento']], left_on='CNPJ Comprador', right_on='CNPJ', how='left').drop(columns='CNPJ')
    
    cond_marangoni_comprador = df_vendas['RzSocial Comprador'].str.contains('Marangoni', case=False, na=False)
    df_vendas.loc[cond_marangoni_comprador, 'Segmento'] = 'GOOIL'
    cond_intercompany = (df_vendas['Segmento'].isna()) & (df_vendas['2_DESTINATÁRIO'] != 'CLIENTES')
    df_vendas.loc[cond_intercompany, 'Segmento'] = 'Intercompany'
    cond_posto = (df_vendas['Segmento'].isna()) & (df_vendas['RzSocial Comprador'].str.contains('Posto', case=False, na=False))
    df_vendas.loc[cond_posto, 'Segmento'] = 'Posto'
    cond_gooil_filial = (df_vendas['Segmento'].isna()) & (df_vendas['Empresa'] == 'Maragoni')
    df_vendas.loc[cond_gooil_filial, 'Segmento'] = 'GOOIL'
    cond_congenere = (df_vendas['Segmento'].isna()) & (df_vendas['Descricao Produto'].str.contains('Gasolina A|Diesel A', case=False, na=False))
    df_vendas.loc[cond_congenere, 'Segmento'] = 'congerenere'
    df_vendas['Segmento'] = df_vendas['Segmento'].fillna('B2B')
    print("✅ Coluna 'Segmento' tratada.")

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
    print("✅ Coluna 'Produto_1' criada.")

    df_vendas['chave_vendas'] = df_vendas['Produto_1'].astype(str).str.upper() + '&' + df_vendas['2_EXPEDIDOR'].astype(str).str.upper()
    dfs['produto_acabado']['chave_estoque'] = dfs['produto_acabado']['Produto'].astype(str).str.upper() + '&' + dfs['produto_acabado']['Base'].astype(str).str.upper()
    df_temp = pd.merge(df_vendas, dfs['produto_acabado'][['chave_estoque']], left_on='chave_vendas', right_on='chave_estoque', how='left')
    df_vendas['Armazena Acabado'] = np.where(df_temp['chave_estoque'].notna(), 'Sim', 'Não')
    
    lista_override = ['Hidratado', 'MGO', 'Gasolina A', 'Diesel A S10', 'Diesel A S500', 'Anidro', 'Biodiesel']
    condicao_override = df_vendas['Produto_1'].isin(lista_override)
    df_vendas.loc[condicao_override, 'Armazena Acabado'] = 'Sim'
    
    df_vendas = df_vendas.drop(columns=['chave_vendas'])
    print("✅ Coluna 'Armazena Acabado' criada e tratada.\n")
    
    # --- Aplicando regra de classificação de Tipo de Vendas ---
    print("✅ Aplicando regras de classificação para 'Tipo de vendas'.")
    cond_congenere_produto = df_vendas['Descricao Produto'].str.contains(
        'Gasolina A|Diesel A S10|Diesel A S500|Anidro|Biodiesel',
        case=False, na=False
    )
    df_vendas['Tipo de vendas'] = np.where(
        cond_congenere_produto, 'Venda Congênere', 'Produto Acabado'
    )
    cond_intercompany_override = df_vendas['2_DESTINATÁRIO TIPO'].str.upper() == 'INTERCOMPANY'
    df_vendas.loc[cond_intercompany_override, 'Tipo de vendas'] = 'INTERCOMPANY'
    print("✅ Coluna 'Tipo de vendas' criada e tratada.\n")
    
    return df_vendas

def criar_df_convertido_sense(df_tratado):
    print("--- 5. Criando a base de dados 'Convertido' ---")
    
    df_sim = df_tratado[df_tratado['Armazena Acabado'] == 'Sim'].copy()
    df_nao = df_tratado[df_tratado['Armazena Acabado'] != 'Sim'].copy()
    
    df_gc_a_converter = df_nao[df_nao['Produto_1'] == 'Gasolina C'].copy()
    df_db_a_converter = df_nao[df_nao['Produto_1'].str.contains('Diesel B', na=False)].copy()

    lista_cestas = []
    
    if not df_gc_a_converter.empty:
        derivado_gas_a = df_gc_a_converter.copy()
        derivado_gas_a['Produto_1'] = 'Gasolina A'
        derivado_gas_a['Quantidade'] *= 0.70
        lista_cestas.append(derivado_gas_a)

    if not df_db_a_converter.empty:
        derivado_diesel_a = df_db_a_converter.copy()
        derivado_diesel_a['Produto_1'] = derivado_diesel_a['Produto_1'].str.replace('B', 'A', regex=False)
        derivado_diesel_a['Quantidade'] *= 0.85
        lista_cestas.append(derivado_diesel_a)

    if not df_gc_a_converter.empty:
        bio_anidro = df_gc_a_converter.copy()
        bio_anidro['Produto_1'] = 'Anidro'
        bio_anidro['Quantidade'] *= 0.30
        lista_cestas.append(bio_anidro)

    if not df_db_a_converter.empty:
        bio_biodiesel = df_db_a_converter.copy()
        bio_biodiesel['Produto_1'] = 'Biodiesel'
        bio_biodiesel['Quantidade'] *= 0.15
        lista_cestas.append(bio_biodiesel)
    
    df_final_convertido = pd.concat([df_sim] + lista_cestas, ignore_index=True)
    
    print("✅ Base 'Convertido' finalizada.\n")
    return df_final_convertido

def salvar_no_sheets(client, df, url_planilha, nome_aba):
    try:
        print(f"--- Salvando dados na planilha: {url_planilha} ---")
        print(f"Aba de destino: '{nome_aba}'")
        
        spreadsheet = client.open_by_url(url_planilha)
        worksheet = spreadsheet.worksheet(nome_aba)
        worksheet.clear()
        
        for col in df.columns:
            if df[col].dtype == 'datetime64[ns]' or 'datetime' in str(df[col].dtype):
                df[col] = df[col].astype(str)

        set_with_dataframe(worksheet, df, include_index=False)
        print(f"✅ Dados salvos com sucesso na aba '{nome_aba}'.\n")
    except gspread.exceptions.WorksheetNotFound:
        print(f"❌ ERRO: A aba '{nome_aba}' não foi encontrada na planilha.")
    except Exception as e:
        print(f"❌ Ocorreu um erro ao salvar no Google Sheets: {e}")

# ==============================================================================
# 3. EXECUÇÃO PRINCIPAL
# ==============================================================================

def main():
    gc = autenticar_e_conectar()
    if not gc: return

    try:
        url_acabado = "https://docs.google.com/spreadsheets/d/1Teoy_KNhlqwQq1a2TNRCrN-Sjs5ofVrEvTM4b-7Ud4k/edit?usp=sharing"
        url_convertido = "https://docs.google.com/spreadsheets/d/1VT7ltCkEoZxRnsNF-dhgdNgqwkdw_u2E02PmduxzuE0/edit?usp=sharing"

        home_dir = os.path.expanduser('~')
        caminho_vendas_original = os.path.join(home_dir, 'Documentos', 'qlik_sense.xlsx')
        
        mod_time_timestamp = os.path.getmtime(caminho_vendas_original)
        data_hora_modificacao = datetime.fromtimestamp(mod_time_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        print(f"ℹ️ Data de modificação do arquivo de entrada será usada: {data_hora_modificacao}\n")
        
        dataframes = carregar_arquivos_sense(home_dir)
        dataframes['vendas'] = limpar_colunas_numericas_sense(dataframes['vendas'])
        df_tratado = mesclar_dados_sense(dataframes)
        df_tratado = aplicar_regras_de_negocio_sense(df_tratado, dataframes)
        
        df_tratado['Data_Hora_Atualizacao'] = data_hora_modificacao
        
        df_tratado_para_salvar = df_tratado.copy()
        if 'Quantidade' in df_tratado_para_salvar.columns:
            print("\nℹ️  Dividindo a coluna 'Quantidade' por 10 para a base 'acabado'.")
            df_tratado_para_salvar['Quantidade'] = pd.to_numeric(df_tratado_para_salvar['Quantidade'], errors='coerce').fillna(0)
            df_tratado_para_salvar['Quantidade'] = df_tratado_para_salvar['Quantidade']
        
        df_convertido = criar_df_convertido_sense(df_tratado)
        df_convertido['Data_Hora_Atualizacao'] = data_hora_modificacao
        
        if 'Quantidade' in df_convertido.columns:
            print("ℹ️  Dividindo a coluna 'Quantidade' por 10 para a base 'convertido'.\n")
            df_convertido['Quantidade'] = pd.to_numeric(df_convertido['Quantidade'], errors='coerce').fillna(0)
            df_convertido['Quantidade'] = df_convertido['Quantidade'] 
            
        colunas_para_salvar = [
            'Quantidade', 'Numero Pedido', 'Data Prevista Fat', 'RzSocial Comprador', 
            'CNPJ Comprador', '2_EMPRESA', '2_EXPEDIDOR', '2_DESTINATÁRIO', 
            '2_DESTINATÁRIO TIPO', 'Segmento', 'Produto_1', 'Armazena Acabado', 'Tipo de vendas',
            'Data_Hora_Atualizacao'
        ]

        # --- Google Sheets: Base Acabado ---
        colunas_existentes_tratado = [col for col in colunas_para_salvar if col in df_tratado_para_salvar.columns]
        df_acabado_final = df_tratado_para_salvar[colunas_existentes_tratado].copy()
        salvar_no_sheets(gc, df_acabado_final, url_acabado, 'Base')

        # --- Google Sheets: Base Convertido ---
        colunas_existentes_convertido = [col for col in colunas_para_salvar if col in df_convertido.columns]
        df_convertido_final = df_convertido[colunas_existentes_convertido].copy()
        salvar_no_sheets(gc, df_convertido_final, url_convertido, 'Base')

        # ======================================================================
        # 4. SALVANDO NO SHAREPOINT (NOVO BLOCO - Sense)
        # ======================================================================
        # Verifica se credenciais do SharePoint existem antes de tentar
        if Config.validate():
            sp_uploader = SharePointUploader(Config)
            
            # --- DEFINIÇÃO DE NOMES DISTINTOS ---
            # Salva com '_Sense' para não sobrescrever os do Qlik View
            nome_arquivo_tratado_sp = "Base_Acabado_Sense.xlsx"
            nome_arquivo_convertido_sp = "Base_Convertido_Sense.xlsx"
            
            # Upload do DF Tratado (Acabado)
            sp_uploader.upload_dataframe(df_acabado_final, nome_arquivo_tratado_sp)
            
            # Upload do DF Convertido
            sp_uploader.upload_dataframe(df_convertido_final, nome_arquivo_convertido_sp)

    except FileNotFoundError as e:
        print(f"❌ ERRO: Arquivo não encontrado! Verifique o caminho: {e.filename}")
    except KeyError as e:
        print(f"❌ ERRO de Chave: A coluna {e} não foi encontrada em uma das tabelas.")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()