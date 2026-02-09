import os
import re
import logging
import pandas as pd
import requests
import time
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from typing import List, Dict, Any

# ==============================================================================
# CONFIGURAÇÃO E LOGGING
# ==============================================================================
def setup_logging():
    """
    Configura o sistema de logging com:
    - Console: mostra todos os logs (INFO, WARNING, ERROR)
    - Arquivo: salva apenas WARNING e ERROR na pasta logs/
    """
    # Criar pasta de logs se não existir
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    # Nome do arquivo de log baseado no nome do script
    script_name = Path(__file__).stem
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = logs_dir / f"{script_name}_erros_{timestamp}.log"
    
    # Configurar formato dos logs
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'
    
    # Handler para console (todos os níveis)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # Handler para arquivo (apenas WARNING e ERROR)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.WARNING)  # Apenas WARNING e ERROR
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    
    # Configurar o logger raiz
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()  # Limpar handlers padrão
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    
    logging.info(f"📝 Sistema de logs configurado. Logs de erro serão salvos em: {log_file}")
    return log_file

# Configurar logging
log_file_path = setup_logging()
load_dotenv()

class Config:
    TENANT_ID = os.getenv("TENANT_ID")
    CLIENT_ID = os.getenv("CLIENT_ID")
    CLIENT_SECRET = os.getenv("CLIENT_SECRET")
    HOSTNAME = os.getenv("HOSTNAME")
    
    SITE_PATH = "sites/Transportes"
    TARGET_SHEET_NAME = "Base"
    
    TRAFEGUS_FILENAME = "Relatório de NF Trafegus.xlsx"
    TRAFEGUS_SHEET_NAME = "Sheet1"
    
    # Nomes exatos das colunas no Relatório Trafegus
    COL_TRAFEGUS_PLACA = "Placa" # Ajuste se o nome exato for diferente (ex: "PLACA")
    COL_TRAFEGUS_POSICAO = "Posição" # Ajuste se o nome exato for diferente
    COL_TRAFEGUS_DATA_FIXA = "Data Última Posição" # Fixado conforme solicitado
    
    ARQUIVOS_PERMITIDOS = [
        "FORM-PPL-000 - Fitplan Hidratado - RJ.xlsx",
        "FORM-PPL-000 - Fitplan Hidratado - SP.xlsx",
        "FORM-PPL-000 - Fitplan Anidro - SP.xlsx",
        "FORM-PPL-000 - Fitplan Anidro - RJ.xlsx",
        'FORM-PPL-000 - Fitplan Biodiesel.xlsx',
        "FORM-PPL-000 - Gasolina.xlsx",
        "FORM-PPL-000 - Diesel e Insumos.xlsx"
    ]

    COLUNAS_TRANSPORTE = [
        "sm", "data_prev_carregamento", "expedidor", "cidade_origem", "ufo",
        "destinatario_venda", "destinatario", "recebedor", "cidade_destino", "ufd",
        "produto", "motorista", "cavalo", "carreta1", "carreta2", "transportadora",
        "nfe", "volume_l", "data_de_carregamento", 
        "horario_de_carregamento", 
        "data_chegada", "data_descarga", "status"
    ]

    @staticmethod
    def get_col_letter(col_name: str) -> str:
        try:
            idx = Config.COLUNAS_TRANSPORTE.index(col_name)
            return chr(65 + idx)
        except: return None

# ==============================================================================
# CLIENTE SHAREPOINT
# ==============================================================================
class SharePointClient:
    def __init__(self, config: Config):
        self.config = config
        self.access_token = self._get_token()
        self.api_site = f"{self.config.HOSTNAME}:/{self.config.SITE_PATH}"
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
        try:
            r = requests.post(url, data=data)
            r.raise_for_status()
            return r.json()["access_token"]
        except requests.exceptions.RequestException as e:
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            response_text = getattr(e.response, 'text', None) if hasattr(e, 'response') and hasattr(e.response, 'text') else None
            logging.error(
                f"❌ ERRO ao obter token de autenticação\n"
                f"   🔗 URL: {url}\n"
                f"   🆔 Tenant ID: {self.config.TENANT_ID}\n"
                f"   🆔 Client ID: {self.config.CLIENT_ID}\n"
                f"   📊 Status Code: {status_code or 'N/A'}\n"
                f"   📝 Response: {response_text[:500] if response_text else 'N/A'}\n"
                f"   ⚠️ Erro: {type(e).__name__}: {str(e)}"
            )
            raise

    def _api_get(self, url: str) -> Any:
        headers = {"Authorization": f"Bearer {self.access_token}"}
        try:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            response_text = getattr(e.response, 'text', None) if hasattr(e, 'response') and hasattr(e.response, 'text') else None
            logging.error(
                f"❌ ERRO na requisição GET\n"
                f"   🔗 URL: {url}\n"
                f"   📊 Status Code: {status_code or 'N/A'}\n"
                f"   📝 Response: {response_text[:500] if response_text else 'N/A'}\n"
                f"   ⚠️ Erro: {type(e).__name__}: {str(e)}"
            )
            raise

    def _api_patch(self, url: str, json_data: Dict) -> Any:
        headers = {"Authorization": f"Bearer {self.access_token}", "Content-Type": "application/json"}
        try:
            r = requests.patch(url, headers=headers, json=json_data)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
            response_text = getattr(e.response, 'text', None) if hasattr(e, 'response') and hasattr(e.response, 'text') else None
            logging.error(
                f"❌ ERRO na requisição PATCH\n"
                f"   🔗 URL: {url}\n"
                f"   📦 Payload: {json_data}\n"
                f"   📊 Status Code: {status_code or 'N/A'}\n"
                f"   📝 Response: {response_text[:500] if response_text else 'N/A'}\n"
                f"   ⚠️ Erro: {type(e).__name__}: {str(e)}"
            )
            raise

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
            actual_sheet = next((s['name'] for s in sheets if s['name'].lower() == sheet_name.lower()), sheets[0]['name'])
            url_range = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{actual_sheet}/usedRange"
            data_json = self._api_get(url_range)
            values = data_json.get('values', [])
            if not values or len(values) < 2: 
                logging.warning(
                    f"⚠️ Excel vazio ou sem dados suficientes\n"
                    f"   🆔 Item ID: {item_id}\n"
                    f"   📄 Sheet esperado: '{sheet_name}' | Sheet usado: '{actual_sheet}'\n"
                    f"   📊 Linhas encontradas: {len(values) if values else 0}\n"
                    f"   📋 Colunas esperadas: {colunas_esperadas}"
                )
                return None
            df = pd.DataFrame(values[1:], columns=values[0])
            if colunas_esperadas:
                df = df.iloc[:, :len(colunas_esperadas)]
                df.columns = colunas_esperadas
            df['__ms_file_id'] = item_id
            df['__ms_sheet_name'] = actual_sheet
            df['__excel_row_num'] = range(2, len(df) + 2)
            return df
        except Exception as e:
            logging.error(
                f"❌ ERRO ao ler Excel\n"
                f"   🆔 Item ID: {item_id}\n"
                f"   📄 Sheet esperado: '{sheet_name}'\n"
                f"   📋 Colunas esperadas: {colunas_esperadas}\n"
                f"   ⚠️ Erro: {type(e).__name__}: {str(e)}"
            )
            return None

    def update_excel_row(self, item_id: str, sheet: str, row_num: int, updates: Dict[str, Any]):
        for col_name, value in updates.items():
            col_letter = Config.get_col_letter(col_name)
            if not col_letter: 
                logging.warning(f"⚠️ Coluna '{col_name}' não encontrada no mapeamento. Colunas disponíveis: {Config.COLUNAS_TRANSPORTE}")
                continue
            address = f"{col_letter}{row_num}"
            url = f"https://graph.microsoft.com/v1.0/drives/{self.drive_id}/items/{item_id}/workbook/worksheets/{sheet}/range(address='{address}')"
            payload = { "values": [[value]] }
            try:
                self._api_patch(url, payload)
                time.sleep(0.1) 
            except Exception as e:
                logging.error(
                    f"❌ ERRO ao atualizar célula no Excel\n"
                    f"   📍 Localização: Sheet='{sheet}' | Célula='{address}' | Linha={row_num}\n"
                    f"   📝 Coluna: '{col_name}' (letra: {col_letter})\n"
                    f"   💾 Valor tentado: {repr(value)}\n"
                    f"   🔗 URL: {url}\n"
                    f"   📦 Payload: {payload}\n"
                    f"   🆔 Item ID: {item_id}\n"
                    f"   ⚠️ Erro: {type(e).__name__}: {str(e)}"
                )

# ==============================================================================
# PROCESSADOR DE DADOS
# ==============================================================================
class DataProcessor:
    @staticmethod
    def normalizar(series: pd.Series) -> pd.Series:
        return series.astype(str).str.upper().str.strip()

    @staticmethod
    def limpar_placa(series: pd.Series) -> pd.Series:
        return series.astype(str).str.upper().str.replace(r'[^A-Z0-9]', '', regex=True)

    @staticmethod
    def limpar_data_com_extras(data_str: str) -> str:
        """
        Extrai apenas a parte da data (DD/MM/YYYY) de strings que contêm data + hora + dia da semana.
        
        Exemplos:
        - '09/02/2026 14:34:27 Seg' -> '09/02/2026'
        - '09/02/2026 14:34:27' -> '09/02/2026'
        - '09/02/2026 Seg' -> '09/02/2026'
        - '09/02/2026' -> '09/02/2026' (sem alteração)
        
        Args:
            data_str: String que pode conter data com ou sem hora e dia da semana
            
        Returns:
            String com apenas a data no formato DD/MM/YYYY, ou string original se não encontrar padrão
        """
        if not data_str or pd.isna(data_str):
            return ''
        
        data_str = str(data_str).strip()
        
        if not data_str or data_str.lower() == 'nan':
            return ''
        
        # Padrão regex para DD/MM/YYYY (com validação básica)
        # Aceita: DD/MM/YYYY, D/MM/YYYY, DD/M/YYYY, D/M/YYYY
        pattern = r'^(\d{1,2}/\d{1,2}/\d{4})'
        match = re.match(pattern, data_str)
        
        if match:
            # Extrai apenas a parte da data
            data_limpa = match.group(1)
            return data_limpa
        else:
            # Se não encontrar padrão, retorna string original
            return data_str

    @staticmethod
    def _tratar_data_excel(series: pd.Series, contexto: str = "") -> pd.Series:
        """
        Trata datas vindas do Excel que podem estar em diferentes formatos:
        - Números seriais do Excel (ex: 45322.0)
        - Strings em formato brasileiro (DD/MM/YYYY)
        - Strings em formato americano (MM/DD/YYYY)
        Retorna uma Series de datetime.
        
        Args:
            series: Series do pandas com valores de data
            contexto: Contexto adicional para os logs (ex: "Trafegus", "Transporte")
        """
        if series is None or series.empty:
            logging.debug(f"📅 [{contexto}] Series vazia ou None - retornando Series vazia")
            return pd.Series(dtype='datetime64[ns]')
        
        # Log estatísticas do formato original
        total_valores = len(series)
        valores_nao_nulos = series.notna().sum()
        valores_nulos = total_valores - valores_nao_nulos
        
        # Analisar tipos dos valores não nulos
        tipos_encontrados = {}
        formatos_encontrados = {}
        numericos_count = 0
        texto_count = 0
        
        for idx, val in series.items():
            if pd.notna(val):
                tipo = type(val).__name__
                tipos_encontrados[tipo] = tipos_encontrados.get(tipo, 0) + 1
                
                val_str = str(val).strip()
                formato = "desconhecido"
                
                # Verificar se é numérico (serial do Excel)
                try:
                    num_val = float(val_str.replace(',', '.'))
                    if num_val > 0:
                        numericos_count += 1
                        formato = f"número serial Excel ({num_val:.2f})"
                    else:
                        texto_count += 1
                        formato = "texto (número <= 0)"
                except (ValueError, TypeError):
                    texto_count += 1
                    # Tentar identificar formato de texto
                    if '/' in val_str:
                        partes = val_str.split('/')
                        if len(partes) == 3:
                            primeiro = partes[0].strip()
                            segundo = partes[1].strip()
                            terceiro = partes[2].strip()
                            try:
                                p1 = int(primeiro)
                                p2 = int(segundo)
                                p3 = int(terceiro)
                                # Verificar se tem dados extras (hora, etc)
                                tem_extras = ' ' in val_str or len(terceiro) > 4
                                extras_info = " (com dados extras)" if tem_extras else ""
                                
                                # Lógica de detecção: se primeiro <= 12 e segundo > 12, provavelmente MM/DD/YYYY
                                if p1 <= 12 and p2 > 12:
                                    formato = f"texto (MM/DD/YYYY?{extras_info})"
                                elif p1 > 12:
                                    formato = f"texto (DD/MM/YYYY?{extras_info})"
                                else:
                                    # Ambíguo (ex: 05/01/2024)
                                    formato = f"texto (ambíguo DD/MM ou MM/DD?{extras_info})"
                            except (ValueError, TypeError):
                                formato = "texto (formato com / mas não numérico)"
                        else:
                            formato = "texto (formato com / mas não 3 partes)"
                    elif '-' in val_str:
                        formato = "texto (formato com -)"
                    else:
                        formato = "texto (sem separador de data)"
                
                formatos_encontrados[formato] = formatos_encontrados.get(formato, 0) + 1
        
        logging.info(f"📊 [{contexto}] ANÁLISE DE DATAS - Total: {total_valores} | Não nulos: {valores_nao_nulos} | Nulos: {valores_nulos} | Numéricos: {numericos_count} | Texto: {texto_count}")
        if tipos_encontrados:
            logging.info(f"📊 [{contexto}] TIPOS ENCONTRADOS: {tipos_encontrados}")
        if formatos_encontrados:
            logging.info(f"📊 [{contexto}] FORMATOS DETECTADOS: {formatos_encontrados}")
        
        # ETAPA 0: Limpeza prévia - Remove hora e dia da semana das datas
        series_limpa = series.copy()
        datas_limpas_count = 0
        exemplos_limpeza = []
        
        for idx, val in series.items():
            if pd.notna(val):
                val_str = str(val).strip()
                val_limpo = DataProcessor.limpar_data_com_extras(val_str)
                if val_limpo != val_str:
                    series_limpa.iloc[idx] = val_limpo
                    datas_limpas_count += 1
                    if len(exemplos_limpeza) < 5:  # Guarda primeiros 5 exemplos
                        exemplos_limpeza.append((val_str, val_limpo))
        
        if datas_limpas_count > 0:
            logging.info(f"🧹 [{contexto}] {datas_limpas_count} datas foram limpas (remoção de hora/dia da semana)")
            if exemplos_limpeza:
                logging.info(f"🧹 [{contexto}] Exemplos de limpeza (primeiros {len(exemplos_limpeza)}):")
                for antes, depois in exemplos_limpeza:
                    logging.info(f"   '{antes}' -> '{depois}'")
        
        # 1. Tenta converter valores numéricos do Excel (ex: 45322.0)
        # O Excel usa 1899-12-30 como origem para números seriais de data
        datas_numericas = pd.to_numeric(series_limpa.astype(str).str.replace(',', '.'), errors='coerce')
        datas_convertidas = pd.to_datetime(datas_numericas, unit='D', origin='1899-12-30', errors='coerce')
        numericos_convertidos = datas_convertidas.notna().sum()
        
        if numericos_convertidos > 0:
            logging.info(f"✅ [{contexto}] Convertidos {numericos_convertidos} valores numéricos (serial Excel)")
        
        # 2. Tenta converter texto com formato fixo brasileiro (DD/MM/YYYY)
        datas_texto = pd.to_datetime(series_limpa, format='%d/%m/%Y', errors='coerce')
        texto_convertido_fixo = datas_texto.notna().sum() - numericos_convertidos
        
        if texto_convertido_fixo > 0:
            logging.info(f"✅ [{contexto}] Convertidos {texto_convertido_fixo} valores texto (formato DD/MM/YYYY fixo)")
        
        # 3. Se ainda houver NaT (falha no formato fixo), tenta o modo flexível com dayfirst=True
        # dayfirst=True força a interpretação brasileira (DD/MM/YYYY)
        mask_faltante = datas_texto.isna() & series_limpa.notna()
        if mask_faltante.any():
            valores_faltantes = mask_faltante.sum()
            logging.info(f"🔄 [{contexto}] Tentando conversão flexível para {valores_faltantes} valores restantes...")
            datas_flexiveis = pd.to_datetime(series_limpa[mask_faltante], dayfirst=True, errors='coerce')
            flexiveis_convertidos = datas_flexiveis.notna().sum()
            if flexiveis_convertidos > 0:
                logging.info(f"✅ [{contexto}] Convertidos {flexiveis_convertidos} valores com conversão flexível (dayfirst=True)")
            datas_texto = datas_texto.fillna(datas_flexiveis)
        
        # Verificar quantos valores não foram convertidos
        resultado_final = datas_convertidas.fillna(datas_texto)
        nao_convertidos = resultado_final.isna().sum()
        if nao_convertidos > 0:
            # Log alguns exemplos dos valores que não foram convertidos
            exemplos_nao_convertidos = series_limpa[resultado_final.isna()].head(10).tolist()
            indices_nao_convertidos = series_limpa[resultado_final.isna()].head(10).index.tolist()
            
            logging.warning(
                f"⚠️ [{contexto}] VALORES NÃO CONVERTIDOS PARA DATETIME\n"
                f"   📊 Total não convertidos: {nao_convertidos} de {total_valores}\n"
                f"   📝 Exemplos (primeiros 10):\n"
                + "\n".join([f"      [{idx}] {repr(val)}" for idx, val in zip(indices_nao_convertidos, exemplos_nao_convertidos)])
            )
        
        # Combina os resultados: prioriza datas numéricas, depois texto
        return resultado_final

    @staticmethod
    def formatar_data_brasileira(data_value, contexto: str = "") -> str:
        """
        Converte um valor de data para string no formato brasileiro (DD/MM/YYYY).
        Aceita: datetime, string, número serial do Excel, ou None.
        
        Args:
            data_value: Valor da data a ser formatado
            contexto: Contexto adicional para os logs (ex: "Trafegus", "Transporte")
        """
        # Identificar tipo e formato original
        tipo_original = type(data_value).__name__
        valor_original_str = str(data_value).strip()
        
        if pd.isna(data_value) or data_value is None or valor_original_str == '' or valor_original_str.lower() == 'nan':
            logging.debug(f"📅 [{contexto}] Data vazia ou nula - retornando vazio")
            return ''
        
        try:
            # Se já for datetime, formata diretamente
            if isinstance(data_value, (pd.Timestamp, datetime)):
                formato_final = data_value.strftime('%d/%m/%Y')
                logging.info(f"📅 [{contexto}] FORMATO LOCALIZADO: datetime | VALOR: {data_value} | FORMATO REPASSADO: {formato_final} | TIPO: datetime")
                return formato_final
            
            # Se for string, limpa primeiro (remove hora e dia da semana)
            data_str_original = valor_original_str
            data_str = DataProcessor.limpar_data_com_extras(valor_original_str)
            
            # Verificar se houve limpeza (dados extras removidos)
            tem_dados_extras = False
            dados_extras_info = ""
            if data_str != data_str_original:
                dados_extras_info = f" | DADOS EXTRAS REMOVIDOS: '{data_str_original[len(data_str):].strip()}'"
                tem_dados_extras = True
            
            # Verificar se é número (serial do Excel)
            is_numero = False
            try:
                num_val = float(data_str.replace(',', '.'))
                if num_val > 0:
                    is_numero = True
                    dt = pd.to_datetime(num_val, unit='D', origin='1899-12-30')
                    formato_final = dt.strftime('%d/%m/%Y')
                    logging.info(f"📅 [{contexto}] FORMATO LOCALIZADO: número serial Excel ({num_val}) | VALOR ORIGINAL: {valor_original_str}{dados_extras_info} | FORMATO REPASSADO: {formato_final} | TIPO: número")
                    return formato_final
            except (ValueError, TypeError):
                pass
            
            # Tenta converter string de data - Primeiro formato brasileiro fixo (DD/MM/YYYY)
            try:
                dt = pd.to_datetime(data_str, format='%d/%m/%Y')
                formato_final = dt.strftime('%d/%m/%Y')
                tipo_detectado = "texto (DD/MM/YYYY)"
                logging.info(f"📅 [{contexto}] FORMATO LOCALIZADO: {tipo_detectado} | VALOR ORIGINAL: {valor_original_str}{dados_extras_info} | FORMATO REPASSADO: {formato_final} | TIPO: texto")
                return formato_final
            except (ValueError, TypeError):
                pass
            
            # Tenta formato americano ou ambíguo com dayfirst=True (força interpretação brasileira)
            try:
                dt = pd.to_datetime(data_str, dayfirst=True, errors='coerce')
                if pd.notna(dt):
                    formato_final = dt.strftime('%d/%m/%Y')
                    # Tentar detectar se era formato americano
                    if '/' in data_str:
                        partes = data_str.split('/')
                        if len(partes) == 3:
                            primeiro = partes[0]
                            segundo = partes[1]
                            # Se primeiro > 12, provavelmente era DD/MM/YYYY
                            # Se primeiro <= 12 e segundo > 12, provavelmente era MM/DD/YYYY
                            if int(primeiro) <= 12 and int(segundo) > 12:
                                tipo_detectado = "texto (MM/DD/YYYY - CORRIGIDO)"
                            else:
                                tipo_detectado = "texto (DD/MM/YYYY - confirmado)"
                        else:
                            tipo_detectado = "texto (formato flexível)"
                    else:
                        tipo_detectado = "texto (formato flexível)"
                    
                    logging.info(f"📅 [{contexto}] FORMATO LOCALIZADO: {tipo_detectado} | VALOR ORIGINAL: {valor_original_str}{dados_extras_info} | FORMATO REPASSADO: {formato_final} | TIPO: texto")
                    return formato_final
            except (ValueError, TypeError) as e:
                pass
            
            # Se tudo falhar, retorna a string original (pode ser um formato não reconhecido)
            logging.warning(
                f"⚠️ [{contexto}] FALHA NA CONVERSÃO DE DATA\n"
                f"   📅 FORMATO LOCALIZADO: DESCONHECIDO\n"
                f"   📝 VALOR ORIGINAL: {repr(valor_original_str)}{dados_extras_info}\n"
                f"   🔢 TIPO ORIGINAL: {tipo_original}\n"
                f"   📤 FORMATO REPASSADO: {valor_original_str} (sem conversão)\n"
                f"   ⚠️ Motivo: Nenhum método de conversão funcionou"
            )
            return valor_original_str
            
        except Exception as e:
            logging.error(
                f"❌ [{contexto}] ERRO CRÍTICO ao formatar data\n"
                f"   📝 VALOR ORIGINAL: {repr(valor_original_str)}\n"
                f"   🔢 TIPO ORIGINAL: {tipo_original}\n"
                f"   ⚠️ Erro: {type(e).__name__}: {str(e)}\n"
                f"   📤 Retornando valor como string: {str(data_value)}"
            )
            return str(data_value)

    @staticmethod
    def formatar_string_final(row):
        # Trata a data corretamente antes de formatar
        data_origem_raw = row[Config.COL_TRAFEGUS_DATA_FIXA]
        arquivo_nome = row.get('__arquivo_nome', 'Desconhecido')
        cavalo = row.get('cavalo', 'N/A')
        linha_excel = row.get('__excel_row_num', 'N/A')
        contexto = f"Arquivo: {arquivo_nome} | Placa: {cavalo} | Linha: {linha_excel}"
        data_origem = DataProcessor.formatar_data_brasileira(data_origem_raw, contexto=contexto)
        
        posicao_original = str(row['ultima_posicao_original']).strip()
        posicao_norm = str(row['ultima_posicao_norm'])
        status_atual = str(row['status_norm'])

        # Lógica Condicional de Verificação
        no_local = False
        
        if status_atual == 'PROGRAMADO':
            # Para Programados, olha a ORIGEM
            expedidor = str(row['expedidor_norm'])
            cidade_origem = str(row['cidade_origem_norm'])
            if (expedidor != "" and expedidor in posicao_norm) or \
               (cidade_origem != "" and cidade_origem in posicao_norm):
                no_local = True
        
        elif 'TRANSITO' in status_atual:
            # Para Em Trânsito, olha o DESTINO
            cidade_destino = str(row['cidade_destino_norm'])
            if cidade_destino != "" and cidade_destino in posicao_norm:
                no_local = True

        # Se estiver "NO LOCAL", não precisa de data - é um resultado válido
        if no_local:
            # Se a data estiver vazia mas está "NO LOCAL", não é erro - apenas log informativo
            if not data_origem or data_origem.strip() == '':
                logging.info(
                    f"ℹ️ [{contexto}] Veículo NO LOCAL (sem data do Trafegus)\n"
                    f"   📄 Arquivo: {arquivo_nome}\n"
                    f"   🚛 Placa: {cavalo}\n"
                    f"   📍 Linha Excel: {linha_excel}\n"
                    f"   📝 Valor original Trafegus: {repr(data_origem_raw)}\n"
                    f"   ✅ Resultado: ' | NO LOCAL' (válido - veículo já no local)"
                )
                return " | NO LOCAL"
            else:
                return f"{data_origem} | NO LOCAL"
        else:
            # Se NÃO estiver "NO LOCAL" e a data estiver vazia, aí sim é um erro
            if not data_origem or data_origem.strip() == '':
                logging.error(
                    f"❌ [{contexto}] DATA VAZIA APÓS FORMATAÇÃO (veículo não está no local)\n"
                    f"   📝 Valor original: {repr(data_origem_raw)}\n"
                    f"   📄 Arquivo: {arquivo_nome}\n"
                    f"   🚛 Placa: {cavalo}\n"
                    f"   📍 Linha Excel: {linha_excel}\n"
                    f"   📍 Posição: {posicao_original}\n"
                    f"   🔄 Usando fallback: data atual"
                )
                # Usar data atual como fallback apenas quando não está "NO LOCAL"
                data_origem = datetime.now().strftime('%d/%m/%Y')
            
            return f"{data_origem} | {posicao_original}"

# ==============================================================================
# EXECUÇÃO PRINCIPAL
# ==============================================================================
def main():
    try:
        sp = SharePointClient(Config)

        logging.info("📂 Lendo arquivos de transporte...")
        arquivos = sp.get_root_items()
        lista_dfs = []

        for arq in arquivos:
            if arq['name'] in Config.ARQUIVOS_PERMITIDOS:
                # LOG DE ARQUIVO LIDO
                logging.info(f"   [CHECK] Processando arquivo: {arq['name']}")
                
                df = sp.read_excel(arq['id'], Config.TARGET_SHEET_NAME, Config.COLUNAS_TRANSPORTE)
                if df is not None:
                    df['__arquivo_nome'] = arq['name']
                    lista_dfs.append(df)

        if not lista_dfs:
            arquivos_encontrados = [arq['name'] for arq in arquivos]
            logging.warning(
                f"⚠️ NENHUM ARQUIVO DE TRANSPORTE PERMITIDO ENCONTRADO\n"
                f"   📋 Arquivos permitidos: {Config.ARQUIVOS_PERMITIDOS}\n"
                f"   📂 Arquivos encontrados no root ({len(arquivos_encontrados)}):\n"
                + "\n".join([f"      - {nome}" for nome in arquivos_encontrados[:20]])  # Limita a 20 para não poluir
            )
            return

        df_transp = pd.concat(lista_dfs, ignore_index=True)
        df_transp['status_norm'] = DataProcessor.normalizar(df_transp['status'])
        
        # Filtra Programados e Em Trânsito
        status_permitidos = ['PROGRAMADO', 'EM TRÂNSITO', 'EM TRÂNSITO BY PASS']
        df_transp = df_transp[df_transp['status_norm'].isin(status_permitidos)].copy()

        if df_transp.empty:
            logging.info("💤 Nenhuma viagem nos status permitidos para processar.")
            return

        # Normalizações para o "Match" de localização
        df_transp['cavalo_match'] = DataProcessor.limpar_placa(df_transp['cavalo'])
        df_transp['expedidor_norm'] = DataProcessor.normalizar(df_transp['expedidor'])
        df_transp['cidade_origem_norm'] = DataProcessor.normalizar(df_transp['cidade_origem'])
        df_transp['cidade_destino_norm'] = DataProcessor.normalizar(df_transp['cidade_destino'])

        # LER TRAFEGUS
        logging.info(f"📄 Lendo fonte de dados: {Config.TRAFEGUS_FILENAME}")
        trafegus_id = sp.get_item_id_by_path(Config.TRAFEGUS_FILENAME)
        df_trafegus = sp.read_excel(trafegus_id, Config.TRAFEGUS_SHEET_NAME)
        
        # Validação das colunas fixas
        if Config.COL_TRAFEGUS_DATA_FIXA not in df_trafegus.columns:
            colunas_encontradas = list(df_trafegus.columns)
            logging.error(
                f"❌ COLUNA OBRIGATÓRIA NÃO ENCONTRADA NO TRAFEGUS\n"
                f"   🔍 Coluna esperada: '{Config.COL_TRAFEGUS_DATA_FIXA}'\n"
                f"   📋 Colunas encontradas ({len(colunas_encontradas)}):\n"
                + "\n".join([f"      - {col}" for col in colunas_encontradas[:30]])  # Limita a 30
            )
            return

        # Identificar colunas de Placa e Posição (caso variem, mas mantendo a lógica)
        col_placa = next((c for c in df_trafegus.columns if 'PLACA' in c.upper()), None)
        col_posicao = next((c for c in df_trafegus.columns if 'POSI' in c.upper() or 'LOCALIZA' in c.upper()), None)
        
        if not col_placa:
            colunas_encontradas = list(df_trafegus.columns)
            logging.error(
                f"❌ COLUNA DE PLACA NÃO ENCONTRADA NO TRAFEGUS\n"
                f"   🔍 Buscando coluna contendo 'PLACA'\n"
                f"   📋 Colunas disponíveis ({len(colunas_encontradas)}):\n"
                + "\n".join([f"      - {col}" for col in colunas_encontradas[:30]])
            )
            return
        
        if not col_posicao:
            colunas_encontradas = list(df_trafegus.columns)
            logging.error(
                f"❌ COLUNA DE POSIÇÃO NÃO ENCONTRADA NO TRAFEGUS\n"
                f"   🔍 Buscando coluna contendo 'POSI' ou 'LOCALIZA'\n"
                f"   📋 Colunas disponíveis ({len(colunas_encontradas)}):\n"
                + "\n".join([f"      - {col}" for col in colunas_encontradas[:30]])
            )
            return

        # Tratamento correto da coluna de data do Trafegus
        logging.info("🔧 Tratando coluna de data do Trafegus...")
        df_trafegus[Config.COL_TRAFEGUS_DATA_FIXA] = DataProcessor._tratar_data_excel(
            df_trafegus[Config.COL_TRAFEGUS_DATA_FIXA],
            contexto="Trafegus"
        )

        df_trafegus['placa_match'] = DataProcessor.limpar_placa(df_trafegus[col_placa])
        df_trafegus['ultima_posicao_original'] = df_trafegus[col_posicao].astype(str)
        df_trafegus['ultima_posicao_norm'] = DataProcessor.normalizar(df_trafegus[col_posicao])

        # Merge
        df_match = pd.merge(
            df_transp, 
            df_trafegus[['placa_match', Config.COL_TRAFEGUS_DATA_FIXA, 'ultima_posicao_norm', 'ultima_posicao_original']], 
            left_on='cavalo_match', 
            right_on='placa_match', 
            how='inner'
        )

        for _, row in df_match.iterrows():
            nova_info = DataProcessor.formatar_string_final(row)
            updates = { "data_chegada": nova_info }
            
            logging.info(f"💾 Atualizando {row['__arquivo_nome']} | Linha {row['__excel_row_num']} | {row['cavalo']} -> {nova_info}")
            
            sp.update_excel_row(
                row['__ms_file_id'], 
                row['__ms_sheet_name'], 
                row['__excel_row_num'], 
                updates
            )

        logging.info("✅ Sincronização Trafegus finalizada.")

    except Exception as e:
        import traceback
        logging.critical(
            f"🔥 ERRO FATAL NA EXECUÇÃO\n"
            f"   ⚠️ Tipo do erro: {type(e).__name__}\n"
            f"   📝 Mensagem: {str(e)}\n"
            f"   📍 Traceback completo:\n"
            + "\n".join([f"      {linha}" for linha in traceback.format_exc().split('\n') if linha.strip()])
        )

if __name__ == "__main__":
    main()