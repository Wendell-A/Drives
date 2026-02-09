# Manual de Correções e Melhorias - Scripts Python

## Objetivo

Este manual documenta todas as correções e melhorias implementadas para facilitar a replicação em outros scripts Python do projeto, especialmente aqueles que trabalham com:

- Processamento de datas do Excel/SharePoint
- Logging e rastreamento de erros
- Validações e tratamento de dados

---

## 1. SISTEMA DE LOGGING MELHORADO

### 1.1 Problema Identificado

- Logs misturados (INFO, WARNING, ERROR) dificultavam identificação de problemas
- Sem rastreamento de erros em arquivos
- Falta de contexto nos logs de erro

### 1.2 Solução Implementada

#### Passo 1: Adicionar Imports Necessários

```python
import logging
from pathlib import Path
from datetime import datetime
```

#### Passo 2: Criar Função setup_logging()

Substituir `logging.basicConfig()` por função customizada:

```python
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

# Configurar logging (substituir logging.basicConfig())
log_file_path = setup_logging()
```

### 1.3 Benefícios

- Arquivos de log organizados por script e timestamp
- Apenas erros e warnings salvos (facilita análise)
- Console mostra todos os logs para acompanhamento em tempo real

---

## 2. TRATAMENTO DE DATAS COM HORA E DIA DA SEMANA

### 2.1 Problema Identificado

- Datas vindas do Excel/SharePoint no formato: `'09/02/2026 14:34:27 Seg'`
- Conversão falhava porque formato incluía hora e dia da semana
- 100% de falha na conversão de datas com extras

### 2.2 Solução Implementada

#### Passo 1: Criar Função de Limpeza

```python
import re

@staticmethod
def limpar_data_com_extras(data_str: str) -> str:
    """
    Extrai apenas a parte da data (DD/MM/YYYY) de strings que contêm data + hora + dia da semana.
    
    Exemplos:
    - '09/02/2026 14:34:27 Seg' -> '09/02/2026'
    - '09/02/2026 14:34:27' -> '09/02/2026'
    - '09/02/2026 Seg' -> '09/02/2026'
    - '09/02/2026' -> '09/02/2026' (sem alteração)
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
```

#### Passo 2: Integrar Limpeza na Função de Tratamento de Datas

Adicionar etapa de limpeza prévia antes de converter:

```python
@staticmethod
def _tratar_data_excel(series: pd.Series, contexto: str = "") -> pd.Series:
    # ... código de análise existente ...
    
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
    
    # Continuar com lógica de conversão usando series_limpa
    # ... resto do código ...
```

#### Passo 3: Integrar na Função de Formatação Individual

```python
@staticmethod
def formatar_data_brasileira(data_value, contexto: str = "") -> str:
    # ... código existente ...
    
    # Se for string, limpa primeiro (remove hora e dia da semana)
    data_str_original = valor_original_str
    data_str = DataProcessor.limpar_data_com_extras(valor_original_str)
    
    # Verificar se houve limpeza (dados extras removidos)
    if data_str != data_str_original:
        dados_extras_info = f" | DADOS EXTRAS REMOVIDOS: '{data_str_original[len(data_str):].strip()}'"
    
    # Continuar com conversão usando data_str limpa
    # ... resto do código ...
```

### 2.3 Benefícios

- 100% de conversão bem-sucedida de datas com extras
- Compatibilidade mantida com formatos existentes
- Logs informativos sobre limpeza realizada

---

## 3. LOGS CONTEXTUAIS DETALHADOS

### 3.1 Problema Identificado

- Logs de erro sem contexto suficiente
- Difícil identificar origem do problema
- Falta de dados para reproduzir erros

### 3.2 Solução Implementada

#### Padrão de Logs para Erros de API

```python
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
```

#### Padrão de Logs para Erros de Atualização

```python
try:
    self._api_patch(url, payload)
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
```

#### Padrão de Logs para Validações

```python
if not data_origem or data_origem.strip() == '':
    # Verificar se é caso válido (ex: "NO LOCAL") antes de logar como erro
    if no_local:
        logging.info(
            f"ℹ️ [{contexto}] Veículo NO LOCAL (sem data do Trafegus)\n"
            f"   📄 Arquivo: {arquivo_nome}\n"
            f"   🚛 Placa: {cavalo}\n"
            f"   📍 Linha Excel: {linha_excel}\n"
            f"   📝 Valor original: {repr(data_origem_raw)}\n"
            f"   ✅ Resultado válido"
        )
    else:
        logging.error(
            f"❌ [{contexto}] DATA VAZIA APÓS FORMATAÇÃO\n"
            f"   📝 Valor original: {repr(data_origem_raw)}\n"
            f"   📄 Arquivo: {arquivo_nome}\n"
            f"   🚛 Placa: {cavalo}\n"
            f"   📍 Linha Excel: {linha_excel}\n"
            f"   🔄 Usando fallback: data atual"
        )
```

### 3.3 Elementos Essenciais nos Logs

- **Contexto**: Arquivo, linha, identificadores relevantes
- **Valores**: Dados originais e tentados
- **Localização**: Sheet, célula, linha do Excel
- **Tipo de Erro**: Nome da exceção e mensagem
- **URLs/Payloads**: Para erros de API

---

## 4. VALIDAÇÕES INTELIGENTES

### 4.1 Problema Identificado

- Validações genéricas marcavam casos válidos como erro
- Exemplo: " | NO LOCAL" é resultado válido, não erro

### 4.2 Solução Implementada

#### Validação Condicional

```python
# Verificar primeiro se é caso válido
if no_local:
    # Caso válido - não é erro
    if not data_origem or data_origem.strip() == '':
        logging.info(...)  # Log informativo, não erro
        return " | NO LOCAL"
    else:
        return f"{data_origem} | NO LOCAL"
else:
    # Caso que precisa de data - aí sim é erro se vazio
    if not data_origem or data_origem.strip() == '':
        logging.error(...)  # Log de erro
        data_origem = datetime.now().strftime('%d/%m/%Y')  # Fallback
    return f"{data_origem} | {posicao_original}"
```

### 4.3 Benefícios

- Logs mais precisos (apenas erros reais)
- Menos ruído nos arquivos de log
- Melhor rastreamento de problemas

---

## 5. CHECKLIST DE IMPLEMENTAÇÃO

### Para Replicar em Outro Script:

- [ ] **1. Sistema de Logging**
  - [ ] Adicionar imports: `Path`, `datetime`
  - [ ] Criar função `setup_logging()`
  - [ ] Substituir `logging.basicConfig()` por `setup_logging()`
  - [ ] Testar criação de pasta `logs/`

- [ ] **2. Tratamento de Datas**
  - [ ] Adicionar import: `re`
  - [ ] Criar função `limpar_data_com_extras()`
  - [ ] Integrar limpeza em funções de tratamento de datas
  - [ ] Adicionar logs informativos sobre limpeza

- [ ] **3. Logs Contextuais**
  - [ ] Adicionar contexto em todos os logs de erro
  - [ ] Incluir: arquivo, linha, valores, URLs quando aplicável
  - [ ] Usar formato estruturado com quebras de linha

- [ ] **4. Validações Inteligentes**
  - [ ] Identificar casos válidos vs. erros reais
  - [ ] Usar `logging.info()` para casos válidos
  - [ ] Usar `logging.error()` apenas para erros reais

- [ ] **5. Testes**
  - [ ] Verificar criação de arquivos de log
  - [ ] Testar conversão de datas com extras
  - [ ] Validar logs contextuais
  - [ ] Confirmar que apenas erros aparecem no arquivo

---

## 6. EXEMPLOS DE CÓDIGO COMPLETO

### Exemplo 1: Estrutura Básica de Imports

```python
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

# Configurar logging
log_file_path = setup_logging()
load_dotenv()
```

### Exemplo 2: Classe DataProcessor Completa

```python
class DataProcessor:
    @staticmethod
    def limpar_data_com_extras(data_str: str) -> str:
        # ... código da função ...
    
    @staticmethod
    def _tratar_data_excel(series: pd.Series, contexto: str = "") -> pd.Series:
        # ... código com limpeza prévia ...
    
    @staticmethod
    def formatar_data_brasileira(data_value, contexto: str = "") -> str:
        # ... código com limpeza integrada ...
```

---

## 7. NOTAS IMPORTANTES

1. **Compatibilidade**: Todas as melhorias mantêm compatibilidade com código existente
2. **Performance**: Limpeza de datas é eficiente (regex simples)
3. **Manutenibilidade**: Código bem documentado e estruturado
4. **Rastreabilidade**: Logs permitem identificar origem de problemas rapidamente

---

## 8. ARQUIVOS DE REFERÊNCIA

- Script implementado: `Atualização Programados Drives.py`
- Plano de execução: `PLANO_EXECUCAO_LIMPEZA_DATAS.md`
- Logs de exemplo: `logs/Atualização Programados Drives_erros_*.log`

---

**Última atualização**: 2026-02-09

**Versão**: 1.0
