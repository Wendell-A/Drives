from playwright.sync_api import sync_playwright, TimeoutError
import calendar
from datetime import date
import time
import os

def test():
    
    # Pega o caminho para a pasta 'home' do usuário atual
    home_dir = os.path.expanduser('~')
    # Cria o caminho completo para a pasta 'Documentos'
    download_dir = os.path.join(home_dir, 'Documentos')
    # Garante que o diretório de download exista.
    os.makedirs(download_dir, exist_ok=True)
    print(f"✅ Arquivos serão salvos em: {download_dir}")

    # Data de hoje
    hoje = date.today()
    ano_atual = hoje.year
    mes_atual_num = hoje.month

    # Conversão número -> nome mês (PT-BR abreviado)
    meses_map = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
    mes_atual_str = meses_map[mes_atual_num - 1]
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            executable_path="C:/Program Files/Google/Chrome/Application/chrome.exe",
            headless=False
        )

        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        page.set_viewport_size({"width": 1500, "height": 1050})

        page.goto("https://bi.gruporefit.com/QvAJAXZfc/opendoc.htm?document=produ%C3%A7%C3%A3o%5Clogistica_vendas_transp.qvw&lang=en-US&host=QVS%40proddcspvmqv01")
        
        # Login
        page.locator("#username").fill("GRUPOREFIT\\QvFiscal")
        page.locator("#password").fill("QlikviewFsc200")
        page.keyboard.press("Tab")
        page.keyboard.press("Enter")
        print("✅ Login realizado com sucesso!")
        time.sleep(8)
        
        # --- LOOP PRINCIPAL DE RETENTATIVAS ---
        max_attempts = 5
        for attempt in range(1, max_attempts + 1):
            print(f"\n--- Iniciando Tentativa {attempt} de {max_attempts} ---")
            try:
                # A partir da segunda tentativa, recarrega a página
                if attempt > 1:
                    print("Recarregando a página (F5) para a nova tentativa...")
                    page.reload(timeout=60000)
                    time.sleep(8)

                # --- ETAPA ADICIONADA: LIMPAR TODOS OS FILTROS ---
                print("Iniciando limpeza de todos os filtros existentes...")
                # O seletor é baseado na classe específica do botão "Limpar"
                clear_filters_button = page.locator("li.ctx-menu-action-CLEARSTATE")
                
                # Aguarda o botão estar visível e clica nele
                clear_filters_button.wait_for(state='visible', timeout=25000)
                clear_filters_button.click()
                
                print("✔️ Botão 'Limpar Filtros' clicado. Aguardando 8 segundos para a página atualizar.")
                # Pausa crucial para o QlikView processar a limpeza
                time.sleep(8)
                # --- FIM DA ETAPA ADICIONADA ---


                # -------- FILTRO DE ANO -------- #
                print("Procurando filtro de ano...")
                filtro_ano_linha = page.locator("div[style*='width: 367px'][style*='height: 16px']")
                todos_anos = filtro_ano_linha.locator("div[class*='QvOptional'], div[class*='QvExcluded'], div[class*='QvSelected']")
                
                if todos_anos.count() == 0:
                    if attempt < max_attempts:
                        wait_time = 15 + (attempt - 1) * 5
                        print(f"⚠️ Filtro de ano não encontrado. Aguardando {wait_time}s antes de tentar novamente.")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise TimeoutError("Filtro de ano não foi encontrado após todas as tentativas.")

                ano_selecionado = None
                # Após limpar, nenhum ano estará selecionado, então o loop não encontrará 'QvSelected'
                # O código abaixo já lida com isso corretamente
                for i in range(todos_anos.count()):
                    l = todos_anos.nth(i)
                    if "QvSelected" in (l.get_attribute("class") or ""):
                        ano_selecionado = l.get_attribute("title")
                        break
                
                print(f"Ano selecionado atualmente: {ano_selecionado}")
                if ano_selecionado != str(ano_atual):
                    print(f"Selecionando ano {ano_atual}...")
                    ano_locator = filtro_ano_linha.locator(f"div[title='{ano_atual}']")
                    ano_locator.first.click(force=True)
                    print(f"✔️ Ano {ano_atual} selecionado")
                else:
                    print(f"✔️ Ano {ano_atual} já está selecionado.")

                # --- PAUSA ESTRATÉGICA ADICIONADA ---
                # Após selecionar o ano, aguardamos o filtro de mês ser atualizado pela página
                print("Aguardando 5 segundos para o filtro de mês atualizar...")
                time.sleep(15)
                # --- FIM DA PAUSA ---


                # -------- FILTRO DE MÊS -------- #
                print("Procurando filtro de mês...")
                filtro_mes_linha = page.locator("div[style*='width: 415px'][style*='height: 16px']")
                todos_meses = filtro_mes_linha.locator("div[class*='QvOptional'], div[class*='QvExcluded'], div[class*='QvSelected']")

                if todos_meses.count() == 0:
                    if attempt < max_attempts:
                        wait_time = 15 + (attempt - 1) * 5
                        print(f"⚠️ Filtro de mês não encontrado. Aguardando {wait_time}s antes de tentar novamente.")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise TimeoutError("Filtro de mês não foi encontrado após todas as tentativas.")
                
                mes_selecionado = None
                for i in range(todos_meses.count()):
                    l = todos_meses.nth(i)
                    if "QvSelected" in (l.get_attribute("class") or ""):
                        mes_selecionado = l.get_attribute("title")
                        break

                print(f"Mês selecionado atualmente: {mes_selecionado}")
                if mes_selecionado != mes_atual_str:
                    print(f"Selecionando mês {mes_atual_str}...")
                    mes_locator = filtro_mes_linha.locator(f"div[title='{mes_atual_str}']")
                    mes_locator.first.click(force=True)
                    print(f"✔️ Mês {mes_atual_str} selecionado")
                else:
                    print(f"✔️ Mês {mes_atual_str} já está selecionado.")
                
                time.sleep(12)

                # -------- EXPORT -------- #
                print("Iniciando processo de exportação...")
                export_menu_x, export_menu_y = 1300, 475
                
                time.sleep(8)
                with page.expect_download(timeout=180000) as download_info:
                    page.mouse.click(export_menu_x, export_menu_y, button="right")
                    press_here_locator = page.locator("ul.ctx-menu li:has-text('Send to Excel')")
                    press_here_locator.wait_for(state="visible", timeout=50000)
                    press_here_locator.click()
                    print("✔️ Clicado em 'Export' para iniciar o download.")

                download = download_info.value

                # Caminho fixo para salvar o arquivo como XLSX
                destino = os.path.join(download_dir, "qlik_view.xlsx")

                # Salva o arquivo baixado no destino especificado
                download.save_as(destino)

                print(f"✅ Arquivo salvo com sucesso em: {destino}")
                break

            except Exception as e:
                print(f"❌ Ocorreu um erro na tentativa {attempt}: {e}")
                if attempt < max_attempts:
                    print("Aguardando 15 segundos antes da próxima tentativa.")
                    time.sleep(15)
                else:
                    print(f"\n❌ Falha crítica. O script falhou após {max_attempts} tentativas.")
        
        browser.close()

if __name__ == "__main__":
    test()