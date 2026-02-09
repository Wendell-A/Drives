# -*- coding: utf-8 -*-
from playwright.sync_api import sync_playwright
import os
import time
from pathlib import Path

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================
# O caminho da pasta de Downloads do usuário é obtido dinamicamente
download_dir = str(Path.home() / "Documentos")
usuario = "qsdev4"
senha = "Dev0100@Refit"


# ==============================================================================
# FUNÇÃO: EXPORTAR DADOS
# ==============================================================================
def exportar_dados(page, frame, download_dir):
    try:
        print("\n--- EXPORTAÇÃO DE DADOS ---")

        page.mouse.move(550, 550)
        time.sleep(1)
        page.mouse.click(550, 550, button="right")
        print("🖱️ Clique com botão direito executado.")

        menu_popover = frame.locator("div.lui-popover")
        menu_popover.wait_for(state="visible", timeout=30000)
        print("✅ Menu Popover visível!")

        item_baixar_como = menu_popover.locator("li#export-group")
        item_baixar_como.click()
        print("✔️ Clicado em 'Baixar como...'")
        
        # --- CORREÇÃO APLICADA AQUI ---
        # Em vez de procurar dentro do popover antigo, procuramos pelo novo item de menu visível
        print("🔍 Procurando a opção 'Dados' no submenu...")
        # Usar get_by_role é mais confiável para encontrar elementos de menu
        dados_item = frame.get_by_role("menuitem", name="Dados")
        dados_item.wait_for(state="visible", timeout=15000)
        dados_item.click()
        print("✔️ Clicado em 'Data'!")

        popup_configuracoes = frame.locator("div#data-export-settings-dialog")
        popup_configuracoes.wait_for(state="visible", timeout=60000)
        print("✅ Popup de configurações visível!")

        exportar_btn = popup_configuracoes.locator('button[tid="table-export"]')
        exportar_btn.click()
        print("🔘 Clicando em 'Exportar'...")

        popup_conclusao = page.locator('div[ng-if="state === states.COMPLETED"]')
        popup_conclusao.wait_for(state="visible", timeout=90000)
        print("✅ Exportação concluída!")

        download_link = popup_conclusao.locator("a.export-url")
        with page.expect_download(timeout=180000) as download_info:
            download_link.click()
            download = download_info.value

        destino = os.path.join(download_dir, "qlik_sense.xlsx")
        download.save_as(destino)
        print(f"✅ Arquivo salvo em: {destino}")

    except Exception as e:
        print(f"❌ Erro durante exportação: {e}")

    time.sleep(2)


# ==============================================================================
# FUNÇÃO PRINCIPAL
# ==============================================================================
def automacao_com_playwright():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            http_credentials={"username": usuario, "password": senha},
            ignore_https_errors=True,
        )

        page = context.new_page()
        page.set_viewport_size({"width": 1500, "height": 1050})

        # Login inicial
        page.goto("https://proddc02vmqks03.grupo.fit/sense/app/0f96c01f-677e-4925-9a88-eadd4c6db60c")
        print("✅ Login HTTP básico realizado!")
        time.sleep(3)

        # Acesso ao app
        page.goto(
            "https://proddc02vmqks03.grupo.fit/sense/app/0f96c01f-677e-4925-9a88-eadd4c6db60c/sheet/c2d4a0c5-fd33-4787-b0a5-795a5c13653d/state/analysis",
            wait_until="load",
        )
        print("✅ App carregado (fase inicial).")

        # Espera até a barra de seleções aparecer
        print("⏳ Aguardando o Qlik Sense renderizar a barra de seleções...")
        page.wait_for_selector(
            'div[tid="current-selections"], [data-testid*="current-selections"]',
            state="visible",
            timeout=90000,
        )
        print("✅ Barra de seleções detectada! Página pronta para interação.")

        # Localiza o frame principal
        frame = None
        for _ in range(20):
            for f in page.frames:
                if "sense" in f.url:
                    frame = f
                    break
            if frame:
                break
            time.sleep(1)
        if not frame:
            raise Exception("❌ Não encontrou o iframe correto do Qlik Sense")
        print("✅ Frame principal identificado!")

        # ----------------------------------------------------------------------
        # PASSO 1: TENTAR LIMPAR FILTROS ANTERIORES (VERSÃO AJUSTADA)
        # ----------------------------------------------------------------------
        print("\n--- PASSO 1: LIMPAR FILTROS ---")
        try:
            clear_button = None
            # Procura em todos os frames para garantir que achamos o elemento certo
            for f in page.frames:
                try:
                    # 1) seletor mais específico que, normalmente, aponta para o botão global
                    candidate = f.locator('button[title="Limpar todas as seleções"], button[data-tid="current-selections-clear"]')
                    cnt = candidate.count()
                    if cnt == 1:
                        clear_button = candidate
                        frame = f
                        break
                    elif cnt > 1:
                        # se houver múltiplos, escolhe o que tem o title que contém "todas as seleções"
                        for i in range(cnt):
                            btn = candidate.nth(i)
                            title = btn.get_attribute("title")
                            if title and "todas as seleções" in title.lower():
                                clear_button = btn
                                frame = f
                                break
                        if clear_button:
                            break

                    # 2) fallback por role/name (mais legível)
                    role_btn = f.get_by_role("button", name="Limpar todas as seleções")
                    if role_btn.count() == 1:
                        clear_button = role_btn
                        frame = f
                        break

                except Exception:
                    # ignora erros deste frame e tenta o próximo
                    continue

            if clear_button:
                # garante visibilidade e clica (sem force inicialmente)
                clear_button.wait_for(state="visible", timeout=15000)
                try:
                    clear_button.click()
                except Exception:
                    # se estiver coberto por overlay, tenta com force
                    clear_button.click(force=True)
                print("🧹 Filtros anteriores limpos com sucesso!")
                time.sleep(2)
            else:
                # captura para debug visual (ajusta o caminho se quiser)
                timestamp = int(time.time())
                dump_path = f"debug_clear_button_not_found_{timestamp}.png"
                page.screenshot(path=dump_path, full_page=True)
                print(f"⚠️ Botão 'Limpar' não encontrado. Screenshot salva em: {dump_path}")

        except Exception as e:
            print(f"⚠️ Erro ao tentar clicar em 'Limpar': {e}")
        print("--- FIM DO PASSO 1 ---\n")


        # PASSO 2: SELECIONAR FILTRO 'STATUS PEDIDO'
        # ----------------------------------------------------------------------
        print("--- PASSO 2: SELECIONAR 'STATUS PEDIDO' ---")
        try:
            status_pedido_button = frame.locator('[data-testid="collapsed-title-Status Pedido"]')
            status_pedido_button.wait_for(state="visible", timeout=10000)
            status_pedido_button.click()
            print("✅ Botão 'Status Pedido' clicado!")
            time.sleep(2)

            pendente_option = frame.locator('.RowColumn-cell[title="PENDENTE"]')
            pendente_option.wait_for(state="visible", timeout=10000)
            pendente_option.click()
            print("✅ Opção 'PENDENTE' selecionada!")

            frame.locator('[data-testid="actions-toolbar-confirm"]').click()
            print("✅ Seleção confirmada.")
            time.sleep(2)

        except Exception as e:
            print(f"⚠️ Não foi possível aplicar o filtro 'PENDENTE': {e}")
        print("--- FIM DO PASSO 2 ---\n")

        # ----------------------------------------------------------------------
        # PASSO 3: EXPORTAR DADOS
        # ----------------------------------------------------------------------
        print("--- PASSO 3: EXPORTAR DADOS ---")
        exportar_dados(page, frame, download_dir)
        print("--- FIM DO PASSO 3 ---\n")

        browser.close()


# ==============================================================================
# EXECUÇÃO
# ==============================================================================
if __name__ == "__main__":
    automacao_com_playwright()

