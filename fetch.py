import requests
import random
import time
import os
import pandas as pd
from datetime import datetime, timedelta

# ================= CONFIGURAÇÕES =================
BASE_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"
API_KEY  = input("Cole aqui sua API KEY: ")
total_desejado = input("Quantos tweets você deseja coletar? ")
# =================================================

def get_now():
    """Retorna o horário atual formatado para o log."""
    return datetime.now().strftime("%H:%M:%S")

def salvar_dados(dados_lista, nome_arquivo):
    """Achata o JSON e salva todas as colunas no CSV."""
    if dados_lista:
        df = pd.json_normalize(dados_lista)
        for col in df.columns:
            if 'id' in col.lower() or 'id_str' in col.lower():
                df[col] = df[col].astype(str)
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
        return True
    return False

def buscar_amostra_temporal(n_total, data_inicio, data_fim):
    # Validação da opção de query
    while True:
        opcao_query = input("Qual query? (1: Total PT | 2: PT + @Grok): ")
        if opcao_query in ["1", "2"]:
            break
        print("Opção inválida. Digite 1 para o total dos tweets em português e 2 para os tweets em português que citam o Grok.")

    if opcao_query == "1":
        nome_arquivo = "tweets_pt_sem_interacoes_com_Grok.csv"
    else:
        nome_arquivo = "tweets_com_interacoes_com_Grok.csv"

    start_dt = datetime.strptime(data_inicio, '%Y-%m-%d')
    end_dt = datetime.strptime(data_fim, '%Y-%m-%d')
    intervalo_segundos = int((end_dt - start_dt).total_seconds())
    
    dados_brutos = []
    ids_coletados = set()
    headers = {"X-API-Key": API_KEY}
    
    # --- CARREGAMENTO DE PROGRESSO ---
    if os.path.exists(nome_arquivo):
        try:
            df_existente = pd.read_csv(nome_arquivo)
            if not df_existente.empty:
                col_id = 'id' if 'id' in df_existente.columns else 'ID'
                df_existente[col_id] = df_existente[col_id].astype(str)
                dados_brutos = df_existente.to_dict('records')
                ids_coletados = set(df_existente[col_id].tolist())
                print(f"[{get_now()}] Arquivo encontrado: {len(dados_brutos)} registros carregados.")
        except Exception:
            print(f"[{get_now()}] Iniciando nova coleta.")

    if len(dados_brutos) >= n_total:
        print(f"[{get_now()}] Meta de {n_total} já atingida no arquivo.")
        return

    print(f"[{get_now()}] Iniciando busca...")

    try:
        # --- OPÇÃO 1: SORTEIO ALEATÓRIO (Busca Geral) ---
        if opcao_query == "1":
            print(f"[{get_now()}] Modo: Sorteio Aleatório Ativado.")
            while len(dados_brutos) < n_total:
                segundo_aleatorio = random.randint(0, intervalo_segundos)
                data_sorteada = start_dt + timedelta(seconds=segundo_aleatorio)
                tempo_limite = data_sorteada.strftime('%Y-%m-%d_%H:%M:%S_UTC')
                
                q_string = f"lang:pt since:{data_inicio} until:{tempo_limite}"
                
                cursor = None
                for pagina in range(2):
                    time.sleep(3) 
                    params = {'query': q_string, 'queryType': 'Latest', 'limit': 20}
                    if cursor: params['cursor'] = cursor

                    response = requests.get(BASE_URL, params=params, headers=headers)
                    if response.status_code == 200:
                        res_json = response.json()
                        tweets = res_json.get('tweets', [])
                        cursor = res_json.get('next_cursor')
                        if not tweets: break

                        novos = 0
                        for t in tweets:
                            t_id = str(t.get('id'))
                            if t_id not in ids_coletados and len(dados_brutos) < n_total:
                                dados_brutos.append(t)
                                ids_coletados.add(t_id)
                                novos += 1

                        if novos > 0:
                            salvar_dados(dados_brutos, nome_arquivo)
                            print(f"[{get_now()}] {len(dados_brutos)}/{n_total} | +{novos} de {tempo_limite}")
                        
                        if not cursor or len(dados_brutos) >= n_total: break
                    elif response.status_code == 429:
                        print(f"\n[{get_now()}] Rate Limit (429). Pausa de 30s...")
                        time.sleep(30)
                        break
                    else: break
                time.sleep(1)

        # --- OPÇÃO 2: SEQUENCIAL POR PAGINAÇÃO (Busca Grok) ---
        else:
            print(f"[{get_now()}] Modo: Paginação Sequencial Ativado (sem sorteio).")
            # Define o ponto de partida como o fim do período e vai retroagindo
            tempo_final = f"{data_fim}_23:59:59_UTC"
            q_string = f"(to:Grok OR @Grok) lang:pt since:{data_inicio} until:{tempo_final}"
            cursor = None
            
            while len(dados_brutos) < n_total:
                time.sleep(3) # Pausa preventiva para evitar 429
                params = {
                    'query': q_string,
                    'queryType': 'Latest',
                    'limit': 20
                }
                if cursor:
                    params['cursor'] = cursor

                response = requests.get(BASE_URL, params=params, headers=headers)
                
                if response.status_code == 200:
                    res_json = response.json()
                    tweets = res_json.get('tweets', [])
                    cursor = res_json.get('next_cursor')
                    
                    if not tweets:
                        print(f"[{get_now()}] Fim dos resultados disponíveis.")
                        break

                    novos = 0
                    for t in tweets:
                        t_id = str(t.get('id'))
                        if t_id not in ids_coletados and len(dados_brutos) < n_total:
                            dados_brutos.append(t)
                            ids_coletados.add(t_id)
                            novos += 1

                    if novos > 0:
                        salvar_dados(dados_brutos, nome_arquivo)
                        print(f"[{get_now()}] {len(dados_brutos)}/{n_total} | +{novos} (Página Sequencial)")
                    
                    if not cursor:
                        print(f"[{get_now()}] Paginação finalizada (sem next_cursor).")
                        break
                        
                elif response.status_code == 429:
                    print(f"\n[{get_now()}] Rate Limit (429). Pausa de 30s...")
                    time.sleep(30)
                else:
                    print(f"[{get_now()}] Erro {response.status_code}. Interrompendo coleta.")
                    break

    except KeyboardInterrupt:
        print(f"\n[{get_now()}] Interrompido pelo usuário. Finalizando salvamento...")

    salvar_dados(dados_brutos, nome_arquivo)
    print(f"[{get_now()}] Coleta encerrada. Arquivo: {nome_arquivo}")

if __name__ == "__main__":
    # Mantém o período solicitado: Outubro a Novembro de 2025
    buscar_amostra_temporal(int(total_desejado), "2025-10-27", "2025-11-27")