import requests
import random
import time
import os
import pandas as pd
from datetime import datetime, timedelta

# ================= CONFIGURAÇÕES =================
BASE_URL = "https://api.twitterapi.io/twitter/tweet/advanced_search"
API_KEY  = input("Cole aqui sua API KEY:")
total_desejado = input("Quantos tweets você deseja coletar? ")
# =================================================

def get_now():
    """Retorna o horário atual formatado para o log."""
    return datetime.now().strftime("%H:%M:%S")

def salvar_dados(dados_lista, nome_arquivo):
    if dados_lista:
        # json_normalize lida com os dados brutos salvando TUDO (autor, métricas, etc)
        df = pd.json_normalize(dados_lista)
        for col in df.columns:
            if 'id' in col.lower():
                df[col] = df[col].astype(str)
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
        return True
    return False

def buscar_amostra_temporal(n_total, data_inicio, data_fim):
    # Usamos nomes diferentes para não dar conflito no loop
    opcao_query = input("Qual query? (1: Total PT | 2: PT + @Grok): ")
    if opcao_query not in ["1", "2"]:
        print("Opção inválida. Digite 1 para o total dos tweets em português e 2 para os tweets em português que citam o Grok.")
        opcao_query = input
    elif opcao_query == "1":
        nome_arquivo = f"tweets_pt_sem_interacoes_com_Grok.csv"
    else:
        nome_arquivo = f"tweets_com_interacoes_com_Grok.csv"

    start_dt = datetime.strptime(data_inicio, '%Y-%m-%d')
    end_dt = datetime.strptime(data_fim, '%Y-%m-%d')
    intervalo_segundos = int((end_dt - start_dt).total_seconds())
    
    dados_brutos = []
    ids_coletados = set()
    headers = {"X-API-Key": API_KEY}
    
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
            print(f"[{get_now()}] Iniciando do zero.")

    if len(dados_brutos) >= n_total:
        print(f"[{get_now()}] Meta atingida.")
        return

    print(f"[{get_now()}] Iniciando busca aleatória em modo de segurança...")

    try:
        while len(dados_brutos) < n_total:
            # 1. Sorteio aleatório no tempo
            segundo_aleatorio = random.randint(0, intervalo_segundos)
            data_sorteada = start_dt + timedelta(seconds=segundo_aleatorio)
            tempo_limite = data_sorteada.strftime('%Y-%m-%d_%H:%M:%S_UTC') #Data no padrão da API para busca (ex: 2025-10-27_14:30:00_UTC)
            
            # 2. Define a string da query baseada na opção
            if opcao_query == "1":
                q_string = f"lang:pt since:{data_inicio} until:{tempo_limite}"
            else:
                q_string = f"(to:Grok OR @Grok) lang:pt since:{data_inicio} until:{tempo_limite}"
            
            cursor = None
            for pagina in range(2):
                # Pausa preventiva para evitar o 429 logo de cara
                time.sleep(3) 
                
                params = {
                    'query': q_string,
                    'queryType': 'Latest',
                    'limit': 20
                }
                if cursor:
                    params['cursor'] = cursor

                try:
                    response = requests.get(BASE_URL, params=params, headers=headers)
                    
                    if response.status_code == 200:
                        res_json = response.json()
                        tweets = res_json.get('tweets', [])
                        cursor = res_json.get('next_cursor')

                        if not tweets:
                            break

                        novos_na_pagina = 0
                        for t in tweets:
                            t_id = str(t.get('id'))
                            if t_id not in ids_coletados and len(dados_brutos) < n_total:
                                dados_brutos.append(t)
                                ids_coletados.add(t_id)
                                novos_na_pagina += 1

                        if novos_na_pagina > 0:
                            salvar_dados(dados_brutos, nome_arquivo)
                            print(f"[{get_now()}] {len(dados_brutos)}/{n_total} | +{novos_na_pagina} de {tempo_limite}")
                        
                        if not cursor or len(dados_brutos) >= n_total:
                            break
                    
                    elif response.status_code == 429:
                        print(f"\n[{get_now()}] Rate Limit (429). Pausa de 30s para resetar...")
                        time.sleep(30)
                        break 
                    else:
                        print(f"\n[{get_now()}] Erro {response.status_code}. Tentando novo ponto...")
                        break

                except Exception as e:
                    print(f"\n[{get_now()}] Erro na requisição: {e}")
                    time.sleep(5)
                    break
            
            time.sleep(1) # Pausa entre sorteios

    except KeyboardInterrupt:
        print(f"\n[{get_now()}] Interrompido. Progresso salvo.")

    salvar_dados(dados_brutos, nome_arquivo)
    print(f"[{get_now()}] Coleta finalizada.")

if __name__ == "__main__":
    buscar_amostra_temporal(int(total_desejado), "2025-10-27", "2025-11-27")