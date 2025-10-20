from prefect import flow, task, get_run_logger
from datetime import date
from pathlib import Path
import requests
import numpy as np
import copy
import json
import pandas as pd
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter1d
from typing import List, Dict, Any, Tuple

ANO_CAMPEONATO = 2025
NUM_SIMULACOES = 100000
DATA_HOJE = str(date.today())
OUTPUT_DIR = Path("assets/figs") 

@task(name="Get_Jogos_e_Times", retries=3)
def fetch_and_prepare_data(ano: int) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Busca os dados JSON da fonte e inicializa a lista de times."""
    
    r = requests.get(f'https://jsuol.com.br/c/monaco/utils/gestor/commons.js?callback=simulador_dados_jsonp&file=commons.uol.com.br/sistemas/esporte/modalidades/futebol/campeonatos/dados/{ano}/30/dados.json', verify=False)
    j = json.loads(r.content[22:-4])
    equipes = {e:j['equipes'][e]['nome-comum'] for e in j['equipes']}
    jogos = j['fases']['4139']['jogos']['id'].values()
    jogos = [{'mandante':equipes[j['time1']],'placar_mandante':j['placar1'],'visitante':equipes[j['time2']],'placar_visitante':j['placar2']} for j in jogos]
    
    times = []
    for j in jogos:
        if j['mandante'] not in times: times.append(j['mandante'])
        if j['visitante'] not in times: times.append(j['visitante'])
    times.sort()
    
    return jogos, times

@task(name="Run_Simulacoes_e_Salvar_TXT")
def run_simulation_and_save_data(jogos: List[Dict[str, Any]], times: List[str], num_sim: int, ano: int):
    """
    Executa o motor de simulação de Monte Carlo e anexa os resultados nos TXTs.
    (Aqui estaria todo o seu código de pontuação e loops de simulação)
    """
    logger = get_run_logger()
    
    dados_time_orig = {}
    for t in times:
        dados_time_orig[t] = {'nome':t, 'pontos':0, 'saldo_gols':0, 'vitorias':0}
        
    resultados = {t: [] for t in times}
    probabilidades = {t: [] for t in times}
    
    for _ in range(10): 
         dados_time = copy.deepcopy(dados_time_orig)
         for t in times: resultados[t].append(np.random.randint(1, 20)) 
    hoje = str(date.today())
    
    with open(f'brasileirao_long_{ano}.txt', 'a+', encoding='utf-8') as f:
         for t in times:
             for j in range(1,len(times)+1):
                 prob = str(np.round(resultados[t].count(j)/num_sim, 5))
                 probabilidades[t].append(prob)
                 f.write(hoje+';'+t+';'+str(j)+';'+prob+'\n')

    with open(f'brasileirao_{ano}.txt', 'a+', encoding='utf-8') as f:
        for t in times:
            f.write(';'.join([hoje,t]+probabilidades[t])+'\n')
            
    logger.info("Dados TXT gerados com sucesso.")
    return True

@task(name="Generate_Plots")
def generate_and_save_plots(ano: int) -> bool:
    """Carrega os dados TXT, gera todos os gráficos e salva na pasta /assets/figs."""
    
    logger = get_run_logger()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        short = pd.read_csv(f'brasileirao_{ano}.txt', sep=';', index_col=False, decimal='.')
        short.data = pd.to_datetime(short.data)
        last_short = short[short['data'] == max(short['data'])]
        short_final = last_short[last_short.columns[1:]].set_index('time')
        short_final.sort_values(by=[str(i) for i in list(range(20,0,-1))], inplace=True)
        short_final.plot(kind='bar', stacked=True, figsize=(20,15), colormap='cool')
        plt.savefig(OUTPUT_DIR / f'short_final_{ano}.png') # Salva no caminho persistente        
        logger.info(f"Gráfico short_final_{ano}.png salvo em {OUTPUT_DIR}.")        
        return True
    except Exception as e:
        logger.error(f"Falha ao gerar gráficos: {e}")
        return False


@task(name="Git_Commit_Data_Files")
def commit_and_push_data():
    """
    Commita e faz o push dos arquivos TXT e PNG gerados.
    (Essa task é copiada do seu workflow original)
    """
    logger = get_run_logger()
    try:
        subprocess.run(["git", "config", "user.name", "lfkopp"], check=True)
        subprocess.run(["git", "config", "user.email", "lfkopp@gmail.com"], check=True)
        subprocess.run(["git", "add", "."], check=True)
        
        commit_check = subprocess.run(["git", "diff", "--staged", "--quiet"], check=False, capture_output=True)
        
        if commit_check.returncode == 0:
            logger.info("Nenhuma mudança nos dados detectada. Commit ignorado.")
        else:
            logger.info("Arquivos TXT/PNG atualizados. Commitando e enviando para o GitHub.")
            subprocess.run(["git", "commit", "-m", "feat:Automated Brasileirão data update (Prefect)"], check=True)
            subprocess.run(["git", "push"], check=True)
            
        return True
    except Exception as e:
        logger.error(f"Falha no Git Commit/Push: {e}")
        raise

@flow(name="Brasileirao-Update-Diario")
def brasileirao_update_flow(ano=ANO_CAMPEONATO, num_sim=NUM_SIMULACOES):
    """O Flow principal agendado para atualizar e versionar os dados do Brasileirão."""

    jogos, times = fetch_and_prepare_data(ano)
    simulation_success = run_simulation_and_save_data(jogos, times, num_sim, ano)
    
    if not simulation_success:
        return 
    plot_success = generate_and_save_plots(ano)
    
    if not plot_success:
        logger.warning("Gráficos não puderam ser gerados, mas continuaremos para o commit.")
        
    commit_success = commit_and_push_data()
    
    if commit_success:
        print("Pipeline de atualização diária concluído e dados versionados no GitHub.")

if __name__ == "__main__":
    brasileirao_update_flow()
