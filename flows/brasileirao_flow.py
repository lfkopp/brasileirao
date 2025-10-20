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
import subprocess
from typing import List, Dict, Any, Tuple
import os

ANO_CAMPEONATO = 2025
NUM_SIMULACOES = 100000
OUTPUT_DIR = Path(".") 

def get_times(jogos: List[Dict[str, Any]]) -> List[str]:
    times = []
    for j in jogos:
        if j['mandante'] not in times:
            times.append(j['mandante'])
        if j['visitante'] not in times:
            times.append(j['visitante'])
    times.sort()
    return times

def gera_dados(times: List[str]) -> Dict[str, Dict[str, Any]]:
    dados_time = {}
    for t in times:
        dados_time[t] = {
            'nome': t,
            'pontos': 0,
            'saldo_gols': 0,
            'vitorias': 0,
        }
    return dados_time

def pontua(line: Dict[str, Any], dados_time: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    if line['placar_mandante'] is None:
        placar_m = int(np.random.poisson(2))
        placar_v = int(np.random.poisson(1))
    else:
        placar_m = int(line['placar_mandante'])
        placar_v = int(line['placar_visitante'])
        
    if placar_m > placar_v:
        dados_time[line['mandante']]['pontos'] += 3
        dados_time[line['mandante']]['vitorias'] += 1
    elif placar_m < placar_v:
        dados_time[line['visitante']]['pontos'] += 3
        dados_time[line['visitante']]['vitorias'] += 1
    else:
        dados_time[line['mandante']]['pontos'] += 1
        dados_time[line['visitante']]['pontos'] += 1
        
    saldo = placar_m - placar_v
    dados_time[line['mandante']]['saldo_gols'] += saldo
    dados_time[line['visitante']]['saldo_gols'] -= saldo
    return dados_time

@task(name="Fetch_CBF_Data", retries=3, retry_delay_seconds=15)
def fetch_cbf_data(ano: int) -> List[Dict[str, Any]]:
    """Busca o JSON dos jogos e retorna a lista."""
    logger = get_run_logger()
    logger.info(f"Buscando jogos do ano {ano}...")
    try:
        r = requests.get(f'https://jsuol.com.br/c/monaco/utils/gestor/commons.js?callback=simulador_dados_jsonp&file=commons.uol.com.br/sistemas/esporte/modalidades/futebol/campeonatos/dados/{ano}/30/dados.json', verify=False)
        j = json.loads(r.content[22:-4])
        equipes = {e:j['equipes'][e]['nome-comum'] for e in j['equipes']}
        jogos = j['fases']['4139']['jogos']['id'].values()
        jogos = [{'mandante':equipes[j['time1']],'placar_mandante':j['placar1'],'visitante':equipes[j['time2']],'placar_visitante':j['placar2']} for j in jogos]
        logger.info(f"Dados extraídos com sucesso. Total de jogos encontrados: {len(jogos)}")
        return jogos
    except Exception as e:
        logger.error(f"Falha na extração de dados: {e}")
        raise
        
@task(name="Run_Monte_Carlo_and_Save_Files")
def run_simulation_and_save_data(jogos: List[Dict[str, Any]], num_sim: int, ano: int):
    logger = get_run_logger()
    times = get_times(jogos)
    dados_time_orig = gera_dados(times)
    resultados = {t: [] for t in times}
    probabilidades = {t: [] for t in times}
    for _ in range(num_sim):
        dados_time = copy.deepcopy(dados_time_orig)
        for line1 in jogos:
            dados_time = pontua(line1, dados_time)
        
        arr = []
        for i in times:
            pontuacao = (  dados_time[i]['pontos']*1000000 + 
                            dados_time[i]['vitorias']*1000 +
                            dados_time[i]['saldo_gols'] +200 + np.random.random()/1000)
            arr.append((pontuacao, dados_time[i]['nome']))
        arr.sort()
        for i, r in enumerate(arr):
            resultados[r[1]].append(len(arr) - i)
    hoje = str(date.today())
    with open(f'brasileirao_long_{ano}.txt', 'a+', encoding='utf-8') as f:
        for t in times:
            for j in range(1, len(times) + 1):
                prob = str(np.round(resultados[t].count(j) / num_sim, 5))
                probabilidades[t].append(prob)
                f.write(hoje + ';' + t + ';' + str(j) + ';' + prob + '\n')
    with open(f'brasileirao_{ano}.txt', 'a+', encoding='utf-8') as f:
        for t in times:
            f.write(';'.join([hoje, t] + probabilidades[t]) + '\n')
    logger.info("Arquivos TXT de dados atualizados com sucesso.")
    return True

@task(name="Generate_and_Save_Plots")
def generate_and_save_plots(ano: int):
    logger = get_run_logger()
    figuras = Path('figs')
    figuras.mkdir(parents=True, exist_ok=True) 

    try:
        short = pd.read_csv(f'brasileirao_{ano}.txt', sep=';', index_col=False, decimal='.')
        short.data = pd.to_datetime(short.data)
        
        last_short = short[short['data'] == max(short['data'])]
        short_final = last_short[last_short.columns[1:]].set_index('time')
        short_final.sort_values(by=[str(i) for i in list(range(20,0,-1))], inplace=True)
        short_final.plot(kind='bar',stacked=True,figsize=(20,15), colormap='cool')
        plt.savefig(figuras / f'short_final_{ano}.png')

        long = pd.read_csv(f'brasileirao_long_{ano}.txt', sep=';', index_col=False, decimal='.')
        long = pd.DataFrame(long.groupby(['data','time','pos'])['chance'].mean()).reset_index()
        long.data = pd.to_datetime(long.data)
        long['points'] = (20-long['pos']) * long['chance']
        long2 = long.groupby(['data','time'])['points'].mean().unstack()
        long2.sort_values(long2.index[-1],axis=1).plot(kind='area',stacked=True,figsize=(15,20), colormap='brg')
        plt.savefig(figuras / f'long2_stacked_{ano}.png')
        
        long2 = long.groupby(['time','data'])['points'].mean().unstack()
        long2.sort_values(long2.columns.max()).plot(kind='barh',stacked=False,figsize=(15,20), colormap='cool')
        plt.savefig(figuras / f'long2_{ano}.png')

        long3 = long[long['data'] == max(long['data'])]
        times = long3['time'].unique()
        fig = plt.figure(figsize=(15,10))
        for t in times:
            sublong = long3[long3['time'] == t]
            ysmoothed = gaussian_filter1d(sublong.chance, sigma=3)
            plt.plot(sublong.pos,ysmoothed,label=t)
        plt.legend(loc=1,ncol=5,fontsize='medium')
        plt.xticks(np.arange(1, 21, 1))
        plt.grid()
        plt.xlim(1,20)
        plt.ylim(0)
        plt.ylabel('Chances (%)')
        plt.xlabel('Position')
        plt.title('Chances for each team falling into x^th position at the end of the Brasileirão')
        plt.savefig(figuras / f'long3_{ano}.png')
        logger.info(f"Todos os 4 gráficos salvos em {figuras}/.")
        return True
    except Exception as e:
        logger.error(f"Erro na plotagem/leitura dos dados: {e}")
        raise 

@task(name="Git_Commit_Data_Files", log_prints=True)
def commit_and_push_data():
    logger = get_run_logger()
    GIT_TOKEN = os.getenv('GIT_PUSH_TOKEN')
    if not GIT_TOKEN:
        logger.error("GIT_PUSH_TOKEN não foi encontrado no ambiente. O push irá falhar.")
        raise ValueError("GIT_PUSH_TOKEN está ausente.")
    try:
        subprocess.run(["git", "init"], check=True, capture_output=True)
        REPO_URL = "github.com/lfkopp/brasileirao.git"
        PUSH_URL = f"https://lfkopp:{GIT_TOKEN}@{REPO_URL}"
        subprocess.run(["git", "config", "user.name", "lfkopp"], check=True)
        subprocess.run(["git", "config", "user.email", "lfkopp@gmail.com"], check=True)      
    except subprocess.CalledProcessError as e:
        logger.error(f"Falha na configuração do Git: {e.stderr.decode()}")
        raise
    try:
        subprocess.run(["git", "add", "."], check=True)
        commit_check = subprocess.run(["git", "diff", "--staged", "--quiet"], check=False, capture_output=True)
        if commit_check.returncode == 0:
            logger.info("Nenhuma mudança nos dados detectada. Commit ignorado.")
            return True
        else:
            commit_message = f"feat:Automated Brasileirão data update ({date.today()})"
            logger.info(f"Arquivos TXT/PNG atualizados. Commitando: '{commit_message}'")
            subprocess.run(["git", "commit", "-m", commit_message], check=True)
            subprocess.run(["git", "push", PUSH_URL], check=True)
            logger.info("Push para o GitHub concluído com sucesso.")
            return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Falha crítica no Git Commit/Push: {e.stderr.decode()}")
        raise
    except Exception as e:
        logger.error(f"Erro inesperado durante a operação Git: {e}")
        raise

@flow(name="Brasileirao-Update-Diario", log_prints=True)
def brasileirao_update_flow(ano=ANO_CAMPEONATO, num_sim=NUM_SIMULACOES):
    logger = get_run_logger()
    logger.info("Iniciando Pipeline de Simulação do Brasileirão.")
    jogos, times = fetch_cbf_data(ano).result()
    simulation_success = run_simulation_and_save_data(jogos, times, num_sim, ano)
    if not simulation_success:
        logger.critical("Simulação falhou. Abortando Flow.")
        return 
    plot_success = generate_and_save_plots(ano)
    if plot_success:
        commit_success = commit_and_push_data()
        if commit_success:
            logger.info("Pipeline concluído e dados versionados no GitHub.")
    else:
        logger.warning("Gráficos não foram gerados, mas o Flow está completo.") 
    return "Pipeline Concluído"


if __name__ == "__main__":
    brasileirao_update_flow()
