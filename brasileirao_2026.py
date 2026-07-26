"""
Brasileirão Série A – Simulação Monte Carlo
Busca resultados da API da CBF e simula jogos futuros usando distribuição de Poisson.
Gera gráficos de probabilidades de classificação por posição.
"""
import requests
import numpy as np
import copy
from collections import Counter
from datetime import date
from pathlib import Path
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter1d
from time import sleep
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

print('iniciando script')

NUM_SIM = 100_000
ANO = 2026
FIGS_DIR = Path('figs')
FIGS_DIR.mkdir(exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:152.0) Gecko/20100101 Firefox/152.0',
    'Accept': 'application/json',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
    'Referer': 'https://www.cbf.com.br/futebol-brasileiro/tabelas/campeonato-brasileiro/serie-a/2026',
    'Content-Type': 'application/json',
    'Connection': 'keep-alive',
    'Cookie': 'cookiesession1=678A3E0E2F39CA38863DD30D14D335D9; cookies_accepted_categories=technically_required%2Cpreferences%2Canalytics%2Cmarketing',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'Priority': 'u=0',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}

API_URL = 'https://www.cbf.com.br/api/cbf/jogos/campeonato/1260611/rodada/{rodada}/fase'


def pega(rodada):
    for i in range(10):
        try:
            response = requests.get(API_URL.format(rodada=rodada), headers=HEADERS, verify=False)
            if response.status_code == 200:
                return response.json()['jogos']
            print(f'Erro HTTP {response.status_code} na rodada {rodada}, tentativa {i+1}')
        except Exception as e:
            print(f'Erro ao pegar rodada {rodada}, tentativa {i+1}: {e}')
        sleep(2)
    return []


def limpa_nome(nome):
    if not nome:
        return None
    for sufixo in (' Saf', ' S.a.f.', ' Fc', ' SAF', ' FC'):
        nome = nome.replace(sufixo, '')
    return nome


def pega_jogos():
    js = []
    times = set()
    for rodada in range(1, 39):
        print(f'rodada {rodada}')
        jogos = pega(rodada)
        if not jogos:
            print(f'Warning: dados não encontrados na rodada {rodada}')
            continue
        for jogo in jogos[0].get('jogo', []):
            mandante = limpa_nome(jogo['mandante']['nome'])
            visitante = limpa_nome(jogo['visitante']['nome'])
            js.append({
                'mandante': mandante,
                'visitante': visitante,
                'placar_mandante': jogo['mandante']['gols'],
                'placar_visitante': jogo['visitante']['gols'],
            })
            if rodada == 1:
                times.add(mandante)
                times.add(visitante)
    return js, sorted(times)


def gera_dados(times):
    return {t: {'nome': t, 'pontos': 0, 'saldo_gols': 0, 'vitorias': 0} for t in times}


def pontua(line, dados_time):
    if line['placar_mandante'] is None:
        placar_m = np.random.poisson(2)
        placar_v = np.random.poisson(1)
    else:
        placar_m = int(line['placar_mandante'])
        placar_v = int(line['placar_visitante'])

    mandante = line['mandante']
    visitante = line['visitante']

    if placar_m > placar_v:
        dados_time[mandante]['pontos'] += 3
        dados_time[mandante]['vitorias'] += 1
    elif placar_m < placar_v:
        dados_time[visitante]['pontos'] += 3
        dados_time[visitante]['vitorias'] += 1
    else:
        dados_time[mandante]['pontos'] += 1
        dados_time[visitante]['pontos'] += 1

    saldo = placar_m - placar_v
    dados_time[mandante]['saldo_gols'] += saldo
    dados_time[visitante]['saldo_gols'] -= saldo
    return dados_time


def calcula_colocacao(dados_time, times):
    arr = []
    for t in times:
        d = dados_time[t]
        pontuacao = (d['pontos'] * 1_000_000 +
                     d['vitorias'] * 1_000 +
                     d['saldo_gols'] + 200 +
                     np.random.random() / 1000)
        arr.append((pontuacao, d['nome']))
    arr.sort()
    return {r[1]: len(arr) - i for i, r in enumerate(arr)}


def salva_resultados(resultados, times, hoje):
    n = len(times)
    with open(f'brasileirao_long_{ANO}.txt', 'a', encoding='utf-8') as f_long:
        with open(f'brasileirao_{ANO}.txt', 'a', encoding='utf-8') as f_short:
            for t in times:
                contagem = Counter(resultados[t])
                probs = []
                for pos in range(1, n + 1):
                    prob = round(contagem.get(pos, 0) / NUM_SIM, 5)
                    probs.append(str(prob))
                    f_long.write(f'{hoje};{t};{pos};{prob}\n')
                f_short.write(';'.join([hoje, t] + probs) + '\n')


def gera_graficos(hoje):
    print('gerando gráficos...')

    short = pd.read_csv(f'brasileirao_{ANO}.txt', sep=';', index_col=False, decimal='.')
    short.data = pd.to_datetime(short.data)
    last_short = short[short['data'] == max(short.data)]
    short_final = last_short[last_short.columns[1:]].set_index('time')
    short_final.sort_values(by=[str(i) for i in range(20, 0, -1)], inplace=True)
    fig, ax = plt.subplots(figsize=(20, 15))
    short_final.plot(kind='bar', stacked=True, ax=ax, colormap='cool')
    ax.set_ylabel('Probabilidade')
    ax.set_title(f'Probabilidade de Posição Final – Brasileirão {ANO}')
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'short_final_{ANO}.png')
    plt.close(fig)

    long = pd.read_csv(f'brasileirao_long_{ANO}.txt', sep=';', index_col=False, decimal='.')
    long = long.groupby(['data', 'time', 'pos'])['chance'].mean().reset_index()
    long.data = pd.to_datetime(long.data)
    long['points'] = (20 - long['pos']) * long['chance']

    long2_stacked = long.groupby(['data', 'time'])['points'].mean().unstack()
    long2_stacked.sort_values(long2_stacked.index[-1], axis=1, inplace=True)
    fig, ax = plt.subplots(figsize=(15, 20))
    long2_stacked.plot(kind='area', stacked=True, ax=ax, colormap='brg')
    ax.set_ylabel('Pontuação Esperada')
    ax.set_title(f'Pontuação Esperada Acumulada – Brasileirão {ANO}')
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'long2_stacked_{ANO}.png')
    plt.close(fig)

    long2_bar = long.groupby(['time', 'data'])['points'].mean().unstack()
    long2_bar.sort_values(long2_bar.columns.max(), inplace=True)
    fig, ax = plt.subplots(figsize=(15, 20))
    long2_bar.plot(kind='barh', stacked=False, ax=ax, colormap='cool')
    ax.set_xlabel('Pontuação Esperada')
    ax.set_title(f'Evolução da Posição Esperada – Brasileirão {ANO}')
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'long2_{ANO}.png')
    plt.close(fig)

    long3 = long[long['data'] == max(long.data)]
    fig, ax = plt.subplots(figsize=(15, 10))
    for t in long3['time'].unique():
        sub = long3[long3['time'] == t]
        y_smooth = gaussian_filter1d(sub.chance, sigma=3)
        ax.plot(sub.pos, y_smooth, label=t)
    ax.legend(loc='upper right', ncol=5, fontsize='medium')
    ax.set_xticks(range(1, 21))
    ax.set_xlim(1, 20)
    ax.set_ylim(bottom=0)
    ax.set_ylabel('Probabilidade (%)')
    ax.set_xlabel('Posição')
    ax.set_title(f'Probabilidade por Posição – Brasileirão {ANO}')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'long3_{ANO}.png')
    plt.close(fig)

    print('gráficos gerados com sucesso')


# ==================== MAIN ====================

jogos, times = pega_jogos()
dados_time_orig = gera_dados(times)
print(f'{len(times)} times, {len(jogos)} jogos encontrados')

jogos_futuros = [j for j in jogos if j['placar_mandante'] is None]
jogos_ja_disputados = [j for j in jogos if j['placar_mandante'] is not None]
print(f'{len(jogos_ja_disputados)} jogos já disputados, {len(jogos_futuros)} jogos a simular')

resultados = {t: [] for t in times}
for sim in range(NUM_SIM):
    dados_time = copy.deepcopy(dados_time_orig)
    for jogo in jogos:
        dados_time = pontua(jogo, dados_time)
    colocacao = calcula_colocacao(dados_time, times)
    for t in times:
        resultados[t].append(colocacao[t])
    if (sim + 1) % 10_000 == 0:
        print(f'  simulação {sim+1}/{NUM_SIM}')

hoje = str(date.today())
salva_resultados(resultados, times, hoje)
gera_graficos(hoje)

print('finalizando script')
