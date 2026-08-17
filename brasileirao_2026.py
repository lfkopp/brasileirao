"""
Brasileirão Série A – Simulação Monte Carlo
Busca resultados da API da CBF e simula jogos futuros usando distribuição de Poisson.
Gera gráficos de probabilidades de classificação por posição.
"""
import requests
import numpy as np

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
    idx = {t: i for i, t in enumerate(times)}
    return {
        'times': times,
        'idx': idx,
        'pontos': np.zeros(len(times), dtype=np.int32),
        'vitorias': np.zeros(len(times), dtype=np.int32),
        'saldo': np.zeros(len(times), dtype=np.int32),
    }


def processa_jogos(jogos, dados):
    idx = dados['idx']
    for j in jogos:
        m = idx[j['mandante']]
        v = idx[j['visitante']]
        gm = int(j['placar_mandante'])
        gv = int(j['placar_visitante'])
        if gm > gv:
            dados['pontos'][m] += 3
            dados['vitorias'][m] += 1
        elif gm < gv:
            dados['pontos'][v] += 3
            dados['vitorias'][v] += 1
        else:
            dados['pontos'][m] += 1
            dados['pontos'][v] += 1
        dados['saldo'][m] += gm - gv
        dados['saldo'][v] -= gm - gv


def gera_placares(jogos, n_sim):
    n_jogos = len(jogos)
    if n_jogos == 0:
        return np.zeros((0, 2), dtype=np.int32)
    gols_m = np.random.poisson(2, size=(n_sim, n_jogos))
    gols_v = np.random.poisson(1, size=(n_sim, n_jogos))
    return np.stack([gols_m, gols_v], axis=-1)


def calcula_colocacao_vetorizado(dados_base, placares, n_sim):
    n_times = len(dados_base['times'])
    n_jogos = placares.shape[1]

    pontos = np.tile(dados_base['pontos'], (n_sim, 1))
    vitorias = np.tile(dados_base['vitorias'], (n_sim, 1))
    saldo = np.tile(dados_base['saldo'], (n_sim, 1))

    idx = dados_base['idx']
    jogos = dados_base['jogos_futuros']

    for k in range(n_jogos):
        gm = placares[:, k, 0]
        gv = placares[:, k, 1]
        m = idx[jogos[k]['mandante']]
        v = idx[jogos[k]['visitante']]

        mandante_vence = gm > gv
        visitante_vence = gm < gv
        empate = gm == gv

        pontos[:, m] += np.where(mandante_vence, 3, np.where(empate, 1, 0))
        pontos[:, v] += np.where(visitante_vence, 3, np.where(empate, 1, 0))
        vitorias[:, m] += mandante_vence.astype(np.int32)
        vitorias[:, v] += visitante_vence.astype(np.int32)
        saldo[:, m] += gm - gv
        saldo[:, v] += gv - gm

    aleatorio = np.random.random((n_sim, n_times))
    chaves = pontos * 1_000_000 + vitorias * 1_000 + saldo + 200 + aleatorio / 1000
    ordem = np.argsort(chaves, axis=1)
    posicoes = np.empty_like(ordem)
    posicoes[np.arange(n_sim)[:, None], ordem] = np.arange(n_times)[None, ::-1] + 1

    return posicoes


def salva_resultados(resultados, times, hoje):
    n = len(times)
    with open(f'brasileirao_long_{ANO}.txt', 'a', encoding='utf-8') as f_long:
        with open(f'brasileirao_{ANO}.txt', 'a', encoding='utf-8') as f_short:
            for t in times:
                contagem = Counter(resultados[t])
                probs = []
                for pos in range(1, n + 1):
                    prob = contagem.get(pos, 0) / NUM_SIM
                    probs.append(f'{prob:.5f}')
                    f_long.write(f'{hoje};{t};{pos};{prob:.5f}\n')
                f_short.write(';'.join([hoje, t] + probs) + '\n')


def gini(values):
    arr = np.sort(values)
    n = len(arr)
    index = np.arange(1, n + 1)
    return (2 * np.sum(index * arr) - (n + 1) * np.sum(arr)) / (n * np.sum(arr))


def gera_graficos(hoje):
    print('gerando graficos...')

    short = pd.read_csv(f'brasileirao_{ANO}.txt', sep=';', index_col=False, decimal='.')
    short.data = pd.to_datetime(short.data)
    last_short = short[short['data'] == max(short.data)]

    long = pd.read_csv(f'brasileirao_long_{ANO}.txt', sep=';', index_col=False, decimal='.')
    long = long.groupby(['data', 'time', 'pos'])['chance'].mean().reset_index()
    long.data = pd.to_datetime(long.data)
    long['points'] = (20 - long['pos']) * long['chance']
    pivot_pts = long.groupby(['data', 'time'])['points'].mean().unstack()

    cmap_time = plt.colormaps.get_cmap('tab20').resampled(20)

    # --- 1. Heatmap de probabilidades (posicao final) ---
    last_long = long[long['data'] == max(long.data)]
    prob_matrix = last_long.pivot(index='time', columns='pos', values='chance')
    prob_matrix = prob_matrix.reindex(columns=range(1, 21))
    expected_pos = (prob_matrix * np.arange(1, 21)).sum(axis=1)
    prob_matrix['_exp'] = expected_pos
    prob_matrix.sort_values('_exp', inplace=True)
    prob_matrix.drop(columns='_exp', inplace=True)

    fig, ax = plt.subplots(figsize=(14, 12))
    im = ax.imshow(prob_matrix.values, aspect='auto', cmap='YlOrRd', vmin=0, vmax=0.25)
    ax.set_xticks(range(20))
    ax.set_xticklabels(range(1, 21))
    ax.set_yticks(range(20))
    ax.set_yticklabels(prob_matrix.index)
    ax.set_xlabel('Posicao')
    ax.set_title(f'Heatmap de Probabilidades - Posicao Final - Brasileirao {ANO}')
    plt.colorbar(im, ax=ax, label='Probabilidade')
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'heatmap_final_{ANO}.png', dpi=150)
    plt.close(fig)

    # --- 2. Corrida de pontos esperados (top 8 + destaque) ---
    top8 = pivot_pts.iloc[-1].nlargest(8).index.tolist()
    fig, ax = plt.subplots(figsize=(16, 9))
    for i, t in enumerate(top8):
        lw = 2.5 if i < 2 else 1.5
        ax.plot(pivot_pts.index, pivot_pts[t], label=t, linewidth=lw, color=cmap_time(i))
    ax.legend(loc='upper left', fontsize=10, ncol=2)
    ax.set_ylabel('Pontos Esperados (acumulados)')
    ax.set_xlabel('Data')
    ax.set_title(f'Evolucao dos Pontos Esperados (Top 8) - Brasileirao {ANO}')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'pontos_top8_{ANO}.png', dpi=150)
    plt.close(fig)

    # --- 3. Gini - competitividade ao longo do campeonato ---
    gini_vals = pivot_pts.apply(lambda row: gini(row.values), axis=1)

    fig, ax1 = plt.subplots(figsize=(14, 7))
    ax1.plot(gini_vals.index, gini_vals.values, color='#e74c3c', linewidth=2.5, label='Indice de Gini')
    ax1.fill_between(gini_vals.index, gini_vals.values, alpha=0.15, color='#e74c3c')
    ax1.set_ylabel('Indice de Gini (0 = igualdade, 1 = concentracao)', color='#e74c3c')
    ax1.tick_params(axis='y', labelcolor='#e74c3c')
    ax1.set_xlabel('Data')
    ax1.set_title(f'Competitividade ao Longo do Campeonato (Gini) - Brasileirao {ANO}')
    ax1.grid(True, alpha=0.3)

    top2_gap = pivot_pts[top8[0]] - pivot_pts[top8[1]]
    ax2 = ax1.twinx()
    ax2.plot(top2_gap.index, top2_gap.values, color='#3498db', linewidth=1.5,
             linestyle='--', alpha=0.7, label=f'Gap {top8[0]} x {top8[1]}')
    ax2.set_ylabel('Gap de pontos (1o - 2o)', color='#3498db')
    ax2.tick_params(axis='y', labelcolor='#3498db')

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right', fontsize=9)
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'gini_{ANO}.png', dpi=150)
    plt.close(fig)

    # --- 4. Distribuicao de posicao final (curvas suavizadas) ---
    last_long2 = long[long['data'] == max(long.data)]
    expected_pos_dict = last_long2.groupby('time').apply(
        lambda g: (g['pos'] * g['chance']).sum(), include_groups=False).sort_values()

    fig, axes = plt.subplots(4, 5, figsize=(22, 16), sharex=True, sharey=True)
    axes = axes.flatten()
    for i, t in enumerate(expected_pos_dict.index):
        sub = last_long2[last_long2['time'] == t]
        y_smooth = gaussian_filter1d(sub['chance'].values, sigma=2)
        axes[i].fill_between(sub['pos'], y_smooth, alpha=0.4, color=cmap_time(i))
        axes[i].plot(sub['pos'], y_smooth, color=cmap_time(i), linewidth=1.5)
        axes[i].set_title(t, fontsize=8, fontweight='bold')
        axes[i].set_xticks([1, 5, 10, 15, 20])
        axes[i].grid(True, alpha=0.2)
        ymax = max(y_smooth) * 1.1
        axes[i].set_ylim(0, ymax)
    fig.suptitle(f'Distribuicao de Probabilidade por Time - Brasileirao {ANO}', fontsize=16, y=1.01)
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'dist_por_time_{ANO}.png', dpi=150, bbox_inches='tight')
    plt.close(fig)

    # --- 5. Posicao esperada ao longo do tempo (line chart) ---
    pivot_pos = long.groupby(['data', 'time']).apply(
        lambda g: (g['pos'] * g['chance']).sum(), include_groups=False).unstack()

    fig, ax = plt.subplots(figsize=(16, 10))
    for i, t in enumerate(pivot_pos.columns):
        ax.plot(pivot_pos.index, pivot_pos[t], label=t, linewidth=1.2, color=cmap_time(i))
    ax.set_ylabel('Posicao Esperada (1 = campeao)')
    ax.invert_yaxis()
    ax.set_ylim(20.5, 0.5)
    ax.legend(loc='lower right', fontsize=8, ncol=3, bbox_to_anchor=(1.15, 0))
    ax.set_title(f'Evolucao da Posicao Esperada - Brasileirao {ANO}')
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'posicao_esperada_{ANO}.png', dpi=150, bbox_inches='tight')
    plt.close(fig)

    # --- 6. Barra empilhada de probabilidade de posicao final (melhorado) ---
    prob_final = last_long.pivot(index='time', columns='pos', values='chance')
    prob_final = prob_final.reindex(columns=range(1, 21))
    prob_final['_exp'] = (prob_final * np.arange(1, 21)).sum(axis=1)
    prob_final.sort_values('_exp', ascending=False, inplace=True)
    prob_final.drop(columns='_exp', inplace=True)

    fig, ax = plt.subplots(figsize=(14, 10))
    bottom = np.zeros(len(prob_final))
    cores = plt.cm.RdYlGn_r(np.linspace(0.1, 0.9, 20))
    for j, pos in enumerate(range(1, 21)):
        vals = prob_final[pos].values
        ax.barh(prob_final.index, vals, left=bottom, color=cores[j], height=0.7)
        bottom += vals
    ax.set_xlabel('Probabilidade')
    ax.set_title(f'Probabilidade de Posicao Final - Brasileirao {ANO}')
    ax.invert_yaxis()

    from matplotlib.patches import Patch
    legenda = [Patch(facecolor=cores[j], label=f'{j+1}o') for j in range(20)]
    ax.legend(handles=legenda, loc='lower right', ncol=4, fontsize=8, title='Posicao')
    plt.tight_layout()
    plt.savefig(FIGS_DIR / f'barra_final_{ANO}.png', dpi=150)
    plt.close(fig)

    print('graficos gerados com sucesso')


# ==================== MAIN ====================

jogos, times = pega_jogos()
print(f'{len(times)} times, {len(jogos)} jogos encontrados')

jogos_futuros = [j for j in jogos if j['placar_mandante'] is None]
jogos_ja_disputados = [j for j in jogos if j['placar_mandante'] is not None]
print(f'{len(jogos_ja_disputados)} jogos já disputados, {len(jogos_futuros)} jogos a simular')

dados = gera_dados(times)
processa_jogos(jogos_ja_disputados, dados)
dados['jogos_futuros'] = jogos_futuros

placares = gera_placares(jogos_futuros, NUM_SIM)
print(f'  gerados {NUM_SIM} placares para {len(jogos_futuros)} jogos futuros')

posicoes = calcula_colocacao_vetorizado(dados, placares, NUM_SIM)

resultados = {}
for i, t in enumerate(times):
    resultados[t] = posicoes[:, i].tolist()

hoje = str(date.today())
salva_resultados(resultados, times, hoje)
gera_graficos(hoje)

print('finalizando script')
