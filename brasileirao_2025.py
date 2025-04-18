#%%
#from bs4 import BeautifulSoup
import requests
import numpy as np
import copy
from datetime import date
from pathlib import Path
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from scipy.ndimage import gaussian_filter1d

#%% 

print('iniciando script')
num_sim = 100000
ano = 2025

# def get_line(c):
#     '''
#     Extrair conteúdo do HTML da CBF e retornar em um dicionário.
#     '''
#     line = {}
#     line['mandante']  = c.find(attrs={'class':'icon escudo x45 pull-right'})['title']
#     line['visitante'] = c.find(attrs={'class':'icon escudo x45 pull-left'})['title']
#     try:
#         cell = c.find('strong').find('span')
#         line['placar_mandante'], line['placar_visitante'] = [int(x) for x in cell.text.strip().split(' x ')]
#     except:
#         line['placar_mandante']  = None
#         line['placar_visitante'] = None 
#     return line

# def pega_jogos():
#     '''
#     Pegar HTML da CBF e retornar lista de dicionários.
#     '''
#     r = requests.get(f'https://www.cbf.com.br/futebol-brasileiro/competicoes/campeonato-brasileiro-serie-a/{ano}', verify=False)
#     soup = BeautifulSoup(r.content, 'html.parser')
#     table = soup.find(attrs={'class':'swiper-wrapper'})
#     content = table.find_all('li')
#     jogos = []
#     for c1 in content:
#         line = get_line(c1)
#         jogos.append(line)
#     return jogos

def pega_jogos():
    r = requests.get(f'https://jsuol.com.br/c/monaco/utils/gestor/commons.js?callback=simulador_dados_jsonp&file=commons.uol.com.br/sistemas/esporte/modalidades/futebol/campeonatos/dados/{ano}/30/dados.json', verify=False)
    j = json.loads(r.content[22:-4])
    equipes = {e:j['equipes'][e]['nome-comum'] for e in j['equipes']}

    jogos = j['fases']['4139']['jogos']['id'].values()
    jogos = [{'mandante':equipes[j['time1']],'placar_mandante':j['placar1'],
        'visitante':equipes[j['time2']],'placar_visitante':j['placar2']
        } for j in jogos]
    return jogos

def get_times(jogos):
    '''
    Retornar lista com nomes dos times
    '''
    times=[]
    for j in jogos:
        if j['mandante'] not in times:
            times.append(j['mandante'])
        if j['visitante'] not in times:
            times.append(j['visitante'])
    times.sort()
    return times

def gera_dados(times):
    '''
    Inicializa dicionário para os dados
    '''
    dados_time = {}
    for t in times:
        dados_time[t] = {   'nome':t,
                            'pontos':0,
                            'saldo_gols':0, ## gols pró mandante
                            'vitorias':0,
                            }
    return dados_time
# %%
jogos = pega_jogos()
print('jogos',jogos)
# %%
times = get_times(jogos)
print('times',times)
# %%
dados_time_orig = gera_dados(times)
print('dados_time_orig',dados_time_orig)
# %%
def pontua(line, dados_time):
    if line['placar_mandante'] == None:
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


# %%
resultados = {}
probabilidades = {}
for t in times:
    resultados[t] = []
    probabilidades[t] = []
for _ in range(num_sim):
    dados_time = copy.deepcopy(dados_time_orig)
    for line1 in jogos:
        dados_time = pontua(line1,dados_time)
    arr = []
    for i in times:
        pontuacao = (  dados_time[i]['pontos']*1000000 + 
                                        dados_time[i]['vitorias']*1000 +
                                        dados_time[i]['saldo_gols'] +200 + np.random.random()/1000)
        arr.append( (pontuacao, dados_time[i]['nome']) )
    arr.sort()
    for i,r in enumerate(arr):
        resultados[r[1]].append(len(arr)-i)


hoje = str(date.today())
# data;time;pos;chance
with open(f'brasileirao_long_{ano}.txt', 'a+', encoding='utf-8') as f:
    for t in times:
        for j in range(1,len(times)+1):
            prob = str(np.round(resultados[t].count(j)/num_sim,5))
            probabilidades[t].append(prob)
            f.write(hoje+';'+t+';'+str(j)+';'+prob+'\n')
## data;time;1;2;3;4;5;6;7;8;9;10;11;12;13;14;15;16;17;18;19;20
with open(f'brasileirao_{ano}.txt', 'a+', encoding='utf-8') as f:
    for t in times:
        f.write(';'.join([hoje,t]+probabilidades[t])+'\n')




# %%
figuras = Path('figs')

short = pd.read_csv(f'brasileirao_{ano}.txt',sep=';', index_col=False, decimal='.')
short.data = pd.to_datetime(short.data)
last_short = short[short['data'] == max(short['data'])]
short_final = last_short[last_short.columns[1:]].set_index('time')
short_final.sort_values(by=[str(i) for i in list(range(20,0,-1))], inplace=True)
short_final.plot(kind='bar',stacked=True,figsize=(20,15), colormap='cool')
plt.savefig(figuras / f'short_final_{ano}.png')

#%%

long = pd.read_csv(f'brasileirao_long_{ano}.txt',sep=';', index_col=False, decimal='.')
long = pd.DataFrame(long.groupby(['data','time','pos'])['chance'].mean()).reset_index()
long.data = pd.to_datetime(long.data)
long['points'] = (20-long['pos']) * long['chance']
long2 = long.groupby(['data','time'])['points'].mean().unstack()
long2.sort_values(long2.index[-1],axis=1).plot(kind='area',stacked=True,figsize=(15,20), colormap='brg')
plt.savefig(figuras / f'long2_stacked_{ano}.png')
#%%
long2

#%%
long2 = long.groupby(['time','data'])['points'].mean().unstack()
long2.sort_values(long2.columns.max()).plot(kind='barh',stacked=False,figsize=(15,20), colormap='cool')
plt.savefig(figuras / f'long2_{ano}.png')

#%%
long3 = long[long['data'] == max(long['data'])]
times = long3['time'].unique()

fig= plt.figure(figsize=(15,10))

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

# %%
print('finalizando script')

#%%
