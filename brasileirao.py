from bs4 import BeautifulSoup
import requests
import re
from pprint import pprint
import numpy as np
from datetime import datetime
import copy
import os



def limpa(text):
    text = re.sub("\s\s+" , " ", text)
    return text.strip()

def pega_jogos():
    r = requests.get('https://www.cbf.com.br/futebol-brasileiro/competicoes/campeonato-brasileiro-serie-a/2019')
    soup = BeautifulSoup(r.content, 'html.parser')
    table = soup.find(attrs={'class':'swiper-wrapper'})
    content = table.find_all('li')
    jogos = []
    for c in content:
        line = {}
        info = [limpa(x.text) for x in c.find_all('span')]
        a = len(info)
        if a == 4:
            local = info[-1].split('\n')[0].split(' - ',1)
            if len(local)>1:
                estadio,cidade = local[0],local[1]
            else:
                estadio,cidade = '',''
            line = {'data': info[0].split(' - ')[0], 'mandante':info[1],'visitante': info[2],
            'placar_mandante':'', 'placar_visitante':'','estadio': estadio, 'cidade':cidade}
        elif a == 5:
            local = info[-1].split('\n')[0].split(' - ',1)
            if len(local)>1:
                estadio,cidade = local[0],local[1]
            else:
                estadio,cidade = '',''
            if len(info[3]) < 6:
                try:
                    mandante,visitante = info[1],info[2]
                    placar_m = int(info[3].split(' x ')[0])
                    placar_v = int(info[3].split(' x ')[1])
                except:
                    mandante,visitante = info[2],info[3]
                    placar_m,placar_v = '',''
            else:
                mandante,visitante = info[1],info[2]
                placar_m,placar_v = '',''
            line = {'data': info[0].split(' - ')[0], 'mandante':mandante,'visitante': visitante,
                'placar_mandante':placar_m, 'placar_visitante':placar_v,'estadio': estadio, 'cidade':cidade}
        elif a == 6:
            local = info[-1].split('\n')[0].split(' - ',1)
            estadio,cidade = local[0],local[1]
            placar_m = int(info[4].split(' x ')[0])
            placar_v = int(info[4].split(' x ')[1])
            line = {'data': info[0].split(' - ')[0], 'mandante':info[2],'visitante': info[3],
            'placar_mandante':placar_m, 'placar_visitante':placar_v,'estadio': estadio, 'cidade':cidade}
        jogos.append(line)
    return jogos

def get_times(jogos):
    times=[]
    for j in jogos:
        if j['mandante'] not in times:
            times.append(j['mandante'])
        if j['visitante'] not in times:
            times.append(j['visitante'])
    times.sort()
    return times



def pontua(jog):
    timesx = dict()
    for t in times:
        timesx[t] = {'nome':t,'classificacao':0,'pontuacao':0,'pontos':0,'gpm':0,'gpv':0,'gcm':0,'gcv':0,'j_vitorias':0,'j_empates':0,'j_derrotas':0}
    for g in jog:
        if (g['placar_visitante'] =='') or (g['placar_mandante'] == ''):
            g['placar_mandante'] = int(np.random.poisson(2))
            g['placar_visitante'] = int(np.random.poisson(1))
        if g['placar_mandante'] > g['placar_visitante']:
            timesx[g['mandante']]['pontos'] += 3
            timesx[g['mandante']]['gpm'] += g['placar_mandante']
            timesx[g['mandante']]['gcm'] += g['placar_visitante']
            timesx[g['mandante']]['j_vitorias'] += 1
            timesx[g['visitante']]['j_derrotas'] += 1
            timesx[g['visitante']]['gpv'] += g['placar_visitante']
            timesx[g['visitante']]['gcv'] += g['placar_mandante']
        elif g['placar_mandante'] < g['placar_visitante']:
            timesx[g['visitante']]['pontos'] += 3
            timesx[g['mandante']]['gpm'] += g['placar_mandante']
            timesx[g['mandante']]['gcm'] += g['placar_visitante']
            timesx[g['visitante']]['j_vitorias'] += 1
            timesx[g['mandante']]['j_derrotas'] += 1
            timesx[g['visitante']]['gpv'] += g['placar_visitante']
            timesx[g['visitante']]['gcv'] += g['placar_mandante']
        elif g['placar_mandante'] == g['placar_visitante']:
            timesx[g['mandante']]['pontos'] += 1
            timesx[g['visitante']]['pontos'] += 1
            timesx[g['mandante']]['gpm'] += g['placar_mandante']
            timesx[g['mandante']]['gcm'] += g['placar_visitante']
            timesx[g['visitante']]['j_empates'] += 1
            timesx[g['mandante']]['j_empates'] += 1
            timesx[g['visitante']]['gpv'] += g['placar_visitante']
            timesx[g['visitante']]['gcv'] += g['placar_mandante']

    for i in times:
        timesx[i]['pontuacao'] = ((timesx[i]['pontos']*100 + timesx[i]['j_vitorias'])*1000 +
        timesx[i]['gpm'] + timesx[i]['gpv'] - timesx[i]['gcm'] - timesx[i]['gcv'])+500
    arr=[]
    for i in times:
        arr.append((timesx[i]['pontuacao'],timesx[i]['nome']))
    arr.sort()
    tot = len(times)
    for i in arr:
        timesx[i[1]]['classificacao']=tot
        tot = tot - 1
    return timesx


def clear_screen():
    os.system("cls" if os.name == "nt" else "clear")


jogos = pega_jogos()
times = get_times(jogos)

resultadox=[]
for j in times:
    resultadox.append([j])
cont = 20000
for i in range(cont):
    j1 = copy.deepcopy(jogos)
    c = pontua(j1)
    clear_screen()
    for j in times:
        v = c[j]['classificacao']
        for z in resultadox:
            if z[0]==j:
                z.append(v)
    print(i,'\n')
    for f in resultadox:
        print(f[0],round(np.mean(f[1:]),2))



with open("brasileirao_2019.txt", "a", encoding="utf-8") as file:
    for j in range(0,20):
        text=datetime.now().strftime('%Y-%m-%d')+";"+resultadox[j][0]+';'
        print(text,end='')
        for i in range(1,21):
            text=text + str(resultadox[j].count(i)/cont) + ';'
            print(resultadox[j].count(i)/cont,end=';', flush=True)
        print('')
        file.write(str(text) + '\n')



with open("brasileirao_long_2019.txt", "a", encoding="utf-8") as file2:
    for j in range(0,20):
        for i in range(1,21):
            text2=str(datetime.now().strftime('%Y-%m-%d') + ';' + resultadox[j][0] + ';' + str(i) + ';' + str(resultadox[j].count(i)/cont*100))
            print(text2, flush=True)
            file2.write(str(text2).replace(".",",") + '\n')


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt



short = pd.read_csv('brasileirao_2019.txt',sep=';', index_col=False)
short.data = pd.to_datetime(short.data)
last_short = short[short['data'] == max(short['data'])]
short_final = last_short[last_short.columns[1:]].set_index('time')
short_final.sort_values(by=[str(i) for i in list(range(20,0,-1))], inplace=True)
short_final.plot(kind='bar',stacked=True,figsize=(20,15), colormap='autumn')
plt.savefig('short_final.png')



long = pd.read_csv('brasileirao_long_2019.txt',sep=';', index_col=False, decimal=',')
long.data = pd.to_datetime(long.data)
long['points'] = (20-long['pos']) * long['chance']
long2 = long.groupby(['data','time'])['points'].mean().unstack()
long2.sort_values(long2.columns.max()).plot(kind='area',stacked=True,figsize=(15,20), colormap='brg')
plt.savefig('long2_stacked.png')


long2 = long.groupby(['time','data'])['points'].mean().unstack()
long2.sort_values(long2.columns.max()).plot(kind='barh',stacked=False,figsize=(15,20), colormap='autumn')
plt.savefig('long2.png')
