import urllib.request
from bs4 import BeautifulSoup
from pprint import pprint
import numpy as np
import time
from operator import itemgetter
from collections import Counter
from datetime import datetime
import os



def limpa():
    os.system("cls" if os.name == "nt" else "clear")

print('iniciando', flush=True)
url = 'http://www.tabeladobrasileirao.net/'
dados = []
times = {}
resultado = {}
content = urllib.request.urlopen(url).read()
soup = BeautifulSoup(content.decode('utf-8','ignore'),'html.parser')
print(soup, flush=True)

table = soup.find("table", attrs = {'class' : 'table'})
for i in table.findAll("td"):
    jogo = []
    for j in i.findAll("div"):
        jogo.append(j.text.strip())
    jogo.pop(2)
    jogo.pop(3)
    jogo.pop(4)
    times[jogo[2]]=0
    resultado[jogo[2]]={'nome':jogo[2],'pos':0,'campeao':0,'libertadores':0,'rebaixado':0}
    data = jogo[0].split(" - ")
    jogo[0] = str(data[1] + ' ' + data[2])     #time.strptime(str(data[1] + ' ' + data[2]), '%d/%m/%Y %H:%M')
    dados.append(jogo)
print('pegou dados', flush=True)

def pontua(data):
    timesx={}
    for i in times:
        timesx[i]={'nome':i,'classificacao':0,'pontuacao':0,'pontos':0,'gpm':0,'gpv':0,'gcm':0,'gcv':0,'j_vitorias':0,'j_empates':0,'j_derrotas':0}

    for i in data:
        man=i[2]
        vis=i[5]

        if i[3]=='':
            placar_man=int(np.random.poisson(2))
            placar_vis=int(np.random.poisson(1))
        else:
            placar_man=int(i[3])
            placar_vis=int(i[4])
        if placar_man > placar_vis:
            timesx[man]['j_vitorias']=timesx[man]['j_vitorias']+1
            timesx[vis]['j_derrotas']=timesx[vis]['j_derrotas']+1
            timesx[man]['pontos']=timesx[man]['pontos']+3
            timesx[man]['pontuacao']=timesx[man]['pontuacao']+301000
        elif placar_man < placar_vis:
            timesx[vis]['j_vitorias']=timesx[vis]['j_vitorias']+1
            timesx[man]['j_derrotas']=timesx[man]['j_derrotas']+1
            timesx[vis]['pontos']=timesx[vis]['pontos']+3
            timesx[vis]['pontuacao']=timesx[vis]['pontuacao']+301000
        else:
            timesx[man]['j_empates']=timesx[man]['j_empates']+1
            timesx[vis]['j_empates']=timesx[vis]['j_empates']+1
            timesx[vis]['pontos']=timesx[vis]['pontos']+1
            timesx[man]['pontos']=timesx[man]['pontos']+1
            timesx[vis]['pontuacao']=timesx[vis]['pontuacao']+100100
            timesx[man]['pontuacao']=timesx[man]['pontuacao']+100100
        timesx[man]['pontuacao']=timesx[man]['pontuacao']+placar_man-placar_vis
        timesx[vis]['pontuacao']=timesx[vis]['pontuacao']+placar_vis-placar_man
        timesx[man]['gpm']=timesx[man]['gpm']+placar_man
        timesx[vis]['gpv']=timesx[vis]['gpv']+placar_vis
        timesx[man]['gcm']=timesx[man]['gcm']+placar_vis
        timesx[vis]['gcv']=timesx[vis]['gcv']+placar_man
    arr=[]
    for i in times:
        arr.append((timesx[i]['pontuacao'],timesx[i]['nome']))
    arr.sort()
    tot = len(times)
    for i in arr:
        timesx[i[1]]['classificacao']=tot
        tot = tot - 1
    return timesx




resultadox=[]
for j in times:
    resultadox.append([j])
cont=20000
for i in range(cont):
    limpa()
    print(round(i/cont*100,int(np.log10(cont))-int(np.log10(i+1))), flush=True)
    c = pontua(dados)
    for j in times:
        v = c[j]['classificacao']
        for z in resultadox:
            if z[0]==j:
                z.append(v)
limpa()

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
print("fim")
