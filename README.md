# Brasileirão - Monte Carlo Simulation

Simulação estatística do Campeonato Brasileiro de Futebol (Série A) usando **Monte Carlo** com distribuição de Poisson para prever resultados de jogos futuros e probabilidades de classificação.

## Metodologia

### Coleta de Dados
- Dados obtidos em tempo real via API da **CBF** (Confederação Brasileira de Futebol)
- 38 rodadas, 20 times, 380 jogos por temporada
- Resultados já disputados são utilizados como dados reais

### Modelo de Simulação
- **100.000 simulações** por execução
- Jogos futuros simulados com **distribuição de Poisson**:
  - Mandante: λ = 2 (vantagem de jogar em casa)
  - Visitante: λ = 1
- Sistema de pontuação: Vitória = 3pts, Empate = 1pt, Derrota = 0pts
- Critérios de desempate: vitórias → saldo de gols → ruído aleatório

### Automação
- Execução diária via **GitHub Actions** (cron: 01:00 UTC)
- Atualização automática dos dados e gráficos
- Commit e push automático dos resultados

## Resultados - Temporada 2026 (em andamento)

### Probabilidade de Posição Final (barra empilhada)
![short_final_2026](figs/short_final_2026.png)

### Evolução da Posição Esperada ao Longo do Campeonato
![long2_2026](figs/long2_2026.png)

### Pontuação Esperada Acumulada
![long2_stacked_2026](figs/long2_stacked_2026.png)

### Distribuição de Probabilidades por Posição
![long3_2026](figs/long3_2026.png)

---

## Temporadas Anteriores

### 2025
![short_final_2025](figs/short_final_2025.png)
![long2_2025](figs/long2_2025.png)
![long2_stacked_2025](figs/long2_stacked_2025.png)
![long3_2025](figs/long3_2025.png)

### 2024
![short_final_2024](figs/short_final_2024.png)
![long2_2024](figs/long2_2024.png)
![long2_stacked_2024](figs/long2_stacked_2024.png)
![long3_2024](figs/long3_2024.png)

### 2023
![short_final_2023](figs/short_final_2023.png)
![long2_2023](figs/long2_2023.png)
![long2_stacked_2023](figs/long2_stacked_2023.png)
![long3_2023](figs/long3_2023.png)

### 2022
![short_final_2022](figs/short_final_2022.png)
![long2_2022](figs/long2_2022.png)
![long2_stacked_2022](figs/long2_stacked_2022.png)
![long3_2022](figs/long3_2022.png)

### 2021
![short_final_2021](figs/short_final_2021.png)
![long2_2021](figs/long2_2021.png)
![long2_stacked_2021](figs/long2_stacked_2021.png)
![long3_2021](figs/long3_2021.png)

### 2020
![short_final_2020](figs/short_final_2020.png)
![long2_2020](figs/long2_2020.png)
![long2_stacked_2020](figs/long2_stacked_2020.png)
![long3_2020](figs/long3_2020.png)

### 2019
![short_final_2019](figs/short_final_2019.png)
![long2_2019](figs/long2_2019.png)
![long2_stacked_2019](figs/long2_stacked_2019.png)
![long3_2019](figs/long3_2019.png)

### 2018
![short_final_2018](figs/short_final_2018.png)
![long2_2018](figs/long2_2018.png)
![long2_stacked_2018](figs/long2_stacked_2018.png)
![long3_2018](figs/long3_2018.png)

### 2017
![short_final_2017](figs/short_final_2017.png)
![long2_2017](figs/long2_2017.png)
![long2_stacked_2017](figs/long2_stacked_2017.png)
![long3_2017](figs/long3_2017.png)

---

## Estrutura do Projeto

```
brasileirao/
├── brasileirao_2026.py          # Script principal (temporada atual)
├── brasileirao_2026.txt         # Resultados (formato curto)
├── brasileirao_long_2026.txt    # Resultados (formato longo)
├── figs/                        # Gráficos gerados (4 por temporada)
├── _old/                        # Scripts históricos e dados (2017-2025)
├── .github/workflows/           # Automação via GitHub Actions
├── requirements.txt             # Dependências Python
├── Dockerfile                   # Container para execução local
└── README.md
```

## Tecnologias

- **Python 3.11** - Linguagem principal
- **NumPy** - Distribuição de Poisson e operações numéricas
- **Pandas** - Manipulação e análise de dados
- **Matplotlib** - Visualizações
- **SciPy** - Suavização de curvas (Gaussian filter)
- **Requests** - Consumo da API da CBF
- **GitHub Actions** - Automação e agendamento

## Como Executar

### Localmente
```bash
pip install -r requirements.txt
python brasileirao_2026.py
```

### Via Docker
```bash
docker build -t brasileirao .
docker run brasileirao
```

## Licença

Projeto para fins educacionais e de análise estatística.
