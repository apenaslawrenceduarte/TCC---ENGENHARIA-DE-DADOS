# TCC – Engenharia de Dados em Saúde
## Pipeline para Análise Sociodemográfica dos Usuários de PrEP no Rio de Janeiro (2018–2024)

**Instituição:** PUC Minas
**Fonte dos dados:** Painel de PrEP – DATASUS / Ministério da Saúde

---

## Sobre o Projeto

Pipeline de dados completo para análise do perfil sociodemográfico e tendências de adesão à PrEP no município do Rio de Janeiro entre 2018 e 2024, com dashboard interativo para visualização dos resultados.

---

## Arquivos do Repositório

| Arquivo | Descrição |
|---|---|
| `pipeline.py` | ETL: lê os CSVs, filtra RJ (IBGE 3304557), limpa e salva em `prep_rj.db` |
| `dashboard.py` | Dashboard Streamlit com gráficos e mapa interativo |
| `prep_rj.db` | Banco SQLite já processado — pronto para rodar o dashboard |
| `Banco_PrEP_usuarios.csv` | ~236k usuários nacionais (fonte: DATASUS) |
| `requirements.txt` | Dependências Python |
| `Dicionário de Dados.pdf` | Descrição de todas as variáveis |

---

## Download do Arquivo de Dispensações

O arquivo `Banco_PrEP_dispensas.csv` (~231MB) é grande demais para o GitHub.
Baixe diretamente pelo link oficial do DATASUS:

**https://mediacenter.aids.gov.br/prep/Dados_PrEP_transparencia.zip**

> Caso o link não abra direto, copie e cole em uma nova aba do navegador.

Após baixar e extrair o `.zip`, coloque o `Banco_PrEP_dispensas.csv` na mesma pasta do projeto.

---

## Como Executar

### 1. Instalar dependências
```bash
pip install -r requirements.txt
```

### 2. Opção A — Rodar direto o dashboard (banco já incluído)
```bash
streamlit run dashboard.py
```

### 3. Opção B — Reprocessar os dados do zero
> Necessário ter o `Banco_PrEP_dispensas.csv` baixado (ver seção acima)
```bash
python pipeline.py
streamlit run dashboard.py
```

---

## Dados do Rio de Janeiro (após filtragem)

- **17.692 usuários** residentes no município do Rio de Janeiro
- **90.538 dispensações** desses usuários
- Período: 2018–2024
- Filtro: código IBGE 3304557

---

## Dashboard

O dashboard possui 5 abas:

- **Visão Geral** — KPIs, distribuição por gênero e raça, novos usuários por ano
- **Perfil Sociodemográfico** — faixa etária, escolaridade e cruzamentos
- **Adesão à PrEP** — evolução temporal, taxa de descontinuação
- **Mapa por Zona** — distribuição geográfica por Zona Central, Sul, Norte e Sudoeste
- **Tabelas** — tabelas resumo e cruzadas exportáveis
