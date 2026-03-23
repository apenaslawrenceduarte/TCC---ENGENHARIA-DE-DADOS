"""
Dashboard – Análise Sociodemográfica dos Usuários de PrEP no Rio de Janeiro
TCC: Engenharia de Dados em Saúde

Uso:
    streamlit run dashboard.py
"""

import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Configuração da página
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="PrEP Rio de Janeiro",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded",
)

DB_PATH = Path(__file__).parent / "prep_rj.db"

# ---------------------------------------------------------------------------
# Paleta de cores
# ---------------------------------------------------------------------------
CORES_ZONA = {
    "Zona Central":   "#E63946",
    "Zona Sul":       "#2A9D8F",
    "Zona Norte":     "#457B9D",
    "Zona Sudoeste":  "#E9C46A",
    "Não identificada": "#ADB5BD",
}

# Coordenadas aproximadas dos centros de cada zona (lat, lon)
ZONA_COORDS = {
    "Zona Central":   {"lat": -22.9028, "lon": -43.1729},
    "Zona Sul":       {"lat": -22.9630, "lon": -43.1900},
    "Zona Norte":     {"lat": -22.8480, "lon": -43.3020},
    "Zona Sudoeste":  {"lat": -22.9280, "lon": -43.4820},
    "Não identificada": {"lat": -22.9068, "lon": -43.2200},
}

FETAR_ORDER = ["<18", "18 a 24", "25 a 29", "30 a 39", "40 a 49", "50 e mais"]
ESCOL_ORDER = [
    "Sem educação formal a 3 anos",
    "De 4 a 7 anos",
    "De 8 a 11 anos",
    "12 ou mais anos",
    "Ignorada",
]

# ---------------------------------------------------------------------------
# Carregamento de dados (com cache)
# ---------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_table(query: str) -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


def load_all():
    usuarios = load_table("SELECT * FROM usuarios")
    dispensas = load_table("SELECT * FROM dispensas")
    adesao = load_table("SELECT * FROM adesao_temporal")
    novos_ano = load_table("SELECT * FROM novos_ano")
    return usuarios, dispensas, adesao, novos_ano


# ---------------------------------------------------------------------------
# Sidebar – filtros
# ---------------------------------------------------------------------------
def sidebar(usuarios: pd.DataFrame):
    st.sidebar.title("Filtros")
    st.sidebar.markdown("---")

    # Gênero
    gen_opts = sorted(usuarios["genero_simplificado"].dropna().unique().tolist())
    gen_sel = st.sidebar.multiselect("Gênero / População", gen_opts, default=gen_opts)

    # Raça/Cor
    raca_opts = sorted(usuarios["raca4_cat"].dropna().unique().tolist())
    raca_sel = st.sidebar.multiselect("Raça / Cor", raca_opts, default=raca_opts)

    # Zona
    zona_opts = sorted(usuarios["zona_rj"].dropna().unique().tolist())
    zona_sel = st.sidebar.multiselect("Zona do Rio de Janeiro", zona_opts, default=zona_opts)

    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Fonte: Painel de PrEP – DATASUS / Ministério da Saúde  \n"
        "Período: 2018 – 2024  \n"
        "Filtro: Residentes no município do Rio de Janeiro"
    )

    return gen_sel, raca_sel, zona_sel


def apply_filters(
    df: pd.DataFrame,
    gen_sel: list,
    raca_sel: list,
    zona_sel: list,
) -> pd.DataFrame:
    mask = (
        df["genero_simplificado"].isin(gen_sel)
        & df["raca4_cat"].isin(raca_sel)
        & df["zona_rj"].isin(zona_sel)
    )
    return df[mask]


# ---------------------------------------------------------------------------
# ABA 0 – Visão Geral
# ---------------------------------------------------------------------------
def tab_overview(usuarios: pd.DataFrame, dispensas: pd.DataFrame, novos_ano: pd.DataFrame):
    # KPIs
    total_u = len(usuarios)
    total_d = len(dispensas)
    em_prep_24 = usuarios["EmPrEP_2024"].str.contains("Em PrEP", na=False).sum()
    udms = usuarios["nome_udm"].nunique()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Usuários (RJ)", f"{total_u:,}")
    c2.metric("Dispensações", f"{total_d:,}")
    c3.metric("Em PrEP em 2024", f"{em_prep_24:,}")
    c4.metric("Unidades de saúde", f"{udms}")

    st.markdown("---")

    col1, col2 = st.columns(2)

    # Gênero – pizza
    with col1:
        gd = usuarios["genero_simplificado"].value_counts().reset_index()
        gd.columns = ["Gênero/População", "Total"]
        fig = px.pie(
            gd,
            values="Total",
            names="Gênero/População",
            title="Distribuição por Gênero/População",
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.35,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # Raça/Cor – barras horizontais
    with col2:
        rd = usuarios["raca4_cat"].value_counts().reset_index()
        rd.columns = ["Raça/Cor", "Total"]
        fig = px.bar(
            rd,
            x="Total",
            y="Raça/Cor",
            orientation="h",
            title="Distribuição por Raça/Cor",
            color="Total",
            color_continuous_scale="Blues",
            text="Total",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            yaxis={"categoryorder": "total ascending"},
        )
        st.plotly_chart(fig, use_container_width=True)

    # Novos usuários por ano
    if not novos_ano.empty:
        fig = px.bar(
            novos_ano.sort_values("ano"),
            x="ano",
            y="novos_usuarios",
            title="Novos Usuários por Ano (primeira dispensação)",
            labels={"ano": "Ano", "novos_usuarios": "Novos usuários"},
            color="novos_usuarios",
            color_continuous_scale="Teal",
            text="novos_usuarios",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            xaxis=dict(tickmode="linear", dtick=1),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# ABA 1 – Perfil Sociodemográfico
# ---------------------------------------------------------------------------
def tab_sociodemografico(usuarios: pd.DataFrame):
    st.subheader("Perfil Sociodemográfico dos Usuários")

    col1, col2 = st.columns(2)

    # Faixa etária
    with col1:
        fd = usuarios["fetar_clean"].value_counts().reset_index()
        fd.columns = ["Faixa Etária", "Total"]
        fd["Faixa Etária"] = pd.Categorical(
            fd["Faixa Etária"], categories=FETAR_ORDER, ordered=True
        )
        fd = fd.sort_values("Faixa Etária")
        fig = px.bar(
            fd,
            x="Faixa Etária",
            y="Total",
            title="Distribuição por Faixa Etária",
            color="Total",
            color_continuous_scale="Viridis",
            text="Total",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(coloraxis_showscale=False)
        st.plotly_chart(fig, use_container_width=True)

    # Escolaridade
    with col2:
        ed = usuarios["escol4"].value_counts().reset_index()
        ed.columns = ["Escolaridade", "Total"]
        ed["Escolaridade"] = pd.Categorical(
            ed["Escolaridade"], categories=ESCOL_ORDER, ordered=True
        )
        ed = ed.sort_values("Escolaridade")
        # Rótulos curtos para o gráfico
        labels_curtos = {
            "Sem educação formal a 3 anos": "Sem instrução / até 3 anos",
            "De 4 a 7 anos": "Fund. incompleto (4-7 anos)",
            "De 8 a 11 anos": "Fund./Médio (8-11 anos)",
            "12 ou mais anos": "Superior (12+ anos)",
            "Ignorada": "Ignorada",
        }
        ed["Label"] = ed["Escolaridade"].map(lambda x: labels_curtos.get(x, x))
        fig = px.bar(
            ed,
            y="Label",
            x="Total",
            orientation="h",
            title="Distribuição por Escolaridade",
            color="Total",
            color_continuous_scale="Oranges",
            text="Total",
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            coloraxis_showscale=False,
            yaxis={"categoryorder": "array", "categoryarray": list(labels_curtos.values())},
        )
        st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Cruzamentos")
    col1, col2 = st.columns(2)

    # Faixa etária × Gênero
    with col1:
        ct = pd.crosstab(usuarios["fetar_clean"], usuarios["genero_simplificado"])
        available = [f for f in FETAR_ORDER if f in ct.index]
        ct = ct.reindex(available)
        melted = ct.reset_index().melt(id_vars="fetar_clean", var_name="Gênero", value_name="Total")
        fig = px.bar(
            melted,
            x="fetar_clean",
            y="Total",
            color="Gênero",
            barmode="stack",
            title="Faixa Etária × Gênero/População",
            labels={"fetar_clean": "Faixa Etária"},
            color_discrete_sequence=px.colors.qualitative.Safe,
        )
        fig.update_layout(xaxis={"categoryorder": "array", "categoryarray": FETAR_ORDER})
        st.plotly_chart(fig, use_container_width=True)

    # Raça × Gênero – heatmap
    with col2:
        ct2 = pd.crosstab(usuarios["raca4_cat"], usuarios["genero_simplificado"])
        fig = px.imshow(
            ct2,
            title="Raça/Cor × Gênero/População (Heatmap)",
            color_continuous_scale="Blues",
            text_auto=True,
            aspect="auto",
            labels={"color": "Usuários"},
        )
        st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# ABA 2 – Adesão à PrEP
# ---------------------------------------------------------------------------
def tab_adesao(adesao: pd.DataFrame):
    st.subheader("Tendências de Adesão à PrEP (2018–2024)")

    if adesao.empty:
        st.warning("Dados de adesão não disponíveis.")
        return

    # Série temporal
    trend = adesao.groupby(["ano", "status"]).size().reset_index(name="total")
    color_map = {
        "Em PrEP": "#2A9D8F",
        "Descontinuou": "#E63946",
        "Outro": "#ADB5BD",
    }
    fig = px.line(
        trend,
        x="ano",
        y="total",
        color="status",
        markers=True,
        title="Evolução Anual: Usuários em PrEP × Descontinuações",
        labels={"ano": "Ano", "total": "Usuários", "status": "Status"},
        color_discrete_map=color_map,
    )
    fig.update_layout(xaxis=dict(tickmode="linear", dtick=1))
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)

    # Pizza 2024
    with col1:
        s24 = adesao[adesao["ano"] == 2024]["status"].value_counts().reset_index()
        s24.columns = ["Status", "Total"]
        fig = px.pie(
            s24,
            values="Total",
            names="Status",
            title="Status em 31/12/2024",
            color="Status",
            color_discrete_map=color_map,
            hole=0.35,
        )
        fig.update_traces(textposition="inside", textinfo="percent+label")
        st.plotly_chart(fig, use_container_width=True)

    # Taxa de descontinuação
    with col2:
        pivot = trend.pivot(index="ano", columns="status", values="total").fillna(0)
        if "Em PrEP" in pivot.columns and "Descontinuou" in pivot.columns:
            pivot["taxa_desc"] = (
                pivot["Descontinuou"]
                / (pivot["Em PrEP"] + pivot["Descontinuou"])
                * 100
            ).round(1)
            fig = px.bar(
                pivot.reset_index(),
                x="ano",
                y="taxa_desc",
                title="Taxa de Descontinuação por Ano (%)",
                color="taxa_desc",
                color_continuous_scale="Reds",
                text="taxa_desc",
                labels={"taxa_desc": "%", "ano": "Ano"},
            )
            fig.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
            fig.update_layout(
                xaxis=dict(tickmode="linear", dtick=1),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")
    st.subheader("Adesão por Gênero/População")
    adesao_gen = adesao.groupby(["ano", "status", "genero_simplificado"]).size().reset_index(name="total")
    fig = px.bar(
        adesao_gen[adesao_gen["status"] == "Em PrEP"],
        x="ano",
        y="total",
        color="genero_simplificado",
        barmode="stack",
        title="Usuários em PrEP por Ano e Gênero/População",
        labels={"ano": "Ano", "total": "Usuários", "genero_simplificado": "Gênero"},
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig.update_layout(xaxis=dict(tickmode="linear", dtick=1))
    st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# ABA 3 – Mapa por Zona
# ---------------------------------------------------------------------------
def tab_mapa(usuarios: pd.DataFrame, dispensas: pd.DataFrame):
    st.subheader("Distribuição Geográfica no Rio de Janeiro")

    # Estatísticas por zona
    zona_stats = (
        usuarios["zona_rj"]
        .value_counts()
        .reset_index()
        .rename(columns={"zona_rj": "zona", "count": "usuarios"})
    )
    # Compatibilidade com diferentes versões do pandas
    if "count" in zona_stats.columns:
        zona_stats = zona_stats.rename(columns={"count": "usuarios"})
    else:
        zona_stats.columns = ["zona", "usuarios"]

    zona_stats["lat"] = zona_stats["zona"].map(
        lambda z: ZONA_COORDS.get(z, {"lat": -22.9068})["lat"]
    )
    zona_stats["lon"] = zona_stats["zona"].map(
        lambda z: ZONA_COORDS.get(z, {"lon": -43.1729})["lon"]
    )
    zona_stats["pct"] = (
        zona_stats["usuarios"] / zona_stats["usuarios"].sum() * 100
    ).round(1)

    col1, col2 = st.columns([3, 1])

    with col1:
        fig = px.scatter_mapbox(
            zona_stats,
            lat="lat",
            lon="lon",
            size="usuarios",
            color="zona",
            hover_name="zona",
            hover_data={"usuarios": True, "pct": True, "lat": False, "lon": False},
            title="Usuários de PrEP por Zona do Rio de Janeiro",
            mapbox_style="open-street-map",
            zoom=9.2,
            center={"lat": -22.92, "lon": -43.36},
            size_max=70,
            color_discrete_map=CORES_ZONA,
        )
        fig.update_layout(
            height=520,
            margin=dict(l=0, r=0, t=40, b=0),
            legend=dict(
                yanchor="top", y=0.99,
                xanchor="left", x=0.01,
                bgcolor="rgba(255,255,255,0.8)",
            ),
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Usuários por Zona**")
        for _, row in zona_stats.sort_values("usuarios", ascending=False).iterrows():
            cor = CORES_ZONA.get(row["zona"], "#ADB5BD")
            st.markdown(
                f"""
                <div style="
                    padding: 10px 12px;
                    margin: 5px 0;
                    border-left: 5px solid {cor};
                    background: #f8f9fa;
                    border-radius: 4px;
                ">
                    <strong>{row['zona']}</strong><br>
                    <span style="font-size:1.1em">{row['usuarios']:,}</span>
                    &nbsp;<small>({row['pct']}%)</small>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.info(
            "**Nota metodológica:** A zona foi atribuída por correspondência de "
            "palavras-chave no nome da unidade dispensadora (UDM). "
            "Registros sem referência geográfica no nome da unidade aparecem como "
            "*Não identificada*."
        )

    st.markdown("---")
    col1, col2 = st.columns(2)

    with col1:
        if "tp_servico_atendimento" in dispensas.columns:
            sd = (
                dispensas["tp_servico_atendimento"]
                .value_counts()
                .reset_index()
            )
            sd.columns = ["Tipo de Serviço", "Total"]
            fig = px.pie(
                sd,
                values="Total",
                names="Tipo de Serviço",
                title="Dispensações por Tipo de Serviço",
                hole=0.35,
                color_discrete_sequence=px.colors.qualitative.Pastel,
            )
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

    with col2:
        if "Publico_privado_disp" in dispensas.columns:
            pp = (
                dispensas["Publico_privado_disp"]
                .value_counts()
                .reset_index()
            )
            pp.columns = ["Tipo", "Total"]
            fig = px.bar(
                pp,
                x="Tipo",
                y="Total",
                title="Dispensações: Público × Privado",
                color="Tipo",
                text="Total",
                color_discrete_map={
                    "Público": "#457B9D",
                    "Privado": "#E63946",
                },
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)


# ---------------------------------------------------------------------------
# ABA 4 – Tabelas Resumo
# ---------------------------------------------------------------------------
def tab_tabelas(usuarios: pd.DataFrame):
    st.subheader("Tabelas Resumo")

    inner_tabs = st.tabs(
        ["Por variável", "Faixa Etária × Gênero", "Raça × Escolaridade", "Amostra de dados"]
    )

    variaveis = {
        "Gênero/População": "genero_simplificado",
        "Faixa Etária": "fetar_clean",
        "Raça/Cor": "raca4_cat",
        "Escolaridade": "escol4",
        "Zona RJ": "zona_rj",
    }

    with inner_tabs[0]:
        for nome, col in variaveis.items():
            if col not in usuarios.columns:
                continue
            df = usuarios[col].value_counts().reset_index()
            df.columns = ["Categoria", "Total"]
            df["% do total"] = (df["Total"] / df["Total"].sum() * 100).round(1)
            st.markdown(f"**{nome}**")
            st.dataframe(df, use_container_width=True, hide_index=True)

    with inner_tabs[1]:
        ct = pd.crosstab(
            usuarios["fetar_clean"],
            usuarios["genero_simplificado"],
            margins=True,
            margins_name="Total",
        )
        avail = [f for f in FETAR_ORDER if f in ct.index] + ["Total"]
        st.dataframe(ct.reindex([a for a in avail if a in ct.index]), use_container_width=True)

    with inner_tabs[2]:
        ct2 = pd.crosstab(
            usuarios["raca4_cat"],
            usuarios["escol4"],
            margins=True,
            margins_name="Total",
        )
        st.dataframe(ct2, use_container_width=True)

    with inner_tabs[3]:
        cols = [c for c in [
            "Cod_unificado", "raca4_cat", "escol4", "fetar_clean",
            "genero_simplificado", "zona_rj", "dt_disp_min", "dt_disp_max",
        ] if c in usuarios.columns]
        st.dataframe(usuarios[cols].head(2000), use_container_width=True, hide_index=True)
        st.caption(f"Exibindo 2.000 de {len(usuarios):,} registros totais.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    if not DB_PATH.exists():
        st.error(
            f"**Banco de dados não encontrado:** `{DB_PATH.name}`  \n\n"
            "Execute o pipeline antes de abrir o dashboard:\n"
            "```\npython pipeline.py\n```"
        )
        st.stop()

    with st.spinner("Carregando dados do banco…"):
        usuarios, dispensas, adesao, novos_ano = load_all()

    # Sidebar
    gen_sel, raca_sel, zona_sel = sidebar(usuarios)
    usuarios_f = apply_filters(usuarios, gen_sel, raca_sel, zona_sel)
    adesao_f = adesao[
        adesao["genero_simplificado"].isin(gen_sel)
        & adesao["raca4_cat"].isin(raca_sel)
        & adesao["zona_rj"].isin(zona_sel)
    ]

    # Cabeçalho
    st.title("Análise Sociodemográfica dos Usuários de PrEP")
    st.markdown(
        "**Município do Rio de Janeiro · 2018–2024**  "
        "| Fonte: Painel de PrEP – DATASUS / Ministério da Saúde"
    )
    if len(usuarios_f) < len(usuarios):
        st.info(f"Filtros ativos: exibindo **{len(usuarios_f):,}** de {len(usuarios):,} usuários.")

    st.markdown("---")

    tabs = st.tabs([
        "📊 Visão Geral",
        "👥 Perfil Sociodemográfico",
        "💊 Adesão à PrEP",
        "🗺️ Mapa por Zona",
        "📋 Tabelas",
    ])

    with tabs[0]:
        tab_overview(usuarios_f, dispensas, novos_ano)
    with tabs[1]:
        tab_sociodemografico(usuarios_f)
    with tabs[2]:
        tab_adesao(adesao_f)
    with tabs[3]:
        tab_mapa(usuarios_f, dispensas)
    with tabs[4]:
        tab_tabelas(usuarios_f)

    st.markdown("---")
    st.caption(
        "TCC · Engenharia de Dados em Saúde: Pipeline para Análise Sociodemográfica "
        "dos Usuários de PrEP no Rio de Janeiro"
    )


if __name__ == "__main__":
    main()
