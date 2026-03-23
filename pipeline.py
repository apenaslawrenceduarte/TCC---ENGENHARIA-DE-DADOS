"""
Pipeline ETL – Análise Sociodemográfica dos Usuários de PrEP no Rio de Janeiro
TCC: Engenharia de Dados em Saúde

Etapas:
  1. EXTRACT  – Leitura dos arquivos CSV do DATASUS
  2. TRANSFORM – Filtragem (município RJ), limpeza e enriquecimento
  3. LOAD      – Carga no banco de dados SQLite prep_rj.db

Uso:
    python pipeline.py
"""

import re
import sqlite3
import logging
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
USUARIOS_CSV = BASE_DIR / "Banco_PrEP_usuarios.csv"
DISPENSAS_CSV = BASE_DIR / "Banco_PrEP_dispensas.csv"
DB_PATH = BASE_DIR / "prep_rj.db"

# Código IBGE do município do Rio de Janeiro (7 dígitos, com dígito verificador)
RJ_IBGE = "3304557"

# ---------------------------------------------------------------------------
# Mapeamento de zonas do Rio de Janeiro
#
# Estratégia (em ordem de prioridade):
#  1. Código AP no nome da UDM  (ex: "SMS CMS JOAO BARROS - AP 21" → Zona Sul)
#  2. Serviços conhecidos sem código AP (FIOCRUZ/INI, HUCFF, etc.)
#  3. Palavras-chave de bairro/região no nome
#
# Áreas de Planejamento do Rio de Janeiro → Zona:
#   AP 1.0 (AP 10) – Centro               → Zona Central
#   AP 2.1 (AP 21) – Zona Sul             → Zona Sul
#   AP 2.2 (AP 22) – Grande Tijuca        → Zona Sul
#   AP 3.1 (AP 31) – Zona Norte (Ramos)   → Zona Norte
#   AP 3.2 (AP 32) – Zona Norte (Méier)   → Zona Norte
#   AP 3.3 (AP 33) – Zona Norte (Madureira)→ Zona Norte
#   AP 4.0 (AP 40) – Barra/Jacarepaguá    → Zona Sudoeste
#   AP 5.1 (AP 51) – Bangu/Realengo       → Zona Sudoeste
#   AP 5.2 (AP 52) – Campo Grande         → Zona Sudoeste
#   AP 5.3 (AP 53) – Santa Cruz           → Zona Sudoeste
# ---------------------------------------------------------------------------
_AP_TO_ZONA: dict[str, str] = {
    "10": "Zona Central",
    "21": "Zona Sul",
    "22": "Zona Sul",
    "31": "Zona Norte",
    "32": "Zona Norte",
    "33": "Zona Norte",
    "40": "Zona Sudoeste",
    "51": "Zona Sudoeste",
    "52": "Zona Sudoeste",
    "53": "Zona Sudoeste",
}

# Serviços de referência conhecidos que não usam "AP XX" no nome
_KNOWN_UNITS: dict[str, str] = {
    "EVANDRO CHAGAS": "Zona Norte",   # INI/FIOCRUZ – Manguinhos
    "FIOCRUZ":        "Zona Norte",
    "HUCFF":          "Zona Norte",   # UFRJ – Ilha do Fundão
    "IPEC":           "Zona Norte",
    "GAFFREE":        "Zona Sul",     # Hospital Gaffrée e Guinle – Tijuca
    "GUINLE":         "Zona Sul",
    "ROCHA MAIA":     "Zona Sul",
    "TORTELLY":       "Zona Norte",   # HM Carlos Tortelly – Niterói (fora do RJ)
    "FRANCISCO DE ASSIS": "Zona Central",
    "HELIO PELLEGRINO":   "Zona Sul",
}

# Palavras-chave de bairro como fallback
_KW_BAIRRO: dict[str, str] = {
    # Central
    "CENTRO":          "Zona Central",
    "LAPA":            "Zona Central",
    "SANTA TERESA":    "Zona Central",
    "GLORIA":          "Zona Central",
    "CATUMBI":         "Zona Central",
    "RIO COMPRIDO":    "Zona Central",
    "GAMBOA":          "Zona Central",
    "CAJU":            "Zona Central",
    # Sul
    "BOTAFOGO":        "Zona Sul",
    "COPACABANA":      "Zona Sul",
    "IPANEMA":         "Zona Sul",
    "LEBLON":          "Zona Sul",
    "FLAMENGO":        "Zona Sul",
    "LARANJEIRAS":     "Zona Sul",
    "TIJUCA":          "Zona Sul",
    "VILA ISABEL":     "Zona Sul",
    "ANDARAI":         "Zona Sul",
    "GRAJAI":          "Zona Sul",
    "MARACANA":        "Zona Sul",
    # Norte
    "MANGUINHOS":      "Zona Norte",
    "MARE":            "Zona Norte",
    "BONSUCESSO":      "Zona Norte",
    "PENHA":           "Zona Norte",
    "RAMOS":           "Zona Norte",
    "MEIER":           "Zona Norte",
    "MADUREIRA":       "Zona Norte",
    "PAVUNA":          "Zona Norte",
    "GUADALUPE":       "Zona Norte",
    "ANCHIETA":        "Zona Norte",
    "INHAUMA":         "Zona Norte",
    "IRAJA":           "Zona Norte",
    # Sudoeste
    "BARRA DA TIJUCA": "Zona Sudoeste",
    "JACAREPAGUA":     "Zona Sudoeste",
    "RECREIO":         "Zona Sudoeste",
    "BANGU":           "Zona Sudoeste",
    "CAMPO GRANDE":    "Zona Sudoeste",
    "SANTA CRUZ":      "Zona Sudoeste",
    "REALENGO":        "Zona Sudoeste",
    "DEODORO":         "Zona Sudoeste",
    "PACIENCIA":       "Zona Sudoeste",
    "SEPETIBA":        "Zona Sudoeste",
}

_AP_RE = re.compile(r"\bAP\s*(\d{1,2})\b")


def _remove_accents(text: str) -> str:
    """Remove acentos para facilitar comparação de strings."""
    table = str.maketrans(
        "áàãâäéèêëíìîïóòõôöúùûüçÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ",
        "aaaaaeeeeiiiioooooouuuucAAAAEEEEIIIIOOOOOUUUUC",
    )
    return text.translate(table)


def assign_zone(nome_udm) -> str:
    """Atribui zona do Rio de Janeiro com base no nome da UDM."""
    if pd.isna(nome_udm):
        return "Não identificada"

    normalized = _remove_accents(str(nome_udm).upper())

    # Prioridade 1: código AP (ex: "AP 21", "AP21")
    m = _AP_RE.search(normalized)
    if m:
        code = m.group(1).zfill(2)
        if code in _AP_TO_ZONA:
            return _AP_TO_ZONA[code]

    # Prioridade 2: serviços de referência conhecidos
    for kw, zona in _KNOWN_UNITS.items():
        if kw in normalized:
            return zona

    # Prioridade 3: palavras-chave de bairro
    for kw, zona in _KW_BAIRRO.items():
        if kw in normalized:
            return zona

    return "Não identificada"


# ---------------------------------------------------------------------------
# ETAPA 1 – EXTRACT
# ---------------------------------------------------------------------------
def extract() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Lê os arquivos CSV e retorna os DataFrames brutos."""
    log.info("=" * 60)
    log.info("ETAPA 1 – EXTRACT")
    log.info("=" * 60)

    for path in (USUARIOS_CSV, DISPENSAS_CSV):
        if not path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    log.info("Lendo Banco_PrEP_usuarios.csv …")
    usuarios = pd.read_csv(
        USUARIOS_CSV,
        dtype={"codigo_ibge_resid": str, "cod_ibge_udm": str, "Cod_unificado": str},
        encoding="utf-8",
    )
    log.info(f"  → {len(usuarios):>10,} usuários lidos")

    log.info("Lendo Banco_PrEP_dispensas.csv …")
    dispensas = pd.read_csv(
        DISPENSAS_CSV,
        dtype={"cod_ibge_udm": str, "Cod_unificado": str},
        encoding="utf-8",
    )
    log.info(f"  → {len(dispensas):>10,} dispensações lidas")

    return usuarios, dispensas


# ---------------------------------------------------------------------------
# ETAPA 2 – TRANSFORM
# ---------------------------------------------------------------------------
def transform(
    usuarios: pd.DataFrame, dispensas: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Filtra, limpa, enriquece e retorna dados prontos para carga."""
    log.info("=" * 60)
    log.info("ETAPA 2 – TRANSFORM")
    log.info("=" * 60)

    # ------------------------------------------------------------------
    # 2a. Filtrar Rio de Janeiro
    # ------------------------------------------------------------------
    log.info(f"Filtrando residentes do Rio de Janeiro (IBGE {RJ_IBGE}) …")
    usuarios_rj = usuarios[
        usuarios["codigo_ibge_resid"].str.strip() == RJ_IBGE
    ].copy()
    log.info(f"  → {len(usuarios_rj):>10,} usuários residentes no RJ")

    rj_ids = set(usuarios_rj["Cod_unificado"].unique())
    dispensas_rj = dispensas[dispensas["Cod_unificado"].isin(rj_ids)].copy()
    log.info(f"  → {len(dispensas_rj):>10,} dispensações de usuários do RJ")

    # ------------------------------------------------------------------
    # 2b. Variável genero_simplificado (manter categoria original + label curta)
    # ------------------------------------------------------------------
    genero_map = {
        "Gays e outros HSH cis": "Gays/HSH",
        "Mulheres cis": "Mulheres cis",
        "Homens heterossexuais cis": "Homens hétero",
        "Mulheres trans": "Mulheres trans",
        "Homens trans": "Homens trans",
        "Não binaries": "Não-binário",
        "Travestis": "Travestis",
    }
    usuarios_rj["genero_simplificado"] = (
        usuarios_rj["Pop_genero_pratica"]
        .map(genero_map)
        .fillna("Não informado")
    )

    # ------------------------------------------------------------------
    # 2c. fetar_clean – strip de espaços
    # ------------------------------------------------------------------
    usuarios_rj["fetar_clean"] = usuarios_rj["fetar"].str.strip()

    # ------------------------------------------------------------------
    # 2d. Escolaridade – ordem ordinal para visualização
    # ------------------------------------------------------------------
    escol_ordem = {
        "Sem educação formal a 3 anos": 1,
        "De 4 a 7 anos": 2,
        "De 8 a 11 anos": 3,
        "12 ou mais anos": 4,
        "Ignorada": 5,
    }
    usuarios_rj["escolaridade_ordem"] = (
        usuarios_rj["escol4"].map(escol_ordem).fillna(5).astype(int)
    )

    # ------------------------------------------------------------------
    # 2e. Atribuição de zona do Rio de Janeiro
    # ------------------------------------------------------------------
    log.info("Atribuindo zonas às UDMs …")
    usuarios_rj["zona_rj"] = usuarios_rj["nome_udm"].apply(assign_zone)
    dispensas_rj["zona_rj"] = dispensas_rj["nome_udm"].apply(assign_zone)

    zona_dist = usuarios_rj["zona_rj"].value_counts()
    for zona, n in zona_dist.items():
        pct = n / len(usuarios_rj) * 100
        log.info(f"    {zona:<22} {n:>6,} ({pct:.1f}%)")

    # ------------------------------------------------------------------
    # 2f. Parsing de datas
    # ------------------------------------------------------------------
    for df in (usuarios_rj, dispensas_rj):
        for col in ("dt_disp", "dt_disp_min", "dt_disp_max"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        if "dt_disp" in df.columns:
            df["ano_disp"] = df["dt_disp"].dt.year.astype("Int64")
            df["mes_disp"] = df["dt_disp"].dt.month.astype("Int64")
        if "dt_disp_min" in df.columns:
            df["ano_inicio"] = df["dt_disp_min"].dt.year.astype("Int64")

    # ------------------------------------------------------------------
    # 2g. Adesão temporal: desmembrar colunas EmPrEP_YYYY
    # ------------------------------------------------------------------
    log.info("Criando tabela de adesão temporal …")
    anos = [str(a) for a in range(2018, 2025)]
    adesao_dfs = []
    for ano in anos:
        col = f"EmPrEP_{ano}"
        if col not in usuarios_rj.columns:
            continue
        df_ano = usuarios_rj[["Cod_unificado", "genero_simplificado",
                               "raca4_cat", "fetar_clean", "zona_rj", col]].copy()
        df_ano = df_ano.rename(columns={col: "status_raw"})
        df_ano = df_ano[df_ano["status_raw"].notna()]
        df_ano["ano"] = int(ano)
        df_ano["status"] = df_ano["status_raw"].apply(
            lambda x: "Em PrEP" if "Em PrEP" in str(x)
            else ("Descontinuou" if "Descontinuou" in str(x) else "Outro")
        )
        adesao_dfs.append(df_ano.drop(columns="status_raw"))

    adesao_temporal = (
        pd.concat(adesao_dfs, ignore_index=True)
        if adesao_dfs
        else pd.DataFrame(columns=["Cod_unificado", "ano", "status",
                                    "genero_simplificado", "raca4_cat",
                                    "fetar_clean", "zona_rj"])
    )
    log.info(f"  → {len(adesao_temporal):>10,} registros na tabela de adesão")

    # ------------------------------------------------------------------
    # 2h. Novos usuários por ano (primeira dispensação)
    # ------------------------------------------------------------------
    if "ano_inicio" in usuarios_rj.columns:
        novos_ano = (
            usuarios_rj.groupby("ano_inicio")
            .size()
            .reset_index(name="novos_usuarios")
            .rename(columns={"ano_inicio": "ano"})
        )
    else:
        novos_ano = pd.DataFrame(columns=["ano", "novos_usuarios"])

    return usuarios_rj, dispensas_rj, adesao_temporal, novos_ano


# ---------------------------------------------------------------------------
# ETAPA 3 – LOAD
# ---------------------------------------------------------------------------
def load(
    usuarios_rj: pd.DataFrame,
    dispensas_rj: pd.DataFrame,
    adesao_temporal: pd.DataFrame,
    novos_ano: pd.DataFrame,
) -> None:
    """Carrega os dados no banco SQLite e cria views analíticas."""
    log.info("=" * 60)
    log.info("ETAPA 3 – LOAD")
    log.info("=" * 60)

    # Converter colunas datetime para string (SQLite não tem tipo DATE nativo)
    def _prep_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
            df[col] = df[col].astype(str)
        return df

    conn = sqlite3.connect(DB_PATH)
    try:
        usuarios_rj = _prep_df(usuarios_rj)
        dispensas_rj = _prep_df(dispensas_rj)

        usuarios_rj.to_sql("usuarios", conn, if_exists="replace", index=False)
        log.info(f"  Tabela 'usuarios'      → {len(usuarios_rj):>7,} linhas")

        dispensas_rj.to_sql("dispensas", conn, if_exists="replace", index=False)
        log.info(f"  Tabela 'dispensas'     → {len(dispensas_rj):>7,} linhas")

        adesao_temporal.to_sql("adesao_temporal", conn, if_exists="replace", index=False)
        log.info(f"  Tabela 'adesao_temporal' → {len(adesao_temporal):>5,} linhas")

        novos_ano.to_sql("novos_ano", conn, if_exists="replace", index=False)
        log.info(f"  Tabela 'novos_ano'     → {len(novos_ano):>7,} linhas")

        # ------------------------------------------------------------------
        # Views analíticas
        # ------------------------------------------------------------------
        views = {
            "v_genero": """
                SELECT genero_simplificado AS categoria,
                       COUNT(*) AS total,
                       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM usuarios), 2) AS pct
                FROM usuarios
                GROUP BY genero_simplificado
                ORDER BY total DESC
            """,
            "v_raca": """
                SELECT raca4_cat AS categoria,
                       COUNT(*) AS total,
                       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM usuarios), 2) AS pct
                FROM usuarios
                GROUP BY raca4_cat
                ORDER BY total DESC
            """,
            "v_fetar": """
                SELECT fetar_clean AS categoria,
                       COUNT(*) AS total,
                       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM usuarios), 2) AS pct
                FROM usuarios
                GROUP BY fetar_clean
                ORDER BY total DESC
            """,
            "v_escolaridade": """
                SELECT escol4 AS categoria,
                       escolaridade_ordem,
                       COUNT(*) AS total,
                       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM usuarios), 2) AS pct
                FROM usuarios
                GROUP BY escol4, escolaridade_ordem
                ORDER BY escolaridade_ordem
            """,
            "v_zona": """
                SELECT zona_rj AS zona,
                       COUNT(*) AS total,
                       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM usuarios), 2) AS pct
                FROM usuarios
                GROUP BY zona_rj
                ORDER BY total DESC
            """,
            "v_adesao_ano": """
                SELECT ano, status,
                       COUNT(*) AS total
                FROM adesao_temporal
                GROUP BY ano, status
                ORDER BY ano, status
            """,
        }

        for view_name, view_sql in views.items():
            conn.execute(f"DROP VIEW IF EXISTS {view_name}")
            conn.execute(f"CREATE VIEW {view_name} AS {view_sql}")

        conn.commit()
        log.info(f"  {len(views)} views analíticas criadas.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Orquestração principal
# ---------------------------------------------------------------------------
def run_pipeline() -> None:
    log.info("╔══════════════════════════════════════════════════════════╗")
    log.info("║  PIPELINE DE DADOS – PrEP RIO DE JANEIRO (2018-2024)    ║")
    log.info("╚══════════════════════════════════════════════════════════╝")

    usuarios, dispensas = extract()
    usuarios_rj, dispensas_rj, adesao_temporal, novos_ano = transform(usuarios, dispensas)
    load(usuarios_rj, dispensas_rj, adesao_temporal, novos_ano)

    log.info("=" * 60)
    log.info("Pipeline concluído com sucesso!")
    log.info(f"  Banco de dados: {DB_PATH}")
    log.info("  Próximo passo : streamlit run dashboard.py")
    log.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
