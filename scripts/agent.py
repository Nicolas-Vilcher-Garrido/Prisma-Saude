# -*- coding: utf-8 -*-
"""
agent.py
Agente de relatórios de saúde (Excel + Python + SQL):

- Lê CSV/TXT de sample_data (auto detecção de delimitador e encoding)
- Limpa dados:
    • Data inválida -> descarta
    • Negativos em Qtde/PrecoUnitario -> 0
    • Calcula Receita ausente = Qtde * PrecoUnitario
    • Remove duplicados por (Data, ClienteId, Procedimento)
- Merge com DimClientes (-> Segmento)
- Abas do Excel: Dados, DimClientes, Rankings, Auditoria e Resumo
- Resumo: 1 gráfico (barras = Total por mês; linhas = Top-N clientes)
- Auditoria com contagens + período
- Persistência SQL opcional: staging #tmp_at + EXEC app.sp_UpsertAtendimentos

Requisitos do ambiente:
    pip install pandas pyyaml xlwings pyodbc openpyxl
Fechar o Excel antes de executar este script.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import logging
from pathlib import Path
from typing import List, Tuple, Dict

import pandas as pd
import yaml
import xlwings as xw

try:
    import pyodbc  # opcional: apenas se SQL estiver habilitado
except Exception:
    pyodbc = None

# --------------------------------------------------------------------------------------
# Caminhos/Constantes
# --------------------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[1]
WB_PATH = ROOT / "Relatorio_Saude.xlsm"
LOG_DIR = ROOT / "logs"
LOG_FILE = LOG_DIR / "agent.log"
CONFIG_FILE = ROOT / "config.yaml"
SAMPLE_SIG = ROOT / "sample_data" / "sigsaude"
SAMPLE_OP = ROOT / "sample_data" / "operadoras"
DIM_CLIENTES_PATH = ROOT / "sample_data" / "dim_clientes.csv"

SHEET_RESUMO = "Resumo"
SHEET_DADOS = "Dados"
SHEET_DIM = "DimClientes"
SHEET_RANK = "Rankings"
SHEET_PARAM = "Parametros"
SHEET_AUDIT = "Auditoria"

COLS_BASE = [
    "Data", "ClienteId", "Operadora", "Procedimento", "Categoria",
    "UF", "Qtde", "PrecoUnitario", "Receita"
]
COLS_SQL = [
    "Data", "ClienteId", "Operadora", "Procedimento", "Categoria",
    "Qtde", "PrecoUnitario", "Receita"
]

# Excel chart enums
XL_LINE = 4
XL_COLUMN = 51

# --------------------------------------------------------------------------------------
# Utilidades de log/config
# --------------------------------------------------------------------------------------

def setup_logger() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("relatorio_saude")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
        fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
    return logger


def load_config() -> dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

# --------------------------------------------------------------------------------------
# Leitura/normalização
# --------------------------------------------------------------------------------------

def detect_delimiter_and_encoding(p: Path) -> Tuple[str, str]:
    """Detecção simples (sem chardet): tenta utf-8->latin-1; delimitador pelo header."""
    encodings = ["utf-8", "latin-1"]
    header_line = ""
    used_enc = "utf-8"
    for enc in encodings:
        try:
            with open(p, "r", encoding=enc, errors="strict") as f:
                header_line = f.readline()
            used_enc = enc
            break
        except Exception:
            continue
    if not header_line:
        with open(p, "r", encoding="utf-8", errors="ignore") as f:
            header_line = f.readline()
        used_enc = "utf-8"
    for d in [",", ";", "|", "\t"]:
        if d in header_line:
            return d, used_enc
    try:
        dialect = csv.Sniffer().sniff(header_line)
        return dialect.delimiter, used_enc
    except Exception:
        return ",", used_enc


def load_all_data(logger: logging.Logger) -> Tuple[pd.DataFrame, List[Tuple[str,int,str,str]]]:
    """Lê arquivos de sigsaude/ e operadoras/."""
    files: List[Path] = []
    if SAMPLE_SIG.exists():
        files += list(SAMPLE_SIG.glob("*.*"))
    if SAMPLE_OP.exists():
        files += list(SAMPLE_OP.glob("*.*"))

    infos = []
    frames = []
    for fp in files:
        delim, enc = detect_delimiter_and_encoding(fp)
        try:
            df = pd.read_csv(fp, delimiter=delim, encoding=enc)
        except Exception:
            df = pd.read_csv(fp, delimiter=delim, encoding="utf-8", errors="ignore")
            enc = "utf-8(ignore)"
        rows = len(df)
        infos.append((fp.name, rows, delim, enc))
        logger.info(f"Imported {rows} rows from {fp.name} (delimiter '{delim}', encoding '{enc}')")
        frames.append(df)

    if frames:
        raw = pd.concat(frames, axis=0, ignore_index=True)
    else:
        raw = pd.DataFrame(columns=COLS_BASE)
    return raw, infos


def normalize_and_clean(raw: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str,int]]:
    """Normaliza colunas, trata tipos, calcula Receita, zera negativos e descarta datas inválidas."""
    df = raw.copy()

    # padroniza nomes
    rename_map = {c: c.strip() for c in df.columns}
    df = df.rename(columns=rename_map)

    for c in COLS_BASE:
        if c not in df.columns:
            df[c] = pd.NA

    # Data
    s = pd.to_datetime(df["Data"], errors="coerce")
    invalid_dates = int(s.isna().sum())
    df = df.loc[s.notna()].copy()
    df["Data"] = s.loc[s.notna()].dt.date

    # Numéricos
    neg_counts = {"neg_qtde": 0, "neg_preco": 0}
    for col in ["Qtde", "PrecoUnitario", "Receita"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # negativos -> 0
    m_q = df["Qtde"] < 0
    m_p = df["PrecoUnitario"] < 0
    neg_counts["neg_qtde"] = int(m_q.fillna(False).sum())
    neg_counts["neg_preco"] = int(m_p.fillna(False).sum())
    df.loc[m_q, "Qtde"] = 0
    df.loc[m_p, "PrecoUnitario"] = 0.0

    # receita ausente
    rec_na = df["Receita"].isna()
    df.loc[rec_na, "Receita"] = df.loc[rec_na, "Qtde"].fillna(0) * df.loc[rec_na, "PrecoUnitario"].fillna(0.0)

    # strings
    for c in ["Operadora", "Procedimento", "Categoria", "ClienteId", "UF"]:
        df[c] = df[c].astype(str).str.strip()

    metrics = {
        "invalid_dates": invalid_dates,
        "neg_qtde": neg_counts["neg_qtde"],
        "neg_preco": neg_counts["neg_preco"],
    }
    return df, metrics


def apply_filters(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    df = df.copy()
    pi = pd.to_datetime(cfg.get("periodo_inicio"), errors="coerce")
    pf = pd.to_datetime(cfg.get("periodo_fim"), errors="coerce")
    if not pd.isna(pi):
        df = df[pd.to_datetime(df["Data"]) >= pi]
    if not pd.isna(pf):
        df = df[pd.to_datetime(df["Data"]) <= pf]
    uf_inc = set(cfg.get("uf_incluir", []))
    if uf_inc:
        df = df[df["UF"].isin(uf_inc)]
    cat_inc = set(cfg.get("categoria_incluir", []))
    if cat_inc:
        df = df[df["Categoria"].isin(cat_inc)]
    df["Data"] = pd.to_datetime(df["Data"]).dt.date
    return df


def remove_duplicates(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    before = len(df)
    df2 = df.drop_duplicates(subset=["Data", "ClienteId", "Procedimento"], keep="last")
    return df2, before - len(df2)


def load_dim_clientes() -> pd.DataFrame:
    if DIM_CLIENTES_PATH.exists():
        delim, enc = detect_delimiter_and_encoding(DIM_CLIENTES_PATH)
        dim = pd.read_csv(DIM_CLIENTES_PATH, delimiter=delim, encoding=enc)
    else:
        dim = pd.DataFrame(columns=["ClienteId", "Segmento"])
    if "ClienteId" not in dim.columns:
        dim["ClienteId"] = pd.NA
    if "Segmento" not in dim.columns:
        dim["Segmento"] = pd.NA
    dim["ClienteId"] = dim["ClienteId"].astype(str).str.strip()
    dim["Segmento"] = dim["Segmento"].astype(str).str.strip()
    return dim


def merge_dim(df: pd.DataFrame, dim: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["ClienteId"] = d["ClienteId"].astype(str).str.strip()
    dm = dim.copy()
    dm["ClienteId"] = dm["ClienteId"].astype(str).str.strip()
    return d.merge(dm, on="ClienteId", how="left")


def compute_percentile_flag(df: pd.DataFrame, col="Receita", p=0.9) -> Tuple[pd.DataFrame, float]:
    d = df.copy()
    v = pd.to_numeric(d[col], errors="coerce").fillna(0)
    p90 = v.quantile(p) if len(v) else 0.0
    d["AltoValor"] = (v >= p90).astype(int)
    return d, float(p90)


def make_pivots_and_rankings(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    d = df.copy()
    d["Mes"] = pd.to_datetime(d["Data"]).dt.to_period("M").astype(str)
    pv = pd.pivot_table(
        d, index="Mes", columns=["Categoria", "Operadora"],
        values="Receita", aggfunc="sum", fill_value=0.0
    )
    top_oper = (
        d.groupby("Operadora", as_index=False)["Receita"].sum()
         .sort_values("Receita", ascending=False).head(20)
    )
    top_proc = (
        d.groupby("Procedimento", as_index=False)["Receita"].sum()
         .sort_values("Receita", ascending=False).head(20)
    )
    return pv, top_oper, top_proc

# --------------------------------------------------------------------------------------
# Excel helpers
# --------------------------------------------------------------------------------------

def open_wb() -> xw.Book:
    if not WB_PATH.exists():
        raise FileNotFoundError(f"Workbook não encontrado: {WB_PATH}")
    app = xw.App(visible=False, add_book=False)
    app.display_alerts = False
    app.screen_updating = False
    wb = app.books.open(str(WB_PATH))
    return wb


def ensure_sheets(wb: xw.Book):
    for name in [SHEET_RESUMO, SHEET_DADOS, SHEET_DIM, SHEET_RANK, SHEET_PARAM, SHEET_AUDIT]:
        if name not in [s.name for s in wb.sheets]:
            wb.sheets.add(name)


def clear_and_write(ws: xw.Sheet, df: pd.DataFrame, start_cell: str = "A1"):
    ws.clear()
    if df is None or df.empty:
        ws.range(start_cell).value = [["(sem dados)"]]
        return
    ws.range(start_cell).options(index=False).value = df


def write_rankings_sheet(wb: xw.Book, top_oper: pd.DataFrame, top_proc: pd.DataFrame):
    ws = wb.sheets[SHEET_RANK]
    ws.clear()
    ws.range("A1").value = "Top Operadoras (Receita)"
    ws.range("A2").options(index=False).value = top_oper
    ws.range("E1").value = "Top Procedimentos (Receita)"
    ws.range("E2").options(index=False).value = top_proc


def write_auditoria(ws: xw.Sheet, cfg: dict, meta: dict):
    ws.clear()
    rows = [
        ["Execucao", dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
        ["Periodo_inicio", cfg.get("periodo_inicio")],
        ["Periodo_fim", cfg.get("periodo_fim")],
        ["Moeda", cfg.get("moeda", "R$")],
        ["UF_incluir", ", ".join(cfg.get("uf_incluir", []))],
        ["Categoria_incluir", ", ".join(cfg.get("categoria_incluir", []))],
        ["Arquivos_lidos", meta.get("arquivos_lidos", 0)],
        ["Registros_importados", meta.get("registros_importados", 0)],
        ["Registros_filtrados", meta.get("registros_filtrados", 0)],
        ["Descartes_data_invalida", meta.get("descartes_data", 0)],
        ["Negativos_Qtde_zerados", meta.get("neg_qtde", 0)],
        ["Negativos_Preco_zerados", meta.get("neg_preco", 0)],
        ["Duplicados_removidos", meta.get("duplicados", 0)],
        ["Percentil90_Receita", meta.get("p90", 0.0)],
        ["Persistidos_SQL", meta.get("persistidos_sql", 0)],
    ]
    ws.range("A1").value = rows
    ws.range("A1:A15").api.Font.Bold = True

# --------------------------------------------------------------------------------------
# Tabela de apoio + gráfico único (Total por mês + Top-N clientes como linhas)
# --------------------------------------------------------------------------------------

def make_support_table(df: pd.DataFrame, top_n: int = 5) -> pd.DataFrame:
    d = df.copy()
    s = pd.to_datetime(d["Data"], errors="coerce")
    d = d.loc[s.notna()].copy()
    d["Mes"] = s.loc[s.notna()].dt.to_period("M").astype(str)
    d["Chave"] = d["ClienteId"].astype(str) + " – " + d["Segmento"].fillna("N/A")
    pv = d.pivot_table(index="Mes", columns="Chave", values="Receita", aggfunc="sum", fill_value=0.0).sort_index()
    total = pv.sum(axis=1)
    top_cols = list(pv.sum(axis=0).sort_values(ascending=False).head(top_n).index)
    support = pd.DataFrame({"Mes": pv.index, "Total": total.values})
    for c in top_cols:
        support[c] = pv[c].values
    return support


def generate_dashboard_chart(wb: xw.Book, support_df: pd.DataFrame):
    ws_r = wb.sheets[SHEET_RESUMO]

    # remove gráficos antigos
    for ch in list(ws_r.api.ChartObjects()):
        try:
            ch.Delete()
        except Exception:
            pass

    # escreve a tabela de apoio
    ws_r.clear()
    ws_r.range("A1").value = [["Dashboard — Receita por Mês (Total + Top clientes)"]]
    start = "A3"
    ws_r.range(start).options(index=False).value = support_df

    # seleciona faixa
    tbl = ws_r.range(start).expand()
    last_row = tbl.rows.count + tbl.row - 1
    last_col = tbl.columns.count + tbl.column - 1
    data_rng = ws_r.range((tbl.row, tbl.column), (last_row, last_col)).api

    # cria gráfico
    co = ws_r.api.ChartObjects().Add(Left=20, Top=120, Width=1100, Height=360)
    co.Name = "ch_dashboard"
    ch = co.Chart
    ch.SetSourceData(Source=data_rng)
    ch.PlotBy = 2  # xlColumns
    ch.ChartType = XL_COLUMN
    ch.HasTitle = True
    ch.ChartTitle.Text = "Receita por Mês (Total + Top clientes)"
    ch.HasLegend = True

    # eixo Y em R$
    try:
        # 2 = xlValue
        ch.Axes(2).TickLabels.NumberFormat = "R$ #,##0.00"
    except Exception:
        pass

    # Série 1 = Total (coluna); demais = linhas
    sc = ch.SeriesCollection()
    if sc.Count >= 1:
        sc.Item(1).ChartType = XL_COLUMN
        sc.Item(1).Name = "Total"
    for i in range(2, sc.Count + 1):
        sc.Item(i).ChartType = XL_LINE

    # categorias inclinadas
    try:
        ch.Axes(1).TickLabels.Orientation = 45  # xlCategory
    except Exception:
        pass

# --------------------------------------------------------------------------------------
# Persistência SQL (staging + MERGE por proc)
# --------------------------------------------------------------------------------------

def _conn_str(c: dict) -> str:
    driver = c.get("driver", "ODBC Driver 18 for SQL Server")
    server = c.get("server", "localhost,1433")
    db = c.get("database", "SaudeDev")
    uid = c.get("username", "etl_user")
    pwd = c.get("password", "")
    encrypt = "yes" if c.get("encrypt") else "no"
    tsc = "yes" if c.get("trust_server_certificate", True) else "no"
    return (f"DRIVER={{{driver}}};SERVER={server};DATABASE={db};"
            f"UID={uid};PWD={pwd};Encrypt={encrypt};TrustServerCertificate={tsc}")


def persist_to_sql_merge(df_final: pd.DataFrame, logger: logging.Logger | None = None) -> int:
    if pyodbc is None:
        if logger: logger.info("pyodbc indisponível; pulando persistência SQL.")
        return 0
    cfg = load_config()
    sql_cfg = cfg.get("sql", {})
    if not sql_cfg.get("enable", False):
        if logger: logger.info("sql.enable=false; pulando persistência.")
        return 0

    dfx = df_final[COLS_SQL].copy()
    dfx["Data"] = pd.to_datetime(dfx["Data"], errors="coerce").dt.date
    dfx = dfx.dropna(subset=["Data"])
    dfx["Qtde"] = pd.to_numeric(dfx["Qtde"], errors="coerce").fillna(0).astype(int)
    for c in ["PrecoUnitario", "Receita"]:
        dfx[c] = pd.to_numeric(dfx[c], errors="coerce").fillna(0.0)

    rows = list(map(tuple, dfx.values.tolist()))
    if not rows:
        if logger: logger.info("Nada para persistir no SQL.")
        return 0

    cs = _conn_str(sql_cfg)
    with pyodbc.connect(cs) as conn:
        conn.autocommit = False
        cur = conn.cursor()
        try:
            cur.execute("""
                IF OBJECT_ID('tempdb..#tmp_at') IS NOT NULL DROP TABLE #tmp_at;
                CREATE TABLE #tmp_at(
                  Data date, ClienteId nvarchar(50), Operadora nvarchar(100),
                  Procedimento nvarchar(100), Categoria nvarchar(10),
                  Qtde int, PrecoUnitario decimal(18,2), Receita decimal(18,2)
                );
            """)
            cur.fast_executemany = True
            cur.executemany("""
                INSERT INTO #tmp_at (Data,ClienteId,Operadora,Procedimento,Categoria,Qtde,PrecoUnitario,Receita)
                VALUES (?,?,?,?,?,?,?,?)
            """, rows)
            cur.execute("EXEC app.sp_UpsertAtendimentos;")
            conn.commit()
            if logger:
                logger.info(f"Persistência no SQL concluída via MERGE. Linhas em staging: {len(rows)}.")
            return len(rows)
        except Exception as ex:
            conn.rollback()
            if logger:
                logger.exception(f"Falha ao gravar no SQL (rollback): {ex}")
            raise

# --------------------------------------------------------------------------------------
# Fluxos
# --------------------------------------------------------------------------------------

def update_workbook(df_dados: pd.DataFrame, dim: pd.DataFrame, pv: pd.DataFrame,
                    top_oper: pd.DataFrame, top_proc: pd.DataFrame, cfg: dict, meta: dict,
                    generate_summary_charts: bool = True):
    wb = open_wb()
    try:
        ensure_sheets(wb)
        ws_dados = wb.sheets[SHEET_DADOS]
        ws_dim = wb.sheets[SHEET_DIM]
        ws_rank = wb.sheets[SHEET_RANK]
        ws_aud = wb.sheets[SHEET_AUDIT]

        clear_and_write(ws_dim, dim)
        clear_and_write(ws_dados, df_dados)
        write_rankings_sheet(wb, top_oper, top_proc)
        write_auditoria(ws_aud, cfg, meta)

        if generate_summary_charts:
            support = make_support_table(df_dados, top_n=5)
            generate_dashboard_chart(wb, support)

        wb.save()
    finally:
        app = wb.app
        wb.close()
        app.quit()


def fluxo_atualizar_tudo(logger: logging.Logger):
    logger.info("=== Iniciando atualização completa ===")
    cfg = load_config()

    raw, infos = load_all_data(logger)
    df, m = normalize_and_clean(raw)
    df = apply_filters(df, cfg)
    df, dups = remove_duplicates(df)
    dim = load_dim_clientes()
    df = merge_dim(df, dim)
    df, p90 = compute_percentile_flag(df, "Receita", 0.9)
    pv, top_oper, top_proc = make_pivots_and_rankings(df)

    meta = {
        "arquivos_lidos": len(infos),
        "registros_importados": int(raw.shape[0]),
        "registros_filtrados": int(df.shape[0]),
        "descartes_data": int(m["invalid_dates"]),
        "neg_qtde": int(m["neg_qtde"]),
        "neg_preco": int(m["neg_preco"]),
        "duplicados": int(dups),
        "p90": float(p90),
        "persistidos_sql": 0,
    }

    # Escreve tudo e gera o gráfico único
    update_workbook(df, dim, pv, top_oper, top_proc, cfg, meta, generate_summary_charts=True)

    # Persistência SQL
    try:
        persisted = persist_to_sql_merge(df, logger=logger)
        meta["persistidos_sql"] = int(persisted)
    except Exception:
        pass

    # Atualiza auditoria com o número persistido (sem refazer gráfico)
    update_workbook(df, dim, pv, top_oper, top_proc, cfg, meta, generate_summary_charts=False)
    logger.info("=== Atualização completa finalizada ===")


def fluxo_gerar_graficos(logger: logging.Logger):
    logger.info("=== Gerar gráfico (Resumo) ===")
    wb = open_wb()
    try:
        if SHEET_DADOS not in [s.name for s in wb.sheets]:
            logger.info("Aba Dados inexistente; abortando.")
            return
        df = wb.sheets[SHEET_DADOS].range("A1").options(pd.DataFrame, header=1, index=False, expand="table").value
        if df is None or df.empty:
            logger.info("Aba Dados vazia; abortando.")
            return
        support = make_support_table(df, top_n=5)
        generate_dashboard_chart(wb, support)
        wb.save()
    finally:
        app = wb.app
        wb.close()
        app.quit()
    logger.info("=== Gráfico gerado ===")


def fluxo_gerar_rankings(logger: logging.Logger):
    logger.info("=== Gerar Rankings ===")
    wb = open_wb()
    try:
        if SHEET_DADOS not in [s.name for s in wb.sheets]:
            logger.info("Aba Dados inexistente; abortando.")
            return
        df = wb.sheets[SHEET_DADOS].range("A1").options(pd.DataFrame, header=1, index=False, expand="table").value
        if df is None or df.empty:
            logger.info("Aba Dados vazia; abortando.")
            return
        pv, top_oper, top_proc = make_pivots_and_rankings(df)
        write_rankings_sheet(wb, top_oper, top_proc)
        wb.save()
    finally:
        app = wb.app
        wb.close()
        app.quit()
    logger.info("=== Rankings atualizados ===")

# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------

def main():
    logger = setup_logger()
    parser = argparse.ArgumentParser(description="Agente de Relatórios de Saúde")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("atualizar_tudo", help="Importa/limpa dados, atualiza Excel e persiste no SQL (se habilitado).")
    sub.add_parser("gerar_graficos", help="Recria o gráfico único do Resumo.")
    sub.add_parser("gerar_rankings", help="Recria somente a aba Rankings.")
    args = parser.parse_args()

    if args.cmd == "atualizar_tudo":
        fluxo_atualizar_tudo(logger)
    elif args.cmd == "gerar_graficos":
        fluxo_gerar_graficos(logger)
    elif args.cmd == "gerar_rankings":
        fluxo_gerar_rankings(logger)


if __name__ == "__main__":
    main()
