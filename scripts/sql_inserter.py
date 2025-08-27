# -*- coding: utf-8 -*-
"""
sql_inserter.py (atualizado)
- Lê CSV/TXT de sample_data/sigsaude e sample_data/operadoras
- Detecta delimitador e encoding
- Normaliza tipos (Data, Qtde, PrecoUnitario, Receita)
- Suporta modos de carga: append | truncate | merge
- MERGE via staging (#tmp_at) + proc app.sp_UpsertAtendimentos (se existir) ou fallback inline
- Usa config.yaml (seção sql)
"""

import argparse
import csv
import glob
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import pyodbc
import yaml

ROOT = Path(__file__).resolve().parents[1]
CFG_PATH = ROOT / "config.yaml"

# Pastas de entrada
SIG_DIR = ROOT / "sample_data" / "sigsaude"
OP_DIR  = ROOT / "sample_data" / "operadoras"

# ----------------------------------------------------------------------
# Utilitários
# ----------------------------------------------------------------------

def load_cfg() -> dict:
    with open(CFG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def conn_str(c: dict) -> str:
    driver = c.get("driver", "ODBC Driver 18 for SQL Server")
    server = c.get("server", "localhost,1433")
    db     = c.get("database", "SaudeDev")
    uid    = c.get("username", "sa")
    pwd    = c.get("password", "")
    encrypt= "yes" if c.get("encrypt", False) else "no"
    tsc    = "yes" if c.get("trust_server_certificate", True) else "no"
    return f"DRIVER={{{driver}}};SERVER={server};DATABASE={db};UID={uid};PWD={pwd};Encrypt={encrypt};TrustServerCertificate={tsc}"

def detect_delimiter_and_encoding(p: Path) -> Tuple[str, str]:
    # Tenta utf-8, senão latin-1; delim por inspeção da primeira linha
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

def read_table(path: Path) -> List[dict]:
    delim, enc = detect_delimiter_and_encoding(path)
    rows = []
    with open(path, "r", encoding=enc, errors="ignore", newline="") as f:
        r = csv.DictReader(f, delimiter=delim)
        for row in r:
            rows.append(row)
    return rows

def to_float(v):
    if v in (None, "", "NA", "NaN"):
        return 0.0
    try:
        return float(str(v).replace(",", "."))
    except Exception:
        return 0.0

def to_int(v):
    try:
        return int(float(str(v).replace(",", ".")))
    except Exception:
        return 0

def to_date(v):
    if not v:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(v, fmt).date()
        except Exception:
            pass
    return None

# ----------------------------------------------------------------------
# Persistência
# ----------------------------------------------------------------------

TMP_TABLE_SQL = """
IF OBJECT_ID('tempdb..#tmp_at') IS NOT NULL DROP TABLE #tmp_at;
CREATE TABLE #tmp_at(
  Data date,
  ClienteId nvarchar(50),
  Operadora nvarchar(100),
  Procedimento nvarchar(100),
  Categoria nvarchar(10),
  Qtde int,
  PrecoUnitario decimal(18,2),
  Receita decimal(18,2)
);
"""

MERGE_FALLBACK_SQL = """
MERGE SaudeDev.app.Atendimentos AS t
USING #tmp_at AS s
  ON  t.Data = s.Data
  AND t.ClienteId = s.ClienteId
  AND t.Procedimento = s.Procedimento
WHEN MATCHED THEN
  UPDATE SET
    t.Operadora     = s.Operadora,
    t.Categoria     = s.Categoria,
    t.Qtde          = s.Qtde,
    t.PrecoUnitario = s.PrecoUnitario,
    t.Receita       = s.Receita,
    t.LoadDate      = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN
  INSERT (Data, ClienteId, Operadora, Procedimento, Categoria, Qtde, PrecoUnitario, Receita, LoadDate)
  VALUES (s.Data, s.ClienteId, s.Operadora, s.Procedimento, s.Categoria, s.Qtde, s.PrecoUnitario, s.Receita, SYSDATETIME());
"""

def sp_exists(cur, schema: str, proc_name: str) -> bool:
    cur.execute("""
        SELECT 1
        FROM sys.objects
        WHERE object_id = OBJECT_ID(?)
          AND type = 'P';
    """, f"{schema}.{proc_name}")
    return cur.fetchone() is not None

def persist_append(conn, records: List[tuple]) -> int:
    cur = conn.cursor()
    cur.fast_executemany = True
    cur.executemany("""
        INSERT INTO SaudeDev.app.Atendimentos
        (Data, ClienteId, Operadora, Procedimento, Categoria, Qtde, PrecoUnitario, Receita)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    return len(records)

def persist_truncate_then_append(conn, records: List[tuple]) -> int:
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE SaudeDev.app.Atendimentos;")
    return persist_append(conn, records)

def persist_merge(conn, records: List[tuple]) -> int:
    """
    Cria #tmp_at, insere registros, e realiza MERGE.
    Se app.sp_UpsertAtendimentos existir, chama a proc; senão usa fallback inline.
    """
    cur = conn.cursor()
    cur.execute(TMP_TABLE_SQL)
    cur.fast_executemany = True
    cur.executemany("""
        INSERT INTO #tmp_at
        (Data, ClienteId, Operadora, Procedimento, Categoria, Qtde, PrecoUnitario, Receita)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)

    if sp_exists(cur, "app", "sp_UpsertAtendimentos"):
        cur.execute("EXEC app.sp_UpsertAtendimentos;")
    else:
        cur.execute(MERGE_FALLBACK_SQL)
    return len(records)

# ----------------------------------------------------------------------
# Pipeline principal
# ----------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Inserção/Upsert de atendimentos no SQL Server")
    parser.add_argument("--mode", choices=["append", "truncate", "merge"], default=None,
                        help="Modo de carga (sobrepõe sql.load_mode do config.yaml)")
    args = parser.parse_args()

    cfg = load_cfg()
    sql_cfg = cfg.get("sql", {})
    if not sql_cfg.get("enable", False):
        raise SystemExit("sql.enable está falso no config.yaml")

    mode = (args.mode or sql_cfg.get("load_mode") or "append").lower()

    # Coleta arquivos
    all_files = []
    if SIG_DIR.exists(): all_files += glob.glob(str(SIG_DIR / "*.*"))
    if OP_DIR.exists():  all_files += glob.glob(str(OP_DIR / "*.*"))

    if not all_files:
        print("Sem arquivos em sample_data/. Nada a inserir.")
        return

    # Lê/normaliza para a tabela
    records = []
    lidas = 0
    for fp in all_files:
        rows = read_table(Path(fp))
        lidas += len(rows)
        for r in rows:
            data = to_date(r.get("Data"))
            if not data:
                continue
            cliente = (r.get("ClienteId") or "").strip()
            oper    = (r.get("Operadora") or "N/A").strip()
            proc    = (r.get("Procedimento") or "N/A").strip()
            cat     = (r.get("Categoria") or "N/A").strip()
            qtde    = max(0, to_int(r.get("Qtde")))
            pu      = max(0.0, to_float(r.get("PrecoUnitario")))
            rec     = to_float(r.get("Receita")) if r.get("Receita") not in (None, "", "NA") else qtde * pu
            records.append((data, cliente, oper, proc, cat, qtde, pu, rec))

    if not records:
        print("Sem registros válidos para inserir (todas as datas inválidas?).")
        return

    cs = conn_str(sql_cfg)
    with pyodbc.connect(cs) as conn:
        conn.autocommit = False
        try:
            if mode == "truncate":
                n = persist_truncate_then_append(conn, records)
            elif mode == "merge":
                n = persist_merge(conn, records)
            else:
                n = persist_append(conn, records)

            conn.commit()
            print(f"OK: Linhas lidas={lidas}, linhas consideradas={len(records)}, linhas persistidas={n} (modo={mode}).")

        except Exception as ex:
            conn.rollback()
            # Dica útil: se houver índice único (chave natural) e você usar append, pode dar erro de duplicidade.
            print(f"ERRO (rollback feito): {ex}\n"
                  f"Dica: se houver duplicidade, rode com --mode merge ou clean com --mode truncate.")
            raise

if __name__ == "__main__":
    main()
