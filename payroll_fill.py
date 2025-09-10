#!/usr/bin/env python3
import sys, csv, re, shutil
from pathlib import Path
import pandas as pd
from typing import Tuple
from datetime import datetime

# ============================================================
# Header detection
# ============================================================

def norm(s: str) -> str:
    return re.sub(r"[^0-9a-z]+", "", str(s).lower())

EXPECT_NAMES = [
    "Employee Last Name", "Employee First Name",
    "401k", "Roth 401K", "401K Match 2", "Gross Pay",
    "Regular Hours", "Overtime Hours", "Vacation/PTO Hours",
    "Pay Date",
]

ALIASES = {
    "Roth 401K": ["Roth 401k", "Roth401k", "Roth-401k"],
    "401k": ["401(k)", "401 k", "Pre tax 401k", "Pre-tax 401k", "401K"],
    "401k Catchup": ["401k Catch-up", "401(k) Catchup", "Pre-Tax Catchup", "Pre Tax Catchup", "Pre-tax Catchup"],
    "Roth Catchup": ["Roth 401k Catchup", "Roth Catch-up", "Roth Catch up"],
    "401K Match 2": ["401k Match2", "401K Match2", "Safe Harbor Non Elective", "Safe Harbor", "Safe Harbor Match"],
    "Gross Pay": ["Gross", "Gross Wages", "Current Period Compensation"],
    "Regular Hours": ["Reg Hours", "Base Hours"],
    "Overtime Hours": ["OT Hours"],
    "Vacation/PTO Hours": ["PTO Hours", "Vacation Hours", "Paid Time Off", "Leave Hours"],
    "Employee First Name": ["Emp First Name", "Employee First", "First"],
    "Employee Last Name": ["Emp Last Name", "Employee Last", "Last"],
    "Pay Date": ["Paydate", "Pay Dt", "Check Date"],
}

def detect_header_row(csv_path: Path, expect_names, sniff_lines=200) -> int:
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(8192)
        try:
            dialect = csv.Sniffer().sniff(sample)
            delim = dialect.delimiter
        except Exception:
            delim = ","
    exp = set(norm(x) for x in expect_names)
    best_hits, best_div, best_idx = -1, -1, None
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        for i, line in enumerate(f):
            if i >= sniff_lines: break
            cells = [c.strip() for c in line.rstrip("\r\n").split(delim)]
            cellsn = [norm(c) for c in cells if c.strip()]
            hits = sum(1 for c in cellsn if c in exp)
            div  = len(set(cellsn))
            if (hits, div) > (best_hits, best_div):
                best_hits, best_div, best_idx = hits, div, i
    if best_idx is None:
        raise RuntimeError("Could not detect a header row.")
    return best_idx

def build_alias_map(canonical_names, aliases_cfg):
    amap = {}
    for canon in canonical_names:
        amap[canon] = list({norm(canon), *[norm(a) for a in aliases_cfg.get(canon, [])]})
    for k in aliases_cfg:
        amap.setdefault(k, []).append(norm(k))
        amap[k] = list(set(amap[k]))
    return amap

def rename_by_alias(df: pd.DataFrame, alias_map):
    current = {norm(c): c for c in df.columns}
    rename = {}
    for canon, variants in alias_map.items():
        for v in variants:
            if v in current:
                rename[current[v]] = canon
                break
    return df.rename(columns=rename)

# ============================================================
# Name parsing & matching
# ============================================================

T_FIRST  = "First Name"
T_MI     = "MI"
T_LAST   = "Last Name"
C_FIRST  = "Employee First Name"
C_LAST   = "Employee Last Name"

_SUFFIXES = {"JR","SR","II","III","IV","V"}

def _clean_token(s: str) -> str:
    if s is None: return ""
    s = " ".join(str(s).strip().split())
    s = re.sub(r"[.,]", "", s)
    return s

def _strip_suffix(last: str) -> str:
    if not last: return ""
    parts = _clean_token(last).split()
    if parts and parts[-1].upper() in _SUFFIXES:
        parts = parts[:-1]
    return " ".join(parts)

def _norm_key_part(s: str) -> str:
    s = _clean_token(s).upper()
    return re.sub(r"[\s'\-]", "", s)

def _extract_first_and_mi_from_csv(first_field: str) -> Tuple[str, str]:
    s = _clean_token(first_field)
    if not s: return "", ""
    tokens = s.split()
    first = tokens[0]
    mi = ""
    if len(tokens) >= 2:
        cand = tokens[1]
        if re.fullmatch(r"[A-Za-z]\.?", cand):
            mi = cand[0].upper()
        else:
            first = " ".join(tokens)
    return first, mi

def prepare_template_names(df_t: pd.DataFrame) -> pd.DataFrame:
    out = df_t.copy()
    out["_T_FIRST"] = out.get(T_FIRST, "").astype(str).map(_clean_token)
    out["_T_MI"]    = out.get(T_MI, "").astype(str).map(lambda x: x[:1].upper() if x else "")
    out["_T_LAST"]  = out.get(T_LAST, "").astype(str).map(_strip_suffix)
    out["_T_KEY_LOOSE"]  = out["_T_LAST"].map(_norm_key_part) + "|" + out["_T_FIRST"].map(_norm_key_part)
    out["_T_KEY_STRICT"] = out["_T_KEY_LOOSE"] + "|" + out["_T_MI"]
    return out

def prepare_csv_names(df_c: pd.DataFrame) -> pd.DataFrame:
    out = df_c.copy()
    last = out.get(C_LAST, "").astype(str).map(_strip_suffix)
    first_raw = out.get(C_FIRST, "").astype(str)
    parsed = first_raw.map(_extract_first_and_mi_from_csv)
    out["_C_FIRST"] = parsed.map(lambda x: _clean_token(x[0]))
    out["_C_MI"]    = parsed.map(lambda x: x[1])
    out["_C_LAST"]  = last
    out["_C_KEY_LOOSE"]  = out["_C_LAST"].map(_norm_key_part) + "|" + out["_C_FIRST"].map(_norm_key_part)
    out["_C_KEY_STRICT"] = out["_C_KEY_LOOSE"] + "|" + out["_C_MI"]
    return out

def match_template_to_csv(df_t: pd.DataFrame, df_c: pd.DataFrame) -> pd.DataFrame:
    t = prepare_template_names(df_t)
    c = prepare_csv_names(df_c)
    t_strict = t[t["_T_MI"] != ""]
    c_strict = c[c["_C_MI"] != ""].drop_duplicates("_C_KEY_STRICT")
    t1 = t_strict.merge(c_strict, how="left", left_on="_T_KEY_STRICT", right_on="_C_KEY_STRICT", suffixes=("_T", "_C"))
    t1["__MT__"] = "strict"
    t_all = t.copy()
    t_all["_JOINED"] = False
    if not t1.empty:
        matched_keys = set(t1["_T_KEY_STRICT"])
        t_all.loc[t_all["_T_KEY_STRICT"].isin(matched_keys), "_JOINED"] = True
    t_rem = t_all[~t_all["_JOINED"]].drop(columns=["_JOINED"])
    c_loose = c.drop_duplicates("_C_KEY_LOOSE")
    fill = t_rem.merge(c_loose, how="left", left_on="_T_KEY_LOOSE", right_on="_C_KEY_LOOSE", suffixes=("_T","_C"))
    fill["__MT__"] = "loose"
    both = pd.concat([t1, fill], ignore_index=True, sort=False)
    both["_MATCH_TYPE"] = both["__MT__"]
    both.loc[both["_C_LAST"].isna(), "_MATCH_TYPE"] = "unmatched"
    both.drop(columns=["__MT__"], inplace=True)
    return both

# ============================================================
# Field Mapping
# ============================================================

RAW_PRETAX            = "401k"
RAW_PRETAX_CATCHUP    = "401k Catchup"
RAW_ROTH              = "Roth 401K"
RAW_ROTH_CATCHUP      = "Roth Catchup"
RAW_SAFE_HARBOR_NE    = "401K Match 2"
RAW_GROSS             = "Gross Pay"          # existing
RAW_GROSS_PAY         = RAW_GROSS            # <-- compat for tests expecting RAW_GROSS_PAY
RAW_HRS_REG           = "Regular Hours"
RAW_HRS_OT            = "Overtime Hours"
RAW_HRS_PTO           = "Vacation/PTO Hours"
RAW_PAYDATE           = "Pay Date"


T_PRETAX              = "Pretax"
T_PRETAX_CU           = "Pre-Tax Catchup"
T_ROTH                = "Roth"
T_ROTH_CU             = "Roth Catchup"
T_SAFE_HARBOR_NE      = "Safe Harbor Non-Elective"
T_SAFEHARB            = T_SAFE_HARBOR_NE
T_COMP                = "Current Period Compensation"
T_GROSS_PAY           = "Current Period Compensation"  # <-- add this line (compat with tests)
T_COMP                = T_GROSS_PAY                    # keep internal alias if you still use T_COMP
T_HOURS_WORKED        = "Current Period Hours Worked"
T_CHECKDATE           = "Check Date"

def to_num(x) -> float:
    """Coerce currency/number-like strings to float. '$1,234.50 ' -> 1234.5; blanks/None/bad -> 0.0"""
    if pd.isna(x):
        return 0.0
    s = str(x).strip()
    if s == "":
        return 0.0
    s = re.sub(r"[,\$\%\s]", "", s)
    try:
        return float(s)
    except Exception:
        return 0.0


def _ensure_series(s):
    if isinstance(s, pd.DataFrame):
        return s.iloc[:, 0]
    return s

def apply_field_mapping(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in [RAW_HRS_REG, RAW_HRS_OT, RAW_HRS_PTO, RAW_PRETAX, RAW_PRETAX_CATCHUP,
                RAW_ROTH, RAW_ROTH_CATCHUP, RAW_SAFE_HARBOR_NE, RAW_GROSS, RAW_PAYDATE]:
        if col not in out.columns:
            out[col] = 0.0
    out[T_PRETAX]    = _ensure_series(out[RAW_PRETAX]).map(to_num)
    out[T_PRETAX_CU] = _ensure_series(out[RAW_PRETAX_CATCHUP]).map(to_num)
    out[T_ROTH]      = _ensure_series(out[RAW_ROTH]).map(to_num)
    out[T_ROTH_CU]   = _ensure_series(out[RAW_ROTH_CATCHUP]).map(to_num)
    out[T_SAFEHARB]  = _ensure_series(out[RAW_SAFE_HARBOR_NE]).map(to_num)
    out[T_COMP]      = _ensure_series(out[RAW_GROSS]).map(to_num)
    reg = _ensure_series(out[RAW_HRS_REG]).map(to_num)
    ot  = _ensure_series(out[RAW_HRS_OT]).map(to_num)
    pto = _ensure_series(out[RAW_HRS_PTO]).map(to_num)
    out[T_HOURS_WORKED] = reg + ot + pto
    out[T_CHECKDATE]    = out[RAW_PAYDATE]
    return out

# ============================================================
# Main entry
# ============================================================

def main():
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(input("Drag/paste CSV path: ").strip().strip('"'))
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    hdr_idx = detect_header_row(csv_path, EXPECT_NAMES + list(ALIASES.keys()))
    df = pd.read_csv(csv_path, header=0, skiprows=hdr_idx, engine="python", sep=None, dtype=str, encoding="utf-8-sig")
    df = df.loc[:, ~(df.columns.astype(str).str.strip() == "")]
    alias_map = build_alias_map(EXPECT_NAMES, ALIASES)
    df = rename_by_alias(df, alias_map)
    df = df.loc[:, ~df.columns.duplicated()]
    df_in = df.copy()

    # Verification
    def col_sum(df_, name):
        return pd.to_numeric(df_.get(name, pd.Series(dtype=float)), errors="coerce").fillna(0).map(to_num).sum()

    reg = col_sum(df_in, RAW_HRS_REG)
    ot  = col_sum(df_in, RAW_HRS_OT)
    pto = col_sum(df_in, RAW_HRS_PTO)
    pretax     = col_sum(df_in, RAW_PRETAX)
    pretax_cu  = col_sum(df_in, RAW_PRETAX_CATCHUP)
    roth       = col_sum(df_in, RAW_ROTH)
    roth_cu    = col_sum(df_in, RAW_ROTH_CATCHUP)
    safeharbor = col_sum(df_in, RAW_SAFE_HARBOR_NE)
    grand_total_hours = reg + ot + pto
    checksum = pretax + pretax_cu + roth + roth_cu + safeharbor

    print("\n=== Verification: Totals from INCOMING CSV ===")
    print(f"  {RAW_HRS_REG:<22}: {reg:,.2f}")
    print(f"  {RAW_HRS_OT:<22}: {ot:,.2f}")
    print(f"  {RAW_HRS_PTO:<22}: {pto:,.2f}")
    print(f"  {'GRAND TOTAL HOURS':<22}: {grand_total_hours:,.2f}")
    print("\n=== Checksum (INCOMING CSV) ===")
    print(f"  CHECKSUM: ${checksum:,.2f}")

    confirm = input("\nProceed with this batch? (verify totals above) [Y/n]: ").strip().lower()
    if confirm not in ("", "y", "yes"):
        print("Aborting per user choice. No files were written.")
        return

    # Apply mapping
    filled = apply_field_mapping(df_in)

    # Archive incoming file
    archive_dir = Path("data/archive")
    archive_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    shutil.copy2(csv_path, archive_dir / f"{csv_path.stem}-{ts}{csv_path.suffix}")

    # Write one output per Pay Date
    dist_dir = Path("dist")
    dist_dir.mkdir(parents=True, exist_ok=True)
    if RAW_PAYDATE not in filled.columns:
        print("❌ No Pay Date column found in incoming CSV.")
        return
    for pay_date, group in filled.groupby(RAW_PAYDATE):
        try:
            dt = datetime.strptime(str(pay_date).strip(), "%Y-%m-%d").date()
        except Exception:
            dt = datetime.now().date()
        out_name = f"PayrollUpload-{dt.isoformat()}.csv"
        out_path = dist_dir / out_name
        group.to_csv(out_path, index=False)
        print(f"✅ Wrote {len(group)} rows to {out_path}")

    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
