#!/usr/bin/env python3
import sys, csv, re, shutil, os
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
    "401k", "401k Catchup", "Roth 401K", "Roth Catchup", "401K Match 2", "Gross Pay",
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
    # IMPORTANT: do NOT alias money fields "Regular"/"Overtime" to hours
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
# Name parsing & matching (Template <-> CSV)
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

# Raw CSV columns (post-alias)
RAW_PRETAX            = "401k"
RAW_PRETAX_CATCHUP    = "401k Catchup"
RAW_ROTH              = "Roth 401K"
RAW_ROTH_CATCHUP      = "Roth Catchup"
RAW_SAFE_HARBOR_NE    = "401K Match 2"
RAW_GROSS             = "Gross Pay"
RAW_GROSS_PAY         = RAW_GROSS
RAW_HRS_REG           = "Regular Hours"
RAW_HRS_OT            = "Overtime Hours"
RAW_HRS_PTO           = "Vacation/PTO Hours"
RAW_PAYDATE           = "Pay Date"

# Template/output column names
T_PRETAX              = "Pretax"
T_PRETAX_CU           = "Pre-Tax Catchup"
T_ROTH                = "Roth"
T_ROTH_CU             = "Roth Catchup"
T_SAFE_HARBOR_NE      = "Safe Harbor Non-Elective"
T_SAFEHARB            = T_SAFE_HARBOR_NE
T_COMP                = "Current Period Compensation"
T_GROSS_PAY           = "Current Period Compensation"  # compat alias
T_HOURS_WORKED        = "Current Period Hours Worked"
T_CHECKDATE           = "Check Date"

# Final upload column order (as requested)
FINAL_COLUMNS = [
    "SSN","First Name","MI","Last Name","Check Date",
    "Pretax","Pre-Tax Catchup","Roth","Roth Catchup","Safe Harbor Non-Elective",
    "Current Period Compensation","Current Period Hours Worked",
    "Address 1","Address 2","City","State","Zip",
    "Date of Birth","Date of Hire","Date of Term","Rehire Date",
    "Email Address","Profit Share",
]

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
    """
    Expects a DataFrame that already has roster (template) columns + CSV columns
    (i.e., the result of match_template_to_csv(...), merged).
    Fills dynamic fields from raw CSV columns, preserves static fields from roster.
    """
    out = df.copy()
    for col in [RAW_HRS_REG, RAW_HRS_OT, RAW_HRS_PTO, RAW_PRETAX, RAW_PRETAX_CATCHUP,
                RAW_ROTH, RAW_ROTH_CATCHUP, RAW_SAFE_HARBOR_NE, RAW_GROSS, RAW_PAYDATE]:
        if col not in out.columns:
            out[col] = 0.0

    # Numeric field mapping
    out[T_PRETAX]        = _ensure_series(out[RAW_PRETAX]).map(to_num)
    out[T_PRETAX_CU]     = _ensure_series(out[RAW_PRETAX_CATCHUP]).map(to_num)
    out[T_ROTH]          = _ensure_series(out[RAW_ROTH]).map(to_num)
    out[T_ROTH_CU]       = _ensure_series(out[RAW_ROTH_CATCHUP]).map(to_num)
    out[T_SAFEHARB]      = _ensure_series(out[RAW_SAFE_HARBOR_NE]).map(to_num)
    out[T_COMP]          = _ensure_series(out[RAW_GROSS]).map(to_num)

    # Hours sum = Reg + OT + PTO (PTO may be missing)
    reg = _ensure_series(out[RAW_HRS_REG]).map(to_num) if RAW_HRS_REG in out.columns else 0.0
    ot  = _ensure_series(out[RAW_HRS_OT]).map(to_num)  if RAW_HRS_OT  in out.columns else 0.0
    pto = _ensure_series(out[RAW_HRS_PTO]).map(to_num) if RAW_HRS_PTO in out.columns else 0.0
    out[T_HOURS_WORKED] = reg + ot + pto

    # Check Date = Pay Date from raw
    out[T_CHECKDATE] = out[RAW_PAYDATE]

    return out

# ============================================================
# Roster location helper
# ============================================================

def find_roster_path() -> Path | None:
    # 1) Environment override
    rp = os.environ.get("ROSTER_PATH")
    if rp:
        p = Path(rp)
        if p.exists():
            return p

    # 2) cwd/templates/roster.csv
    p = Path("templates/roster.csv")
    if p.exists():
        return p

    # 3) script_dir/templates/roster.csv
    try:
        script_dir = Path(__file__).resolve().parent
        p = script_dir / "templates" / "roster.csv"
        if p.exists():
            return p
    except Exception:
        pass

    return None

# ============================================================
# Main
# ============================================================

def main():
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(input("Drag/paste CSV path: ").strip().strip('"'))
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    # Step 1: detect header & load
    hdr_idx = detect_header_row(csv_path, EXPECT_NAMES + list(ALIASES.keys()))
    df = pd.read_csv(csv_path, header=0, skiprows=hdr_idx, engine="python", sep=None, dtype=str, encoding="utf-8-sig")
    df = df.loc[:, ~(df.columns.astype(str).str.strip() == "")]
    # Step 2: alias normalization (+ safety duplicate drop)
    alias_map = build_alias_map(EXPECT_NAMES, ALIASES)
    df = rename_by_alias(df, alias_map)
    df = df.loc[:, ~df.columns.duplicated()]
    df_in = df.copy()

    print(f"Detected header row at line: {hdr_idx}")
    print("\nColumns parsed (normalized):")
    for c in df_in.columns:
        print(" -", c)

    # Step 3: verification on INCOMING CSV (pre-roster)
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

    # ---- Archive incoming file EARLY so tests expecting archive still pass
    archive_dir = Path("data/archive")
    archive_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    shutil.copy2(csv_path, archive_dir / f"{csv_path.stem}-{ts}{csv_path.suffix}")

    # Step 4: load roster template & match
    tmpl_path = find_roster_path()
    if tmpl_path is None:
        print("❌ templates/roster.csv not found. Set ROSTER_PATH or place roster under ./templates/.")
        return
    roster = pd.read_csv(tmpl_path, dtype=str).fillna("")

    matched = match_template_to_csv(roster, df_in)

    # Some visibility
    print("\n=== Name match summary ===")
    print("  strict :", (matched["_MATCH_TYPE"] == "strict").sum())
    print("  loose  :", (matched["_MATCH_TYPE"] == "loose").sum())
    print("  unmatch:", (matched["_MATCH_TYPE"] == "unmatched").sum())

    # Step 5: map dynamic fields onto matched rows
    filled = apply_field_mapping(matched)

    # Step 6: keep only employees with activity (strict/loose matches)
    active = filled[filled["_MATCH_TYPE"].isin(["strict", "loose"])].copy()

    # Step 7: build final upload frame in required order (fill missing columns with blanks)
    for col in FINAL_COLUMNS:
        if col not in active.columns:
            active[col] = ""

    upload = active[FINAL_COLUMNS].copy()

    # Step 9: write one output per Check/Pay Date
    dist_dir = Path("dist")
    dist_dir.mkdir(parents=True, exist_ok=True)

    if T_CHECKDATE not in upload.columns:
        print("❌ No Check Date found after mapping.")
        return

    groups = upload.groupby(T_CHECKDATE, dropna=False)
    written = []
    for check_date, group in groups:
        # Normalize/parse date string
        try:
            dt = datetime.strptime(str(check_date).strip(), "%Y-%m-%d").date()
            date_str = dt.isoformat()
        except Exception:
            # If bad/missing date, bucket under 'unknown'
            date_str = "unknown"

        out_name = f"PayrollUpload-{date_str}.csv"
        out_path = dist_dir / out_name
        group.to_csv(out_path, index=False)
        written.append((out_path, len(group)))

    print("")
    for p, n in written:
        print(f"✅ Wrote {n} rows to {p}")

    # Optional: write a small unmatched report
    um = filled[filled["_MATCH_TYPE"] == "unmatched"]
    if not um.empty:
        rpt = dist_dir / f"unmatched-{ts}.csv"
        cols = ["First Name","MI","Last Name","Employee Last Name","Employee First Name","_MATCH_TYPE"]
        keep_cols = [c for c in cols if c in um.columns]
        um.to_csv(rpt, index=False, columns=keep_cols)
        print(f"⚠️  Unmatched roster rows: {len(um)} (details: {rpt})")

    input("\nPress Enter to exit...")

if __name__ == "__main__":
    main()
