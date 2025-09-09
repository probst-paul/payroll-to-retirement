#!/usr/bin/env python3
import sys, csv, re
from pathlib import Path
import pandas as pd
from typing import Tuple

# ============================================================
# Header detection
# ============================================================

def norm(s: str) -> str:
    return re.sub(r"[^0-9a-z]+", "", str(s).lower())

# Canonical headers we expect
EXPECT_NAMES = [
    "Employee Last Name", "Employee First Name",
    "401k", "Roth 401K", "401K Match 2", "Gross Pay",
    "Regular Hours", "Overtime Hours", "Vacation/PTO Hours",
    "Pay Date",
]

# Aliases for vendor variations
ALIASES = {
    "Roth 401K": ["Roth 401k", "Roth401k", "Roth-401k"],
    "401k": ["401(k)", "401 k", "Pre tax 401k", "Pre-tax 401k"],
    "401K Match 2": ["401k Match2", "401K Match2", "Safe Harbor Non Elective", "Safe Harbor"],
    "Gross Pay": ["Gross", "Gross Wages", "Current Period Compensation"],
    "Regular Hours": ["Reg Hours", "Regular", "Base Hours"],
    "Overtime Hours": ["OT Hours", "Overtime"],
    "Vacation/PTO Hours": ["PTO Hours", "Vacation Hours", "Paid Time Off", "Leave Hours"],
    "Employee First Name": ["Emp First Name", "Employee First", "First"],
    "Employee Last Name": ["Emp Last Name", "Employee Last", "Last"],
    "Pay Date": ["Paydate", "Pay Dt", "Check Date"],
}

def detect_header_row(csv_path: Path, expect_names, sniff_lines=200) -> int:
    """Return 0-based line index of the header row."""
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
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if any(part.strip() for part in line.split(",")):
                    return i
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

# Column names
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
    """
    Two-pass deterministic match:
      1) STRICT (LAST|FIRST|MI) **only if MI exists on both sides**
      2) LOOSE  (LAST|FIRST) for the remaining rows
    Labels rows with _MATCH_TYPE ∈ {'strict','loose','unmatched'}.
    """
    t = prepare_template_names(df_t)
    c = prepare_csv_names(df_c)

    # ---------- PASS 1: STRICT (only where both have MI) ----------
    t_strict = t[t["_T_MI"] != ""]
    c_strict = c[c["_C_MI"] != ""].drop_duplicates("_C_KEY_STRICT")

    t1 = t_strict.merge(
        c_strict,
        how="left",
        left_on="_T_KEY_STRICT",
        right_on="_C_KEY_STRICT",
        suffixes=("_T", "_C"),
    )
    t1["__MT__"] = "strict"

    # ---------- Identify which template rows are NOT done yet ----------
    # Start with all template rows; mark those already matched strictly
    t_all = t.copy()
    t_all["_JOINED"] = False
    if not t1.empty:
        # Build an index of template key rows that matched (regardless of match/no-match on CSV side)
        matched_keys = set(t1["_T_KEY_STRICT"])
        t_all.loc[t_all["_T_KEY_STRICT"].isin(matched_keys), "_JOINED"] = True

    # Remaining rows (either no MI on template or not matched in strict)
    t_rem = t_all[~t_all["_JOINED"]].drop(columns=["_JOINED"])

    # ---------- PASS 2: LOOSE ----------
    c_loose = c.drop_duplicates("_C_KEY_LOOSE")
    fill = t_rem.merge(
        c_loose,
        how="left",
        left_on="_T_KEY_LOOSE",
        right_on="_C_KEY_LOOSE",
        suffixes=("_T", "_C"),
    )
    fill["__MT__"] = "loose"

    # ---------- COMBINE & LABEL ----------
    both = pd.concat([t1, fill], ignore_index=True, sort=False)
    # Default to __MT__ label, then downgrade truly missing to unmatched
    both["_MATCH_TYPE"] = both["__MT__"]
    both.loc[both["_C_LAST"].isna(), "_MATCH_TYPE"] = "unmatched"
    both.drop(columns=["__MT__"], inplace=True)

    return both

# ============================================================
# Main entry
# ============================================================

def main():
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(input("Drag/paste CSV path: ").strip().strip('"'))
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    # Step 1: detect header
    hdr_idx = detect_header_row(csv_path, EXPECT_NAMES + list(ALIASES.keys()))
    print(f"Detected header row at line: {hdr_idx}")

    # Load CSV
    df = pd.read_csv(csv_path, header=0, skiprows=hdr_idx, engine="python", sep=None, dtype=str, encoding="utf-8-sig")
    df = df.loc[:, ~(df.columns.astype(str).str.strip() == "")]

    # Step 2: normalize headers
    alias_map = build_alias_map(EXPECT_NAMES, ALIASES)
    df = rename_by_alias(df, alias_map)

    print("\nColumns parsed (normalized):")
    for c in df.columns:
        print(" -", c)

    print("\nFirst 5 rows:")
    print(df.head(5).to_string(index=False))

    # Step 3: Name matching (with a dummy template just for demo)
    # In real use, load your roster template from templates/roster.csv or .xlsx
    tmpl_path = Path("templates/roster.csv")
    if tmpl_path.exists():
        df_t = pd.read_csv(tmpl_path, dtype=str).fillna("")
        matched = match_template_to_csv(df_t, df)
        print("\n=== Step 3: Name match summary ===")
        print("  strict :", (matched["_MATCH_TYPE"] == "strict").sum())
        print("  loose  :", (matched["_MATCH_TYPE"] == "loose").sum())
        print("  unmatch:", (matched["_MATCH_TYPE"] == "unmatched").sum())
    else:
        print("\n(no template roster found for name matching demo)")

if __name__ == "__main__":
    main()
