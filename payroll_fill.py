#!/usr/bin/env python3
import sys, csv, re
from pathlib import Path
import pandas as pd

def norm(s: str) -> str:
    return re.sub(r"[^0-9a-z]+", "", str(s).lower())

# Canonical header fields we care about (used for detection & after-normalization)
EXPECT_NAMES = [
    "Employee Last Name", "Employee First Name",
    "401k", "Roth 401K", "401K Match 2", "Gross Pay",
    "Regular Hours", "Overtime Hours", "Vacation/PTO Hours",
    "Pay Date",
]

# NEW: common variants to normalize → canonical keys in EXPECT_NAMES
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

# NEW: build an alias map from canonical → list of normalized variants
def build_alias_map(canonical_names, aliases_cfg):
    amap = {}
    for canon in canonical_names:
        amap[canon] = list({norm(canon), *[norm(a) for a in aliases_cfg.get(canon, [])]})
    # also allow alias keys themselves to map to their own normalized token
    for k in aliases_cfg:
        amap.setdefault(k, []).append(norm(k))
        amap[k] = list(set(amap[k]))
    return amap

# NEW: apply alias map to a dataframe’s columns
def rename_by_alias(df: pd.DataFrame, alias_map):
    current = {norm(c): c for c in df.columns}
    rename = {}
    for canon, variants in alias_map.items():
        for v in variants:
            if v in current:
                rename[current[v]] = canon
                break
    return df.rename(columns=rename)

def main():
    # Accept drag/drop (Windows) or prompt if no argv
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(input("Drag/paste CSV path: ").strip().strip('"'))
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    hdr_idx = detect_header_row(csv_path, EXPECT_NAMES + list(ALIASES.keys()))
    print(f"Detected header row at line: {hdr_idx}")

    df = pd.read_csv(csv_path, header=0, skiprows=hdr_idx, engine="python", sep=None, dtype=str, encoding="utf-8-sig")
    # Drop empty-named columns
    df = df.loc[:, ~(df.columns.astype(str).str.strip() == "")]

    # NEW: normalize headers using aliases
    alias_map = build_alias_map(EXPECT_NAMES, ALIASES)
    df = rename_by_alias(df, alias_map)

    print("\nColumns parsed (normalized):")
    for c in df.columns:
        print(" -", c)

    print("\nFirst 5 rows:")
    print(df.head(5).to_string(index=False))

if __name__ == "__main__":
    main()