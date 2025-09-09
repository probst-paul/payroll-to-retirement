#!/usr/bin/env python3
import sys, csv, re
from pathlib import Path
import pandas as pd

def norm(s: str) -> str:
    return re.sub(r"[^0-9a-z]+", "", str(s).lower())

# For now, list only the names we expect to see somewhere in the header row.
EXPECT_NAMES = [
    "Employee Last Name", "Employee First Name",
    "401k", "Roth 401K", "401K Match 2", "Gross Pay",
    "Regular Hours", "Overtime Hours", "Vacation/PTO Hours",
    "Pay Date",
]

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
        # Fallback: first non-empty row
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            for i, line in enumerate(f):
                if any(part.strip() for part in line.split(",")):
                    return i
        raise RuntimeError("Could not detect a header row.")
    return best_idx

def main():
    # Accept drag-and-drop (Windows) or prompt if no argv
    csv_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(input("Drag/paste CSV path: ").strip().strip('"'))
    if not csv_path.exists():
        print(f"❌ File not found: {csv_path}")
        return

    hdr_idx = detect_header_row(csv_path, EXPECT_NAMES)
    print(f"Detected header row at line: {hdr_idx}")

    df = pd.read_csv(csv_path, header=0, skiprows=hdr_idx, engine="python", sep=None, dtype=str, encoding="utf-8-sig")
    # Drop empty-named columns that sometimes appear
    df = df.loc[:, ~(df.columns.astype(str).str.strip() == "")]
    print("\nColumns parsed:")
    for c in df.columns:
        print(" -", c)
    print("\nFirst 5 rows:")
    print(df.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
