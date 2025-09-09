from pathlib import Path
import importlib.util
import pandas as pd

# Load functions from payroll_fill.py without changing the script
SCRIPT = Path("payroll_fill.py")
spec = importlib.util.spec_from_file_location("payroll_fill", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore

def test_aliases_map_to_canonical(tmp_path: Path):
    # Make a tiny CSV using some variant headers
    csv_text = (
        "Emp Last Name,Emp First Name,Roth 401k,401(k),PTO Hours,Gross,Paydate\n"
        "Doe,Jane A,75,150,0,2400,2025-09-05\n"
    )
    p = tmp_path / "alias_sample.csv"
    p.write_text(csv_text, encoding="utf-8")

    # Read
    hdr_idx = mod.detect_header_row(p, mod.EXPECT_NAMES + list(mod.ALIASES.keys()))
    df = pd.read_csv(p, header=0, skiprows=hdr_idx, engine="python", sep=None, dtype=str, encoding="utf-8-sig")
    df = df.loc[:, ~(df.columns.astype(str).str.strip() == "")]

    # Normalize
    amap = mod.build_alias_map(mod.EXPECT_NAMES, mod.ALIASES)
    df2 = mod.rename_by_alias(df, amap)

    # Assert canonical names exist post-normalization
    assert "Employee Last Name" in df2.columns
    assert "Employee First Name" in df2.columns
    assert "Roth 401K" in df2.columns
    assert "401k" in df2.columns
    assert "Vacation/PTO Hours" in df2.columns
    assert "Gross Pay" in df2.columns
    assert "Pay Date" in df2.columns
