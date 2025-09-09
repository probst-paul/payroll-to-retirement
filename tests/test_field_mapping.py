from pathlib import Path
import importlib.util
import pandas as pd

SCRIPT = Path("payroll_fill.py")
spec = importlib.util.spec_from_file_location("payroll_fill", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore

def _tmpl(rows):
    return pd.DataFrame(rows, columns=[mod.T_FIRST, mod.T_MI, mod.T_LAST])

def test_field_mapping_and_hours_sum():
    df_t = _tmpl([
        ["Jane", "A", "Doe"],
        ["John", "", "Smith"],
    ])
    # Raw CSV representation: headers must match canonical (post-alias) names
    df_c = pd.DataFrame([
        # Employee Last, Employee First, Reg, OT, PTO, Gross, 401k, Roth, Match
        ["Doe",   "Jane A", "80", "0", "0",    "2,400.00", "$150", "75", "50"],
        ["Smith", "John",   "85", "5", "",     "$2,850",   "200",  "100","60"],
    ], columns=[
        mod.C_LAST, mod.C_FIRST,
        mod.RAW_HRS_REG, mod.RAW_HRS_OT, mod.RAW_HRS_PTO,
        mod.RAW_GROSS_PAY, mod.RAW_PRETAX, mod.RAW_ROTH, mod.RAW_SAFE_HARBOR_NE
    ])

    matched = mod.match_template_to_csv(df_t, df_c)
    filled  = mod.apply_field_mapping(matched)

    # Row 0 (Jane)
    j = filled.iloc[0]
    assert j[mod.T_GROSS_PAY] == 2400.0
    assert j[mod.T_PRETAX] == 150.0
    assert j[mod.T_ROTH] == 75.0
    assert j[mod.T_SAFE_HARBOR_NE] == 50.0
    assert j[mod.T_HOURS_WORKED] == 80.0

    # Row 1 (John)
    s = filled.iloc[1]
    assert s[mod.T_GROSS_PAY] == 2850.0
    assert s[mod.T_PRETAX] == 200.0
    assert s[mod.T_ROTH] == 100.0
    assert s[mod.T_SAFE_HARBOR_NE] == 60.0
    assert s[mod.T_HOURS_WORKED] == 90.0  # 85 + 5 + 0

def test_numeric_coercion_handles_symbols_and_commas():
    vals = ["$1,234.50", " 2,000 ", "", None, "bad"]
    nums = [mod.to_num(v) for v in vals]
    assert nums == [1234.5, 2000.0, 0.0, 0.0, 0.0]