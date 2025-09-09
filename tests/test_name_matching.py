from pathlib import Path
import importlib.util
import pandas as pd

# Import from payroll_fill.py
SCRIPT = Path("payroll_fill.py")
spec = importlib.util.spec_from_file_location("payroll_fill", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore

def _tmpl(rows):
    return pd.DataFrame(rows, columns=[mod.T_FIRST, mod.T_MI, mod.T_LAST])

def _raw(rows):
    return pd.DataFrame(rows, columns=[mod.C_LAST, mod.C_FIRST])

def test_strict_match_with_mi():
    df_t = _tmpl([
        ["Jane", "A", "Doe"],
    ])
    df_c = _raw([
        ["Doe", "Jane A"],
    ])
    m = mod.match_template_to_csv(df_t, df_c)
    assert (m["_MATCH_TYPE"] == "strict").iloc[0]

def test_loose_match_no_mi():
    df_t = _tmpl([
        ["John", "", "Smith"],
    ])
    df_c = _raw([
        ["Smith", "John"],
    ])
    m = mod.match_template_to_csv(df_t, df_c)
    assert (m["_MATCH_TYPE"] == "loose").iloc[0]

def test_compound_first_name():
    # CSV first name "Mary Ann" (no MI) should match template First="Mary Ann", MI=""
    df_t = _tmpl([
        ["Mary Ann", "", "Brown"],
    ])
    df_c = _raw([
        ["Brown", "Mary Ann"],
    ])
    m = mod.match_template_to_csv(df_t, df_c)
    assert (m["_MATCH_TYPE"] == "loose").iloc[0]

def test_suffix_handling_jr():
    # Template has no suffix, CSV last name includes "Jr"
    df_t = _tmpl([
        ["Alex", "", "Doe"],
    ])
    df_c = _raw([
        ["Doe Jr", "Alex"],
    ])
    m = mod.match_template_to_csv(df_t, df_c)
    # suffix removed → keys equal → loose match
    assert (m["_MATCH_TYPE"] != "unmatched").iloc[0]

def test_unmatched_row():
    df_t = _tmpl([
        ["Zoe", "", "Nope"],
    ])
    df_c = _raw([
        ["Someone", "Else"],
    ])
    m = mod.match_template_to_csv(df_t, df_c)
    assert (m["_MATCH_TYPE"] == "unmatched").iloc[0]
