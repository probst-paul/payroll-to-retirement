# tests/test_ouputs.py
from pathlib import Path
import importlib.util
import pandas as pd   # ✅ add this

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "payroll_fill.py"

def load_script():
    spec = importlib.util.spec_from_file_location("payroll_fill", SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod

def test_step6_writes_outputs_and_archives(tmp_path, monkeypatch):
    # Work in a clean temp directory so dist/ and data/archive/ are isolated
    monkeypatch.chdir(tmp_path)

    # --- NEW: create a matching roster and point ROSTER_PATH at it ---
    tmpl_dir = tmp_path / "templates"
    tmpl_dir.mkdir(parents=True, exist_ok=True)

    roster_cols = [
        "SSN","First Name","MI","Last Name",
        "Address 1","Address 2","City","State","Zip",
        "Date of Birth","Date of Hire","Date of Term","Rehire Date",
        "Email Address","Profit Share"
    ]
    roster_df = pd.DataFrame([
        ["111-22-3333","Jane","A","Doe","100 Main St","","Springfield","IL","62701","","","","","jane@example.com",""],
        ["222-33-4444","John","","Smith","200 Oak Ave","","Shelbyville","IL","62565","","","","","john@example.com",""],
        ["333-44-5555","Emily","R","Johnson","300 Pine Rd","","Capital City","IL","62702","","","","","emily@example.com",""],
    ], columns=roster_cols)
    roster_path = tmpl_dir / "roster.csv"
    roster_df.to_csv(roster_path, index=False, encoding="utf-8")

    # Point the script to this roster
    monkeypatch.setenv("ROSTER_PATH", str(roster_path))

    # Create a minimal input with 2 pay dates
    src = tmp_path / "in.csv"
    src.write_text(
        ",".join([
            "Pay Date","Transaction Date","Pay Period","Source","Paycheck #","Location Name",
            "Employee Last Name","Employee First Name",
            "Regular Hours","Overtime Hours","Vacation/PTO Hours",
            "Gross Pay","401k","Roth 401K","401K Match 2"
        ]) + "\n" +
        "\n".join([
            "2025-09-05,2025-09-05,08/25/2025 - 09/05/2025,Payroll,1001,Springfield HQ,Doe,Jane A,80,0,0,2400.00,150.00,75.00,50.00",
            "2025-09-05,2025-09-05,08/25/2025 - 09/05/2025,Payroll,1002,Shelbyville Office,Smith,John,85,5,0,2850.00,200.00,100.00,60.00",
            "2025-09-12,2025-09-12,09/08/2025 - 09/12/2025,Payroll,1003,Capital City Plant,Johnson,Emily R,75,0,10,2250.00,175.00,50.00,40.00",
        ]),
        encoding="utf-8",
    )

    # Auto-confirm proceed + auto-exit
    answers = iter(["", ""])  # "" => yes, "" => press enter
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(answers))

    mod = load_script()

    # Simulate CLI
    import sys
    old_argv = sys.argv
    sys.argv = ["payroll_fill.py", str(src)]
    try:
        mod.main()
    finally:
        sys.argv = old_argv

    # Assert archive
    archived = list((tmp_path / "data" / "archive").glob("in-*.csv"))
    assert len(archived) == 1

    # Assert outputs (one per Pay Date)
    outs = sorted((tmp_path / "dist").glob("PayrollUpload-*.csv"))
    assert len(outs) == 2, outs

    # Verify row counts per date
    out_map = {p.name: p for p in outs}
    df_0505 = pd.read_csv(out_map["PayrollUpload-2025-09-05.csv"], dtype=str)
    df_0912 = pd.read_csv(out_map["PayrollUpload-2025-09-12.csv"], dtype=str)
    assert len(df_0505) == 2
    assert len(df_0912) == 1
