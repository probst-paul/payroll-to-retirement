from pathlib import Path
import importlib.util
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "payroll_fill.py"

def load_script():
    spec = importlib.util.spec_from_file_location("payroll_fill", SCRIPT_PATH)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod

def test_final_upload_columns_and_matching(tmp_path, monkeypatch):
    # Work in temp dir
    monkeypatch.chdir(tmp_path)

    # --- Create roster (template) with static fields ---
    tmpl_dir = tmp_path / "templates"
    tmpl_dir.mkdir(parents=True, exist_ok=True)

    # Roster headers include static info; dynamic ones will be filled by script
    roster_cols = [
        "SSN","First Name","MI","Last Name","Address 1","Address 2","City","State","Zip",
        "Date of Birth","Date of Hire","Date of Term","Rehire Date","Email Address","Profit Share"
    ]
    roster_df = pd.DataFrame([
        # Matched: Doe, Jane A
        ["111-22-3333","Jane","A","Doe","100 Main St","","Springfield","IL","62701",
         "1990-01-02","2020-03-04","","","jane@example.com",""],
        # Matched: Smith, John (no MI)
        ["222-33-4444","John","","Smith","200 Oak Ave","Apt 2","Shelbyville","IL","62565",
         "1988-05-06","2019-07-08","","","john@example.com",""],
        # Unmatched roster row: Zoe Nope
        ["333-44-5555","Zoe","","Nope","999 Nowhere","","Capital City","IL","62799",
         "1992-09-10","2021-01-01","","","zoe@example.com",""],
    ], columns=roster_cols)
    roster_df.to_csv(tmpl_dir / "roster.csv", index=False, encoding="utf-8")

    # --- Create incoming payroll CSV with 2 pay dates for grouping ---
    incoming = tmp_path / "incoming.csv"
    incoming.write_text(
        ",".join([
            "Pay Date","Transaction Date","Pay Period","Source","Paycheck #","Location Name",
            "Employee Last Name","Employee First Name",
            "Regular Hours","Overtime Hours","Vacation/PTO Hours",
            "Gross Pay","401k","Roth 401K","401K Match 2"
        ]) + "\n" +
        "\n".join([
            # Matches roster "Doe, Jane A" — Pay Date 1
            "2025-09-05,2025-09-05,08/25/2025 - 09/05/2025,Payroll,1001,Springfield HQ,Doe,Jane A,80,0,0,2400.00,150.00,75.00,50.00",
            # Matches roster "Smith, John" (no MI) — Pay Date 2
            "2025-09-12,2025-09-12,09/08/2025 - 09/12/2025,Payroll,1002,Shelbyville Office,Smith,John,85,5,0,2850.00,200.00,100.00,60.00",
            # (no row for Zoe Nope, so she'll be unmatched)
        ]),
        encoding="utf-8",
    )

    # Auto-confirm proceed + auto-exit
    answers = iter(["", ""])  # "" => yes, "" => press enter
    monkeypatch.setattr("builtins.input", lambda *args, **kwargs: next(answers))

    mod = load_script()

    # Run script as CLI
    import sys
    old_argv = sys.argv
    sys.argv = ["payroll_fill.py", str(incoming)]
    try:
        mod.main()
    finally:
        sys.argv = old_argv

    # --- Assertions ---

    # 1) Archive exists
    archived = list((tmp_path / "data" / "archive").glob("incoming-*.csv"))
    assert len(archived) == 1

    # 2) Two output files (one per pay date)
    dist_dir = tmp_path / "dist"
    outs = {p.name: p for p in dist_dir.glob("PayrollUpload-*.csv")}
    assert "PayrollUpload-2025-09-05.csv" in outs
    assert "PayrollUpload-2025-09-12.csv" in outs

    # 3) Column order matches FINAL_COLUMNS
    # Load module to access FINAL_COLUMNS
    FINAL_COLUMNS = mod.FINAL_COLUMNS

    for name in ["PayrollUpload-2025-09-05.csv", "PayrollUpload-2025-09-12.csv"]:
        df_out = pd.read_csv(outs[name], dtype=str)
        assert list(df_out.columns) == FINAL_COLUMNS, f"Column order mismatch in {name}"

    # 4) Each output should have exactly 1 row (only the matching employee for that date)
    df_0505 = pd.read_csv(outs["PayrollUpload-2025-09-05.csv"], dtype=str)
    df_0912 = pd.read_csv(outs["PayrollUpload-2025-09-12.csv"], dtype=str)
    assert len(df_0505) == 1
    assert len(df_0912) == 1

    # 5) Correct names landed in the right files
    assert df_0505.iloc[0]["First Name"] == "Jane" and df_0505.iloc[0]["Last Name"] == "Doe"
    assert df_0912.iloc[0]["First Name"] == "John" and df_0912.iloc[0]["Last Name"] == "Smith"

    # 6) Unmatched roster row ("Zoe Nope") should NOT appear in any output
    all_out = pd.concat([df_0505, df_0912], ignore_index=True)
    assert not ((all_out["First Name"] == "Zoe") & (all_out["Last Name"] == "Nope")).any()

    # 7) Optional unmatched report exists and lists Zoe
    unmatched_reports = list(dist_dir.glob("unmatched-*.csv"))
    assert len(unmatched_reports) == 1
    um = pd.read_csv(unmatched_reports[0], dtype=str)
    # Depending on columns preserved, just check name appears somewhere in row text
    assert (um.astype(str).apply(lambda r: "Zoe" in " ".join(r.values), axis=1)).any()
    assert (um.astype(str).apply(lambda r: "Nope" in " ".join(r.values), axis=1)).any()
