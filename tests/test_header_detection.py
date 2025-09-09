import sys
from pathlib import Path
import importlib.util

# Import detect_header_row from payroll_cli.py without changing your script
SCRIPT = Path("payroll_fill.py")
spec = importlib.util.spec_from_file_location("payroll_fill", SCRIPT)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore

def test_header0():
    p = Path("data/incoming/header0.csv")
    idx = mod.detect_header_row(p, mod.EXPECT_NAMES)
    assert idx == 0

def test_header11():
    p = Path("data/incoming/header11.csv")
    idx = mod.detect_header_row(p, mod.EXPECT_NAMES)
    assert idx == 11

def test_header20():
    p = Path("data/incoming/header20.csv")
    idx = mod.detect_header_row(p, mod.EXPECT_NAMES)
    assert idx == 20
