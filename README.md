# Payroll Fill Tool

A Python script that normalizes payroll CSV reports into a consistent format using a template roster for upload to the retirement account management portal.  

✔️ Matches employees by **First / MI / Last name** (handles middle initials inside the raw `Employee First Name` field).  
✔️ Maps raw CSV fields (e.g. `401k`, `Gross Pay`) into clean template columns.  
✔️ Computes and verifies total **employee hours** (`Regular + Overtime + PTO`).  
✔️ Copies the raw file to an **archive/** directory, writes a normalized CSV to **dist/**.  
✔️ Displays a **checksum** (sum of Pretax, Catchups, Roth, Safe Harbor).  

---

## Features

- **Name matching**  
  - Template: `First Name`, `MI`, `Last Name`  
  - Raw CSV: `Employee Last Name`, `Employee First Name` (may include MI)  
  - Matches strictly (`First+MI+Last`) then loosely (`First+Last`).

- **Field mapping**  
  | Template Column                | Raw CSV Column(s)                      |
  |--------------------------------|----------------------------------------|
  | Pretax                         | `401k`                                 |
  | Pre-Tax Catchup                | `401k Catchup` (optional)              |
  | Roth                           | `Roth 401K`                            |
  | Roth Catchup                   | `Roth Catchup` (optional)              |
  | Safe Harbor Non-Elective       | `401K Match 2`                         |
  | Current Period Compensation    | `Gross Pay`                            |
  | Current Period Hours Worked    | `Regular Hours + Overtime Hours + Vacation/PTO Hours` |

- **Output**  
  - Preserves static template columns (SSN, address, hire date, etc.)  
  - Fills in dynamic pay/hour fields from raw CSV  
  - Writes clean CSV with fixed column order to `dist/`  

---

## Requirements

- Python **3.8+**
- Dependencies:
  ```bash
  pip install pandas openpyxl
  ```

---

## Usage

### 1. Clone or download
```bash
git clone https://github.com/yourname/payroll-fill.git
cd payroll-fill
```

### 2. Place your template roster
- Put your employee roster in `templates/PayrollTemplate.xlsx` (or `.csv`).  
- It must have **First Name**, **MI**, **Last Name** plus any static info you want carried forward.

### 3. Run the tool

#### Windows
- **Drag and drop**: Drag a raw payroll CSV onto `payroll_fill.py`.  
- Or from PowerShell:
  ```powershell
  py payroll_fill.py path\to\raw.csv
  ```

#### macOS / Linux
```bash
python3 payroll_fill.py /path/to/raw.csv
```
*(or run with no arguments and paste/drag the CSV path when prompted)*

### 4. Verify totals
The script will display **total employee hours** (Reg + OT + PTO).  
Confirm with `Y` to continue.

### 5. Output
- Raw file → copied to `archive/` with a timestamped name.  
- Normalized CSV → written to `dist/<rawfile>_normalized.csv`.  
- Script prints a final **checksum** of contribution totals.

---

## Options

- `--template <path>` : Use a different roster file instead of the default.  
- `--sheet <name>`    : Select a specific sheet in the Excel template.  
- `--append-missing`  : Include employees that appear in the raw CSV but not in the template (adds them to bottom with blanks for static fields).

Example:
```bash
python3 payroll_fill.py data/raw.csv --template templates/AltRoster.xlsx --sheet Employees --append-missing
```

---

## Project Structure

```
payroll-fill/
├─ payroll_fill.py           # main script
├─ templates/
│  └─ PayrollTemplate.xlsx   # your static roster (First/MI/Last + static info)
├─ dist/                     # normalized CSV outputs
├─ archive/                  # archived raw CSVs
└─ README.md
```

---

## Future Improvements

- Add fuzzy matching for typos in employee names.  
- Optional Excel (`.xlsx`) output instead of CSV.  
- GUI wrapper for easier non-technical use.  

---

## License

MIT License © 2025 Paul Probst
