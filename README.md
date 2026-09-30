## Requirements

- Python 3.10 or newer
- `requests`
- `urllib3`

Install the third-party packages with:

```powershell
python -m pip install requests urllib3
```

The other imports used by `mainCode.py` are included with Python and do not need to be installed separately.

## Run

From this folder, run:

```powershell
python mainCode.py
```

By default, the script downloads datasets for the `Hospitals` theme into `working_data/completed` and stores its state and run metadata in `working_data/latest`.
