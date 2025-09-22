# Project Setup Instructions

## Prerequisites
- Python 3.12+
- VS Code
- Recommended VS Code extensions (see `.vscode/extensions.txt`)

## 1. Clone the repository
```bash
git clone https://github.com/chsanjeev/example-fastapi.git
cd example-fastapi
```

## 2. Create and activate a virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

## 3. Install Python dependencies
```bash
pip install -r requirements.txt
```

## 4. Install VS Code extensions
```bash
while read ext; do code --install-extension "$ext"; done < .vscode/extensions.txt
```

## 5. Initial database setup
DuckDB is file-based; no manual setup required. The app will create `data.db` automatically.

## 6. Run the application
```bash
uvicorn app.main:app --reload
```

## 7. Run tests
```bash
pytest
```

## 8. Format and lint code
```bash
black .
isort .
flake8
```

## 9. Automation script
Run the following script to automate steps 2–5:

```bash
bash setup.sh
```
