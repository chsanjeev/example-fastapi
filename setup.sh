#!/bin/bash
set -e

# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 2. Install Python dependencies
pip install -r requirements.txt

# 3. Install VS Code extensions
if command -v code >/dev/null 2>&1; then
  while read ext; do code --install-extension "$ext"; done < .vscode/extensions.txt
else
  echo "VS Code CLI 'code' not found. Please install extensions manually."
fi

echo "Setup complete. Activate your environment with: source .venv/bin/activate"
echo "Run the app: uvicorn app.main:app --reload"
echo "Run tests: pytest"
echo "Format: black . && isort ."
echo "Lint: flake8"
