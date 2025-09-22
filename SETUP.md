# Project Setup Instructions

## Setup Options

You can set up the project in one of two ways:

---

### Option 1: Local (Non-Docker) Setup

#### Windows 11 Compatibility Notes
- All dependencies (Python, DuckDB, VS Code, Docker) work on Windows 11.
- Use Command Prompt, PowerShell, or Git Bash for commands.
- To activate the virtual environment, use:
	```bat
	.venv\Scripts\activate
	```
- For shell scripts (`setup.sh`, `docker-setup.sh`), use Git Bash, WSL, or run commands manually as listed below.
- All VS Code extensions are supported on Windows.


#### Prerequisites
- Python 3.12+
- VS Code
- Recommended VS Code extensions (see `.vscode/extensions.txt`)

#### Steps
1. Clone the repository
	```bash
	git clone https://github.com/chsanjeev/example-fastapi.git
	cd example-fastapi
	```
2. Create and activate a virtual environment
	```bash
	python -m venv .venv
	# On macOS/Linux:
	source .venv/bin/activate
	# On Windows (Command Prompt):
	.venv\Scripts\activate
	# On Windows (PowerShell):
	.venv\Scripts\Activate.ps1
	```
3. Install Python dependencies
	```bash
	pip install -r requirements.txt
	```
4. Install VS Code extensions
	```bash
	while read ext; do code --install-extension "$ext"; done < .vscode/extensions.txt
	```
5. Initial database setup
	- DuckDB is file-based; no manual setup required. The app will create `data.db` automatically.
6. Run the application
	```bash
	uvicorn app.main:app --reload
	```
7. Run tests
	```bash
	pytest
	```
8. Format and lint code
	```bash
	black .
	isort .
	flake8
	```
9. Automation script
	Run the following script to automate steps 2–5:
		- On macOS/Linux:
			```bash
			bash setup.sh
			```
		- On Windows (Git Bash/WSL):
			```bash
			bash setup.sh
			```
		- On Windows (Command Prompt/PowerShell):
			Run the commands in steps 2–5 manually, or use Git Bash/WSL for the script.

---

### Option 2: Docker-Based Setup

#### Windows 11 Compatibility Notes
- Docker Desktop for Windows is supported.
- Use Command Prompt, PowerShell, or Git Bash for commands.

#### Prerequisites
- Docker installed

#### Steps
1. Build and run the app in Docker:
		- On macOS/Linux:
			```bash
			bash docker-setup.sh
			```
		- On Windows (Git Bash/WSL):
			```bash
			bash docker-setup.sh
			```
		- On Windows (Command Prompt/PowerShell):
			Run the commands in `docker-setup.sh` manually, or use Git Bash/WSL for the script.
	This will build the Docker image and start the app in a container on port 8000.
2. (Optional) Run tests inside the container:
	```bash
	docker exec -it example-fastapi pytest
	```

---
