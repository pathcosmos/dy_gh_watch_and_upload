# File Monitoring and Auto-Upload System

A Python-based system for monitoring file changes and automatically uploading them.

## Features

- File system monitoring with watchdog
- Database integration with SQLAlchemy
- Task scheduling with APScheduler
- HTTP API communication with requests
- Configuration management with YAML and environment variables
- PostgreSQL and SQLite support

## Requirements

- Python 3.11 or higher
- uv (Python package manager)
- Git

## Quick Start

### 1. Clone the Repository

```bash
# Clone the repository
git clone <repository-url>
cd dy-gh-watch-and-upload

# Or if you already have the repository, pull latest changes
git pull origin main
```

### 2. Install uv (if not already installed)

#### On macOS and Linux:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

#### On Windows:
```bash
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

#### Using pip (alternative):
```bash
pip install uv
```

#### Verify installation:
```bash
uv --version
```

### 3. Initialize Python Environment with uv

```bash
# Navigate to project directory
cd dy-gh-watch-and-upload

# Initialize uv project (if not already initialized)
uv init --python 3.11

# Install all dependencies
uv sync

# Install development dependencies (optional)
uv sync --extra dev
```

### 4. Activate Virtual Environment

```bash
# Activate the virtual environment
source .venv/bin/activate

# On Windows:
# .venv\Scripts\activate

# Verify activation (you should see (.venv) in your prompt)
which python
# Should show: /path/to/project/.venv/bin/python
```

### 5. Verify Installation

```bash
# Test if all dependencies are working
python -c "import watchdog, sqlalchemy, requests, apscheduler; print('✅ All dependencies imported successfully!')"

# Run tests to ensure everything is working
pytest
```

## Detailed Setup Instructions

### Prerequisites Check

Before starting, ensure you have the required tools:

```bash
# Check Python version (should be 3.11+)
python --version

# Check if git is available
git --version

# Check if uv is available
uv --version
```

### Complete Setup Workflow

```bash
# 1. Clone repository
git clone <repository-url>
cd dy-gh-watch-and-upload

# 2. Initialize uv project
uv init --python 3.11

# 3. Install dependencies
uv sync

# 4. Activate environment
source .venv/bin/activate

# 5. Verify setup
python -c "import sys; print(f'Python: {sys.version}')"
python -c "import watchdog; print(f'Watchdog: {watchdog.__version__}')"
```

### Environment Configuration

```bash
# Copy environment template (if available)
cp .env.example .env

# Edit environment variables
nano .env
# or
code .env
```

### Development Setup

For developers who want to contribute:

```bash
# Install with development dependencies
uv sync --extra dev

# Install pre-commit hooks (if configured)
pre-commit install

# Run linting and formatting
uv run black src/ tests/
uv run flake8 src/ tests/
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Python Version Mismatch
```bash
# If you get Python version errors
uv python install 3.11
uv sync
```

#### 2. Permission Issues
```bash
# If you get permission errors on Linux/macOS
sudo chown -R $USER:$USER .venv/
```

#### 3. Dependency Resolution Failures
```bash
# Clear uv cache and retry
uv cache clean
uv sync
```

#### 4. Virtual Environment Issues
```bash
# Remove and recreate virtual environment
rm -rf .venv/
uv sync
```

#### 5. Git Submodules (if applicable)
```bash
# If the project uses git submodules
git submodule update --init --recursive
```

### Platform-Specific Notes

#### Windows
- Use PowerShell or Command Prompt
- Virtual environment activation: `.venv\Scripts\activate`
- Path separators: Use backslashes `\`

#### macOS
- Virtual environment activation: `source .venv/bin/activate`
- If you get SSL errors, you might need to install certificates:
  ```bash
  /Applications/Python\ 3.11/Install\ Certificates.command
  ```

#### Linux
- Virtual environment activation: `source .venv/bin/activate`
- You might need to install system dependencies:
  ```bash
  sudo apt-get install python3-dev build-essential
  ```

## Project Structure

```
├── src/                    # Source code
│   ├── api/               # API endpoints
│   ├── db/                # Database models and connections
│   ├── monitor/           # File monitoring logic
│   ├── services/          # Business logic services
│   ├── uploader/          # File upload functionality
│   └── utils/             # Utility functions
├── config/                 # Configuration files
├── data/                   # Data storage
├── logs/                   # Log files
├── tests/                  # Test files
├── pyproject.toml          # Project configuration (uv)
├── uv.lock                 # Dependency lock file
├── .venv/                  # Virtual environment (uv)
└── README.md               # This file
```

## Dependencies

### Core Dependencies
- `watchdog>=3.0.0` - File system monitoring
- `SQLAlchemy>=2.0.0` - Database ORM
- `requests>=2.31.0` - HTTP client
- `APScheduler>=3.10.0` - Task scheduling
- `python-dotenv>=1.0.0` - Environment variables
- `PyYAML>=6.0.0` - YAML parsing
- `psycopg2-binary>=2.9.0` - PostgreSQL adapter
- `python-dateutil>=2.8.0` - Date utilities
- `Pillow>=10.0.0` - Image processing

### Development Dependencies
- `pytest>=7.4.0` - Testing framework
- `pytest-mock>=3.11.0` - Mocking for tests
- `pytest-cov>=4.1.0` - Test coverage

## uv Commands Reference

### Basic Commands
- `uv sync` - Install all dependencies
- `uv sync --extra dev` - Install with development dependencies
- `uv add <package>` - Add a new dependency
- `uv remove <package>` - Remove a dependency
- `uv lock` - Generate/update lock file
- `uv run <command>` - Run command in virtual environment

### Advanced Commands
- `uv python install 3.11` - Install specific Python version
- `uv cache clean` - Clean uv cache
- `uv tree` - Show dependency tree
- `uv pip compile` - Compile requirements from pyproject.toml

## Usage

### Running the Application

```bash
# Activate virtual environment
source .venv/bin/activate

# Run the main application
python src/main.py

# Or using uv run
uv run python src/main.py
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src

# Run specific test file
uv run pytest tests/test_monitor.py
```

### Development Workflow

```bash
# 1. Activate environment
source .venv/bin/activate

# 2. Make changes to code

# 3. Run tests
pytest

# 4. Add new dependencies (if needed)
uv add <package-name>

# 5. Update lock file
uv lock

# 6. Commit changes
git add .
git commit -m "Description of changes"
```

## Migration from pip/requirements.txt

This project has been migrated from pip/requirements.txt to uv/pyproject.toml for better dependency management and faster installations.

- **Old workflow**: `pip install -r requirements.txt`
- **New workflow**: `uv sync`

### Benefits of uv over pip:
- **Speed**: 10-100x faster dependency resolution
- **Reliability**: Deterministic builds with lock files
- **Modern**: Uses pyproject.toml standard
- **Integration**: Built-in virtual environment management

## Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Set up development environment: `uv sync --extra dev`
4. Make your changes
5. Run tests: `pytest`
6. Commit your changes: `git commit -m "Add feature"`
7. Push to the branch: `git push origin feature-name`
8. Submit a pull request

## License

[Add your license here]
