@echo off
REM Setup script for Restaurant ELT Pipeline (Windows)

echo ==========================================
echo Restaurant ELT Pipeline - Setup
echo ==========================================
echo.

REM Check Python version
python --version

REM Create virtual environment
echo.
echo Creating virtual environment...
python -m venv venv

REM Activate virtual environment
echo Activating virtual environment...
call venv\Scripts\activate.bat

REM Upgrade pip
echo.
echo Upgrading pip...
python -m pip install --upgrade pip

REM Install dependencies
echo.
echo Installing dependencies...
pip install -r requirements.txt

REM Copy .env.example to .env if not exists
if not exist .env (
    echo.
    echo Creating .env file from .env.example...
    copy .env.example .env
    echo Please update .env with your Azure SAS URL if needed
)

REM Create necessary directories
echo.
echo Creating directories...
if not exist data\csv mkdir data\csv
if not exist data\outputs mkdir data\outputs
if not exist data\dagster_storage mkdir data\dagster_storage

echo.
echo ==========================================
echo Setup complete!
echo ==========================================
echo.
echo Next steps:
echo   1. Activate virtual environment:
echo      venv\Scripts\activate.bat
echo.
echo   2. Verify setup:
echo      python verify_setup.py
echo.
echo   3. Run pipeline:
echo      python run_pipeline.py
echo.

pause
