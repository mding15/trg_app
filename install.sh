#!/bin/bash
# =============================================================================
# install.sh — Bootstrap trg_app on Amazon Linux 2 / Amazon Linux 2023
# Run as ec2-user:  bash install.sh
# =============================================================================
set -e

PYTHON=python3
VENV_DIR=".venv"

echo "======================================================"
echo " trg_app — dependency installer"
echo "======================================================"

# ------------------------------------------------------------------------------
# 1. System packages
# ------------------------------------------------------------------------------
echo ""
echo "[1/4] Installing system packages..."

# Detect package manager (Amazon Linux 2 → yum, AL2023 → dnf)
if command -v dnf &>/dev/null; then
    PKG="sudo dnf install -y"
else
    PKG="sudo yum install -y"
fi

$PKG \
    python3 \
    python3-pip \
    python3-devel \
    gcc \
    gcc-c++ \
    make \
    libffi-devel \
    openssl-devel \
    postgresql-devel \
    unixODBC \
    unixODBC-devel \
    libjpeg-devel \
    zlib-devel

# ------------------------------------------------------------------------------
# 2. Microsoft ODBC Driver (for pyodbc)
# ------------------------------------------------------------------------------
echo ""
echo "[2/4] Installing Microsoft ODBC Driver 18..."

if ! rpm -q msodbcsql18 &>/dev/null; then
    # Import Microsoft package signing key
    sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc

    # Add repo (works for both AL2 and AL2023)
    if command -v dnf &>/dev/null; then
        sudo dnf install -y \
            "https://packages.microsoft.com/config/rhel/9/packages-microsoft-prod.rpm" || true
    else
        sudo yum install -y \
            "https://packages.microsoft.com/config/rhel/7/packages-microsoft-prod.rpm" || true
    fi

    ACCEPT_EULA=Y sudo $PKG msodbcsql18 mssql-tools18
else
    echo "   msodbcsql18 already installed, skipping."
fi

# ------------------------------------------------------------------------------
# 3. Python virtual environment
# ------------------------------------------------------------------------------
echo ""
echo "[3/4] Creating virtual environment at $VENV_DIR ..."

if [ ! -d "$VENV_DIR" ]; then
    $PYTHON -m venv "$VENV_DIR"
fi

source "$VENV_DIR/bin/activate"
pip install --upgrade pip

# ------------------------------------------------------------------------------
# 4. Python packages
# ------------------------------------------------------------------------------
echo ""
echo "[4/4] Installing Python packages..."

pip install \
    `# ── Web framework ──────────────────────────────` \
    Flask \
    flask-cors \
    Flask-Bcrypt \
    Flask-Login \
    Flask-Mail \
    Flask-SQLAlchemy \
    Flask-WTF \
    flasgger \
    \
    `# ── Auth & security ────────────────────────────` \
    PyJWT \
    bcrypt \
    itsdangerous \
    cryptography \
    \
    `# ── Database ───────────────────────────────────` \
    SQLAlchemy \
    psycopg2-binary \
    pyodbc \
    \
    `# ── Data & numerics ────────────────────────────` \
    numpy \
    pandas \
    scipy \
    scikit-learn \
    pyarrow \
    pytz \
    python-dateutil \
    \
    `# ── Azure ──────────────────────────────────────` \
    azure-identity \
    azure-communication-email \
    \
    `# ── Market data ────────────────────────────────` \
    yfinance \
    yahooquery \
    alpha-vantage \
    \
    `# ── File & network ─────────────────────────────` \
    openpyxl \
    xlwings \
    Pillow \
    requests \
    paramiko \
    scp \
    python-dotenv \
    \
    `# ── Misc ───────────────────────────────────────` \
    PyYAML \
    WTForms

echo ""
echo "======================================================"
echo " Installation complete."
echo " Activate the venv with:  source $VENV_DIR/bin/activate"
echo " Start the server with:   python api_main.py -port 8000"
echo "======================================================"

# NOTE: pywin32 is Windows-only and is NOT installed here.
# xlwings is installed but requires Excel to function — it is safe to import
# on Linux but Excel-automation calls will fail at runtime.
