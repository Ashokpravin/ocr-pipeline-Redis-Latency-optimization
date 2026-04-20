#!/bin/bash
# =============================================================================
# CustomOCR Pipeline - Celery Worker Startup Script v3.2
# =============================================================================
# FIXES IN v3.2:
#   1. Redis readiness check before starting worker (prevents crash loop)
#   2. apt-get --fix-missing for held broken packages
#   3. apt cache cleanup after LibreOffice install (prevents disk bloat)
# =============================================================================

set -e

echo "=============================================="
echo " CustomOCR Celery Worker - Startup v3.2"
echo "=============================================="

# -------------------------------------------------
# STEP 0: Environment variables
# -------------------------------------------------
export DEPLOY_TYPE=worker  # FIX: Uses .deps_ok_worker marker
export SAL_USE_VCLPLUGIN=gen
export JAVA_HOME=""
export GLOG_v=0
export DEBIAN_FRONTEND=noninteractive

if ! touch "$HOME/.write_test" 2>/dev/null; then export HOME=/tmp; fi
rm -f "$HOME/.write_test" 2>/dev/null
export PADDLEX_HOME="${HOME}/.paddlex"

# -------------------------------------------------
# STEP 1: Remove conda-installed OpenCV
# -------------------------------------------------
echo "[1/6] Removing conda-installed OpenCV..."
SITE_PKG=$(python -c "import site; print(site.getsitepackages()[0])" 2>/dev/null || echo "/opt/conda/lib/python3.11/site-packages")
rm -rf "${SITE_PKG}/cv2"* 2>/dev/null || true
rm -rf "${SITE_PKG}/opencv"* 2>/dev/null || true
conda remove --force --yes opencv-python-headless opencv-python 2>/dev/null || true
pip uninstall -y opencv-python opencv-contrib-python opencv-python-headless 2>/dev/null || true
echo "  Done"

# -------------------------------------------------
# STEP 2: Install Python requirements
# -------------------------------------------------
echo "[2/6] Installing Python requirements..."
pip install --no-cache-dir -r requirements.txt 2>&1 | tail -3
pip install --no-cache-dir --no-deps paddleocr==3.3.2
pip install --no-cache-dir --no-deps opencv-python-headless==4.12.0.88 opencv-contrib-python-headless==4.10.0.84

# -------------------------------------------------
# STEP 3: Install LibreOffice (with conflict fix)
# -------------------------------------------------
echo "[3/6] Installing LibreOffice..."

SUDO=""
if command -v sudo &>/dev/null; then SUDO="sudo -n"; fi

if command -v soffice &>/dev/null; then
    echo "  Already installed"
else
    # FIX: Aggressively fix held broken packages before installing
    $SUDO apt-get update -qq --fix-missing 2>/dev/null || true
    $SUDO apt-get install -y -f 2>/dev/null || true
    
    $SUDO apt-get install -y -qq --no-install-recommends \
        libreoffice-writer libreoffice-calc libreoffice-impress \
        libreoffice-common fonts-dejavu-core xvfb 2>/dev/null || \
    $SUDO apt-get install -y -qq libreoffice xvfb 2>/dev/null || \
    echo "  LibreOffice install failed."
    
    # FIX: Clean apt cache to prevent disk bloat
    $SUDO rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/* 2>/dev/null || true
fi

# Disable Java in LibreOffice config
LO_PROFILE="${HOME}/.config/libreoffice/4/user"
mkdir -p "$LO_PROFILE"
cat > "${LO_PROFILE}/registrymodifications.xcu" << 'XMLEOF'
<?xml version="1.0" encoding="UTF-8"?>
<oor:items xmlns:oor="http://openoffice.org/2001/registry"
           xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
  <item oor:path="/org.openoffice.Office.Common/Java">
    <node oor:name="ooSetupNode" oor:op="fuse">
      <prop oor:name="Enable" oor:type="xs:boolean">
        <value>false</value>
      </prop>
    </node>
  </item>
</oor:items>
XMLEOF

# -------------------------------------------------
# STEP 4: Pre-download PaddleX DLA model
# -------------------------------------------------
echo "[4/6] Pre-downloading PaddleX DLA model..."
MODEL_YML="${PADDLEX_HOME}/official_models/PP-DocLayout_plus-L/inference.yml"

if [ -f "$MODEL_YML" ]; then
    echo "  Model already present"
else
    python -c "
import os
os.environ['PADDLEX_HOME'] = '${PADDLEX_HOME}'
os.environ['GLOG_v'] = '0'
from paddleocr import LayoutDetection
m = LayoutDetection(model_name='PP-DocLayout_plus-L')
print('Model downloaded')
" 2>&1 | tail -5
fi

# -------------------------------------------------
# STEP 5: Create output directory
# -------------------------------------------------
echo "[5/6] Ensuring output directory..."
mkdir -p "${OCR_OUTPUT_DIR:-/app/output}/completed"

# -------------------------------------------------
# STEP 6: Wait for Redis (FIX: prevents crash loop)
# -------------------------------------------------
echo "[6/6] Waiting for Redis..."
REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"

for i in $(seq 1 15); do
    if python -c "import redis; r = redis.from_url('$REDIS_URL', socket_connect_timeout=5); r.ping(); print('Redis OK')" 2>/dev/null; then
        break
    fi
    echo "  Attempt $i/15 - Redis not ready, waiting 5s..."
    sleep 5
done

echo "=============================================="
echo " Starting Celery Worker..."
echo "=============================================="

exec celery -A celery_app worker \
    --pool=prefork \
    --concurrency="${CELERY_CONCURRENCY:-8}" \
    --loglevel=info \
    -Q ocr_queue