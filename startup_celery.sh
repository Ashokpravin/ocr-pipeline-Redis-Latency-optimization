#!/bin/bash
# =============================================================================
# CustomOCR Pipeline - Celery Worker Startup Script (Katonic Deployment 2)
# =============================================================================
# USE THIS AS YOUR "Main file path" IN KATONIC for the Celery worker deployment.
#
# This script mirrors startup.sh dependency installation but starts
# the Celery worker instead of FastAPI.
#
# CRITICAL: Both this deployment and the FastAPI deployment MUST mount
# the same PVC at /app/output — Celery reads files FastAPI writes there.
# =============================================================================

set -e

echo "=============================================="
echo " CustomOCR Celery Worker - Startup"
echo "=============================================="

# -------------------------------------------------
# STEP 0: Environment variables
# -------------------------------------------------
export SAL_USE_VCLPLUGIN=gen
export SAL_DISABLE_COMPONENTCONTEXT=1
export JAVA_HOME=""
export GLOG_v=0

if ! touch "$HOME/.write_test" 2>/dev/null; then
    export HOME=/tmp
fi
rm -f "$HOME/.write_test" 2>/dev/null
export PADDLEX_HOME="${HOME}/.paddlex"

echo "[env] HOME=$HOME"
echo "[env] PADDLEX_HOME=$PADDLEX_HOME"
echo "[env] REDIS_URL=${REDIS_URL}"
echo "[env] OCR_OUTPUT_DIR=${OCR_OUTPUT_DIR}"

# -------------------------------------------------
# STEP 1: Remove conflicting OpenCV
# -------------------------------------------------
echo "[1/5] Removing conda-installed OpenCV..."
SITE_PKG=$(python -c "import site; print(site.getsitepackages()[0])" 2>/dev/null || echo "/opt/conda/lib/python3.11/site-packages")
rm -rf "${SITE_PKG}/cv2" 2>/dev/null || true
rm -rf "${SITE_PKG}/cv2.cpython"* 2>/dev/null || true
rm -rf "${SITE_PKG}/opencv_python"* 2>/dev/null || true
rm -rf "${SITE_PKG}/opencv_contrib_python"* 2>/dev/null || true
conda remove --force --yes opencv-python-headless opencv-python 2>/dev/null || true
pip uninstall -y opencv-python opencv-contrib-python opencv-python-headless 2>/dev/null || true
echo "  Done"

# -------------------------------------------------
# STEP 2: Install requirements
# -------------------------------------------------
echo "[2/5] Installing Python requirements..."
pip install --no-cache-dir -r requirements.txt 2>&1 | tail -3
pip install --no-cache-dir --no-deps paddleocr==3.3.2
pip install --no-cache-dir --no-deps \
    opencv-python-headless==4.12.0.88 \
    opencv-contrib-python-headless==4.10.0.84
echo "  Done"

# -------------------------------------------------
# STEP 3: LibreOffice + disable Java
# -------------------------------------------------
echo "[3/5] Checking LibreOffice..."
if command -v soffice &>/dev/null; then
    echo "  Already installed"
else
    apt-get update -qq 2>/dev/null && \
    apt-get install -y -qq --no-install-recommends \
        libreoffice-writer libreoffice-calc libreoffice-impress \
        libreoffice-common fonts-dejavu-core 2>/dev/null || true
fi

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
echo "  Java disabled in LibreOffice"

# -------------------------------------------------
# STEP 4: Pre-download PaddleX DLA model
# -------------------------------------------------
echo "[4/5] Checking PaddleX DLA model..."
MODEL_YML="${PADDLEX_HOME}/official_models/PP-DocLayout_plus-L/inference.yml"
if [ -f "$MODEL_YML" ]; then
    echo "  Model already present"
else
    echo "  Downloading PP-DocLayout_plus-L..."
    python -c "
import os
os.environ['PADDLEX_HOME'] = '${PADDLEX_HOME}'
os.environ['GLOG_v'] = '0'
from paddleocr import LayoutDetection
m = LayoutDetection(model_name='PP-DocLayout_plus-L')
print('MODEL_OK')
" 2>&1 | tail -3
fi

# -------------------------------------------------
# STEP 5: Ensure output directory exists
# -------------------------------------------------
echo "[5/5] Ensuring output directory..."
mkdir -p "${OCR_OUTPUT_DIR:-/app/output}/completed"
echo "  Output dir: ${OCR_OUTPUT_DIR:-/app/output}"

echo "=============================================="
echo " Starting Celery Worker (gevent, C=24)..."
echo "=============================================="

# Start Celery worker
exec celery -A celery_app worker \
    --pool=prefork \
    --concurrency=8 \
    --loglevel=info \
    -Q ocr_queue