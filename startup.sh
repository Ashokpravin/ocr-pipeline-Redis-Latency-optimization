#!/bin/bash
# =============================================================================
# CustomOCR Pipeline - Katonic Startup Script v3.0
# =============================================================================
# USE THIS AS YOUR "Main file path" IN KATONIC: startup.sh
#
# FIXES in v3.0:
#   - LibreOffice Java disabled (prevents javaldx crash on .docx/.pptx)
#   - PaddleX DLA model pre-downloaded (prevents inference.yml not found)
#   - Environment variables for headless container operation
# =============================================================================

set -e

echo "=============================================="
echo " CustomOCR Pipeline v3.0 - Startup"
echo "=============================================="

# -------------------------------------------------
# STEP 0: Set environment variables for LibreOffice
# -------------------------------------------------
export OCR_OUTPUT_DIR=/app/output
mkdir -p /app/output/completed
export SAL_USE_VCLPLUGIN=gen
export SAL_DISABLE_COMPONENTCONTEXT=1
export JAVA_HOME=""
export GLOG_v=0

# Ensure HOME is writable (LibreOffice needs it)
if ! touch "$HOME/.write_test" 2>/dev/null; then
    export HOME=/tmp
    echo "[env] HOME not writable, using /tmp"
fi
rm -f "$HOME/.write_test" 2>/dev/null

export PADDLEX_HOME="${HOME}/.paddlex"

echo "[env] HOME=$HOME"
echo "[env] PADDLEX_HOME=$PADDLEX_HOME"

# -------------------------------------------------
# STEP 1: Nuke conda-installed opencv
# -------------------------------------------------
echo "[1/7] Removing conda-installed OpenCV..."

SITE_PKG=$(python -c "import site; print(site.getsitepackages()[0])" 2>/dev/null || echo "/opt/conda/lib/python3.11/site-packages")

rm -rf "${SITE_PKG}/cv2" 2>/dev/null || true
rm -rf "${SITE_PKG}/cv2.cpython"* 2>/dev/null || true
rm -rf "${SITE_PKG}/opencv_python"* 2>/dev/null || true
rm -rf "${SITE_PKG}/opencv_contrib_python"* 2>/dev/null || true
rm -rf "${SITE_PKG}/opencv_python_headless"* 2>/dev/null || true
rm -rf "${SITE_PKG}/opencv_contrib_python_headless"* 2>/dev/null || true

conda remove --force --yes opencv-python-headless opencv-python opencv-contrib-python py-opencv libopencv 2>/dev/null || true
pip uninstall -y opencv-python opencv-contrib-python opencv-python-headless opencv-contrib-python-headless 2>/dev/null || true

echo "  ✓ OpenCV removed"

# -------------------------------------------------
# STEP 2: Install requirements.txt
# -------------------------------------------------
echo "[2/7] Installing Python requirements..."
pip install --no-cache-dir -r requirements.txt 2>&1 | tail -3

# -------------------------------------------------
# STEP 3: Install paddleocr WITHOUT its opencv dependency
# -------------------------------------------------
echo "[3/7] Installing PaddleOCR (--no-deps)..."
pip install --no-cache-dir --no-deps paddleocr==3.3.2

# -------------------------------------------------
# STEP 4: Install headless OpenCV LAST
# -------------------------------------------------
echo "[4/7] Installing headless OpenCV..."
pip install --no-cache-dir --no-deps \
    opencv-python-headless==4.12.0.88 \
    opencv-contrib-python-headless==4.10.0.84

# -------------------------------------------------
# STEP 5: Install LibreOffice + disable Java
# -------------------------------------------------
echo "[5/7] Installing LibreOffice..."

if command -v soffice &>/dev/null; then
    echo "  ✓ Already installed: $(which soffice)"
else
    apt-get update -qq 2>/dev/null && \
    apt-get install -y -qq --no-install-recommends \
        libreoffice-writer libreoffice-calc libreoffice-impress \
        libreoffice-common fonts-dejavu-core 2>/dev/null || \
    apt-get install -y -qq libreoffice 2>/dev/null || \
    conda install -y -c conda-forge libreoffice 2>/dev/null || \
    echo "  ✗ LibreOffice install failed"
fi

# Disable Java in LibreOffice config (prevents javaldx crash)
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
echo "  ✓ Java disabled in LibreOffice config"

# -------------------------------------------------
# STEP 6: Pre-download PaddleX DLA model
# -------------------------------------------------
echo "[6/7] Pre-downloading PaddleX DLA model..."

MODEL_YML="${PADDLEX_HOME}/official_models/PP-DocLayout_plus-L/inference.yml"

if [ -f "$MODEL_YML" ]; then
    echo "  ✓ Model already present"
else
    echo "  Downloading PP-DocLayout_plus-L (may take a few minutes)..."
    python -c "
import os
os.environ['PADDLEX_HOME'] = '${PADDLEX_HOME}'
os.environ['GLOG_v'] = '0'
from paddleocr import LayoutDetection
m = LayoutDetection(model_name='PP-DocLayout_plus-L')
print('MODEL_OK')
" 2>&1 | tail -5

    if [ -f "$MODEL_YML" ]; then
        echo "  ✓ Model downloaded successfully"
    else
        echo "  ✗ Model download failed (will retry on first request)"
    fi
fi

# -------------------------------------------------
# STEP 7: Verify everything
# -------------------------------------------------
echo "[7/7] Verifying..."
python -c "
import cv2
print(f'  OpenCV: {cv2.__version__} from {cv2.__file__}')
import fastapi; print(f'  FastAPI: {fastapi.__version__}')
import fitz; print(f'  PyMuPDF: {fitz.__version__}')
from paddleocr import LayoutDetection; print('  PaddleOCR: ok')
print('  All OK!')
" || echo "  WARNING: Verification had issues, continuing..."

command -v soffice &>/dev/null && echo "  LibreOffice: $(soffice --version 2>&1 | head -1)" || echo "  ✗ soffice not found"

[ -f "$MODEL_YML" ] && echo "  DLA Model: ✓ present" || echo "  DLA Model: ✗ missing"

echo "=============================================="
echo " Starting CustomOCR Pipeline API v3.0..."
echo "=============================================="

# Start the FastAPI application
exec uvicorn app:app --host 0.0.0.0 --port 8050 --timeout-keep-alive 120