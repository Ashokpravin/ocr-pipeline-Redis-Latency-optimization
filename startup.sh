#!/bin/bash
# =============================================================================
# CustomOCR Pipeline - API Startup Script v3.2
# =============================================================================
# FIXES IN v3.2:
#   1. DEPLOY_TYPE=api for deployment-specific marker
#   2. DEBIAN_FRONTEND=noninteractive for all apt operations
#   3. apt-get --fix-missing for held broken packages
#   4. apt cache cleanup after LibreOffice install
#   5. PaddleX model pre-download
# =============================================================================

set -e

echo "=============================================="
echo " CustomOCR Pipeline v3.2 - API Startup"
echo "=============================================="

# -------------------------------------------------
# STEP 0: Set environment variables
# -------------------------------------------------
export DEPLOY_TYPE=api  # FIX: Uses .deps_ok_api marker
export OCR_OUTPUT_DIR=/app/output
mkdir -p /app/output/completed
export SAL_USE_VCLPLUGIN=gen
export JAVA_HOME=""
export GLOG_v=0
export DEBIAN_FRONTEND=noninteractive

if ! touch "$HOME/.write_test" 2>/dev/null; then
    export HOME=/tmp
fi
rm -f "$HOME/.write_test" 2>/dev/null

export PADDLEX_HOME="${HOME}/.paddlex"

# -------------------------------------------------
# STEP 1: Nuke conda-installed opencv
# -------------------------------------------------
echo "[1/7] Removing conda-installed OpenCV..."
SITE_PKG=$(python -c "import site; print(site.getsitepackages()[0])" 2>/dev/null || echo "/opt/conda/lib/python3.11/site-packages")
rm -rf "${SITE_PKG}/cv2"* 2>/dev/null || true
rm -rf "${SITE_PKG}/opencv"* 2>/dev/null || true
conda remove --force --yes opencv-python-headless opencv-python opencv-contrib-python py-opencv libopencv 2>/dev/null || true
pip uninstall -y opencv-python opencv-contrib-python opencv-python-headless opencv-contrib-python-headless 2>/dev/null || true
echo "  Done"

# -------------------------------------------------
# STEP 2-4: Python Requirements
# -------------------------------------------------
echo "[2-4/7] Installing Python requirements..."
pip install --no-cache-dir -r requirements.txt 2>&1 | tail -3
pip install --no-cache-dir --no-deps paddleocr==3.3.2
pip install --no-cache-dir --no-deps opencv-python-headless==4.12.0.88 opencv-contrib-python-headless==4.10.0.84

# -------------------------------------------------
# STEP 5: Install LibreOffice (WITH CONFLICT FIX)
# -------------------------------------------------
echo "[5/7] Installing LibreOffice..."

SUDO=""
if command -v sudo &>/dev/null; then SUDO="sudo -n"; fi

if command -v soffice &>/dev/null; then
    echo "  Already installed"
else
    # FIX: Fix held broken packages before installing
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
# STEP 6: Pre-download PaddleX DLA model
# -------------------------------------------------
echo "[6/7] Pre-downloading PaddleX DLA model..."
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
# STEP 7: Verify everything
# -------------------------------------------------
echo "[7/7] Verifying..."
command -v soffice &>/dev/null && echo "  LibreOffice: $(soffice --version 2>&1 | head -1)" || echo "  soffice not found"
[ -f "$MODEL_YML" ] && echo "  DLA Model: present" || echo "  DLA Model: missing"

echo "=============================================="
echo " Starting CustomOCR Pipeline API v3.2..."
echo "=============================================="

exec uvicorn app:app --host 0.0.0.0 --port 8050 --timeout-keep-alive 120