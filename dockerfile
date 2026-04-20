# =============================================================================
# CustomOCR Pipeline API - Dockerfile (Katonic Docker Mode) v3.1
# =============================================================================

FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# LibreOffice environment: disable Java, set headless mode
# CRITICAL FIX: Removed SAL_DISABLE_COMPONENTCONTEXT=1
ENV SAL_USE_VCLPLUGIN=gen
ENV JAVA_HOME=""
ENV HOME=/root

# PaddleX model cache
ENV PADDLEX_HOME=/root/.paddlex
ENV GLOG_v=0

WORKDIR /app

# System dependencies + LibreOffice (critical for .docx/.pptx/.xlsx)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    libgomp1 \
    libstdc++6 \
    poppler-utils \
    xvfb \
    # LibreOffice for Office document conversion
    libreoffice-writer \
    libreoffice-calc \
    libreoffice-impress \
    libreoffice-common \
    # Fonts for Arabic/Unicode rendering
    fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Disable Java in LibreOffice config (prevents "failed to launch javaldx")
RUN mkdir -p /root/.config/libreoffice/4/user && \
    echo '<?xml version="1.0" encoding="UTF-8"?>\n\
<oor:items xmlns:oor="http://openoffice.org/2001/registry"\n\
           xmlns:xs="http://www.w3.org/2001/XMLSchema"\n\
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n\
  <item oor:path="/org.openoffice.Office.Common/Java">\n\
    <node oor:name="ooSetupNode" oor:op="fuse">\n\
      <prop oor:name="Enable" oor:type="xs:boolean">\n\
        <value>false</value>\n\
      </prop>\n\
    </node>\n\
  </item>\n\
</oor:items>' > /root/.config/libreoffice/4/user/registrymodifications.xcu

# Install Python deps and fix OpenCV for PaddleX
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --no-deps paddleocr==3.3.2 && \
    pip uninstall -y opencv-python opencv-contrib-python 2>/dev/null || true && \
    pip install --no-cache-dir --no-deps \
        opencv-python-headless==4.12.0.88 \
        opencv-contrib-python-headless==4.10.0.84

# Create fake opencv metadata for PaddleX compatibility
RUN SITE_PKG=$(python -c "import site; print(site.getsitepackages()[0])") && \
    CV_VER=$(python -c "import cv2; print(cv2.__version__)") && \
    for PKG in opencv-contrib-python opencv-python; do \
        DIST_NAME=$(echo $PKG | tr '-' '_'); \
        FAKE_DIR="${SITE_PKG}/${DIST_NAME}-${CV_VER}.dist-info"; \
        mkdir -p "$FAKE_DIR"; \
        echo "Metadata-Version: 2.1\nName: ${PKG}\nVersion: ${CV_VER}" > "$FAKE_DIR/METADATA"; \
        echo "pip" > "$FAKE_DIR/INSTALLER"; \
        touch "$FAKE_DIR/RECORD"; \
    done

# Copy application
COPY . .

# Pre-download PaddleX DLA model
RUN python -c "\
import os; \
os.environ['PADDLEX_HOME'] = '/root/.paddlex'; \
os.environ['GLOG_v'] = '0'; \
print('Downloading PP-DocLayout_plus-L model...'); \
from paddleocr import LayoutDetection; \
m = LayoutDetection(model_name='PP-DocLayout_plus-L'); \
print('Model downloaded successfully'); \
" || echo "WARNING: Model download failed. Will retry at runtime."

# Create output directory
RUN mkdir -p /app/output/completed
ENV PYTHONPATH=/app

EXPOSE 8050

HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8050/health')" || exit 1

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8050", "--timeout-keep-alive", "120"]