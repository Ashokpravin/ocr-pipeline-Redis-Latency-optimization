"""
Katonic Entry Point — app.py
Katonic runs: uvicorn app:app

FIXES (v3.2 — April 2026):
  1. OpenCV headless + fake metadata for PaddleX
  2. LibreOffice for .docx/.pptx/.xlsx conversion (Java disabled to prevent javaldx crash)
  3. PaddleX DLA model pre-download (prevents runtime FileNotFoundError)
  4. Environment variables for headless container operation
  5. Apt-get conflict resolution for held broken packages
  6. Deployment-specific marker files (prevents API/Worker conflicts)
  7. DEBIAN_FRONTEND forced in subprocess environment
  8. Apt cache cleanup after LibreOffice install (prevents disk bloat)
"""

import subprocess
import sys
import os
import shutil
import site
import glob
import traceback


# =============================================================================
# FIX 7: Force DEBIAN_FRONTEND in ALL subprocess calls
# =============================================================================
def _run(cmd):
    env = os.environ.copy()
    env["DEBIAN_FRONTEND"] = "noninteractive"
    return subprocess.run(cmd, shell=True, capture_output=True, text=True, env=env)


def _get_site_packages():
    try:
        return site.getsitepackages()[0]
    except Exception:
        return "/opt/conda/lib/python3.11/site-packages"


# =============================================================================
# SET ENVIRONMENT VARIABLES BEFORE ANY IMPORTS
# =============================================================================
# LibreOffice: disable Java completely (prevents "failed to launch javaldx")
os.environ.setdefault("SAL_USE_VCLPLUGIN", "gen")
os.environ.setdefault("DEBIAN_FRONTEND", "noninteractive")
if "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = ""

# LibreOffice needs a writable HOME for its user profile directory.
_lo_home = os.environ.get("HOME", "/root")
_lo_profile = os.path.join(_lo_home, ".config", "libreoffice")
try:
    os.makedirs(_lo_profile, exist_ok=True)
    _test = os.path.join(_lo_profile, ".write_test")
    with open(_test, "w") as f:
        f.write("ok")
    os.remove(_test)
except (OSError, PermissionError):
    os.environ["HOME"] = "/tmp"
    os.makedirs("/tmp/.config/libreoffice", exist_ok=True)
    print(f"WARNING: HOME ({_lo_home}) not writable for LibreOffice, using /tmp", flush=True)

# PaddleX: ensure model cache is in a writable location
os.environ.setdefault("PADDLEX_HOME", os.path.join(os.environ["HOME"], ".paddlex"))

# Suppress noisy PaddlePaddle logs in production
os.environ.setdefault("GLOG_v", "0")
os.environ.setdefault("FLAGS_call_stack_level", "0")


def _disable_libreoffice_java():
    """
    Write a LibreOffice user-profile config that disables Java.
    This prevents the "failed to launch javaldx" warning/crash entirely.
    """
    lo_home = os.environ.get("HOME", "/root")
    profile_dir = os.path.join(lo_home, ".config", "libreoffice", "4", "user")
    registry_file = os.path.join(profile_dir, "registrymodifications.xcu")

    try:
        os.makedirs(profile_dir, exist_ok=True)

        write_needed = True
        if os.path.exists(registry_file):
            with open(registry_file, "r") as f:
                if "ooSetupNode" in f.read():
                    write_needed = False

        if write_needed:
            xcu = """<?xml version="1.0" encoding="UTF-8"?>
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
"""
            with open(registry_file, "w") as f:
                f.write(xcu)
            print("  Done: Disabled Java in LibreOffice config", flush=True)
    except Exception as e:
        print(f"  Note: Could not disable LO Java: {e} (non-fatal)", flush=True)


def _predownload_paddlex_model():
    """
    Pre-download the PP-DocLayout_plus-L model so workers don't race to download it.
    """
    paddlex_home = os.environ.get("PADDLEX_HOME", os.path.expanduser("~/.paddlex"))
    model_dir = os.path.join(paddlex_home, "official_models", "PP-DocLayout_plus-L")
    inference_yml = os.path.join(model_dir, "inference.yml")

    if os.path.exists(inference_yml):
        print(f"  Done: Model already present: {model_dir}", flush=True)
        return True

    print(f"  Downloading PP-DocLayout_plus-L to {paddlex_home}...", flush=True)
    
    dl_script = (
        "import os; "
        f"os.environ['PADDLEX_HOME'] = '{paddlex_home}'; "
        "os.environ['GLOG_v'] = '0'; "
        "from paddleocr import LayoutDetection; "
        "m = LayoutDetection(model_name='PP-DocLayout_plus-L'); "
        "print('MODEL_OK'); "
    )
    result = _run(f'{sys.executable} -c "{dl_script}"')

    if result.returncode == 0 and "MODEL_OK" in result.stdout:
        print(f"  Done: Model downloaded successfully", flush=True)
        return True
    else:
        err = result.stderr.strip() if result.stderr else result.stdout.strip()
        err_lines = err.split("\n")[-5:]
        print(f"  FAILED: Model download failed:", flush=True)
        for line in err_lines:
            if line.strip():
                print(f"    {line}", flush=True)
        return False


def _setup():
    os.environ.setdefault("PADDLEX_HOME", "/home/katonic/.paddlex")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    # ==========================================================================
    # FIX 6: Deployment-specific marker file
    # Set DEPLOY_TYPE="worker" in celery_entry.py before calling _setup()
    # This prevents the Celery worker from invalidating the API's marker
    # ==========================================================================
    deploy_type = os.getenv("DEPLOY_TYPE", "api")
    marker = os.path.join(base_dir, f".deps_ok_{deploy_type}")

    if os.path.exists(marker):
        cv2_ok = _run(f'{sys.executable} -c "import cv2"').returncode == 0
        soffice_ok = _run("which soffice").returncode == 0
        paddlex_home = os.environ.get("PADDLEX_HOME", os.path.expanduser("~/.paddlex"))
        model_ok = os.path.exists(os.path.join(paddlex_home, "official_models", "PP-DocLayout_plus-L", "inference.yml"))
        if cv2_ok and soffice_ok and model_ok:
            print(f"[app.py] Dependencies verified via marker: {marker}", flush=True)
            return
        else:
            try: os.remove(marker)
            except OSError: pass

    print("=" * 60, flush=True)
    print(f" CustomOCR - Fixing Dependencies ({deploy_type})", flush=True)
    print("=" * 60, flush=True)

    site_pkg = _get_site_packages()

    # =========================================================
    # STEP 1/6: Remove all OpenCV installations
    # =========================================================
    print("[1/6] Removing all OpenCV installations...", flush=True)
    for pattern in ["cv2", "cv2.cpython*", "opencv_python*", "opencv_contrib_python*", "opencv_python_headless*", "opencv_contrib_python_headless*"]:
        for path in glob.glob(os.path.join(site_pkg, pattern)):
            print(f"  rm {os.path.basename(path)}", flush=True)
            if os.path.isdir(path): shutil.rmtree(path, ignore_errors=True)
            else:
                try: os.remove(path)
                except OSError: pass

    _run("pip uninstall -y opencv-python opencv-contrib-python opencv-python-headless opencv-contrib-python-headless 2>/dev/null")
    print("  Done", flush=True)

    # =========================================================
    # STEP 2/6: Install PaddleOCR without deps
    # =========================================================
    print("[2/6] Installing PaddleOCR (--no-deps)...", flush=True)
    _run("pip install --no-cache-dir --no-deps paddleocr==3.3.2")
    print("  Done", flush=True)

    # =========================================================
    # STEP 3/6: Install headless OpenCV
    # =========================================================
    print("[3/6] Installing headless OpenCV...", flush=True)
    result = _run("pip install --no-cache-dir opencv-python-headless==4.10.0.84 opencv-contrib-python-headless==4.10.0.84")
    if result.returncode != 0:
        _run("pip install --no-cache-dir opencv-python-headless opencv-contrib-python-headless")
    print("  Done", flush=True)

    # =========================================================
    # STEP 4/6: Create fake opencv metadata for PaddleX
    # =========================================================
    print("[4/6] Creating opencv metadata for PaddleX...", flush=True)
    ver_result = _run(f'{sys.executable} -c "import cv2; print(cv2.__version__)"')
    cv_version = ver_result.stdout.strip() if ver_result.returncode == 0 else "4.10.0.84"

    for sp in (site.getsitepackages() if hasattr(site, 'getsitepackages') else [site_pkg]):
        for pkg_name in ["opencv-contrib-python", "opencv-python"]:
            dist_name = pkg_name.replace("-", "_")
            fake_dist = os.path.join(sp, f"{dist_name}-{cv_version}.dist-info")
            try:
                os.makedirs(fake_dist, exist_ok=True)
                with open(os.path.join(fake_dist, "METADATA"), "w") as f:
                    f.write(f"Metadata-Version: 2.1\nName: {pkg_name}\nVersion: {cv_version}\n")
                with open(os.path.join(fake_dist, "INSTALLER"), "w") as f: f.write("pip\n")
                with open(os.path.join(fake_dist, "RECORD"), "w") as f: f.write("")
            except Exception:
                pass
    print("  Done", flush=True)

    # =========================================================
    # STEP 5/6: Install LibreOffice
    # =========================================================
    print("[5/6] Installing LibreOffice...", flush=True)
    soffice_check = _run("which soffice")
    if soffice_check.returncode == 0:
        print(f"  Done: Already installed: {soffice_check.stdout.strip()}", flush=True)
    else:
        sudo_prefix = "sudo -n " if _run("command -v sudo").returncode == 0 else ""
        
        # FIX 5: Aggressively fix held broken packages BEFORE installing
        result = _run(
            f"{sudo_prefix}apt-get update -qq --fix-missing && "
            f"{sudo_prefix}apt-get install -y -f && "
            f"{sudo_prefix}apt-get install -y -qq --no-install-recommends "
            "libreoffice-writer libreoffice-calc libreoffice-impress "
            "libreoffice-common fonts-dejavu-core"
        )
        if result.returncode != 0:
            print("  Minimal install failed, trying full LibreOffice...", flush=True)
            result = _run(f"{sudo_prefix}apt-get install -y -qq libreoffice")

        verify = _run("which soffice")
        if verify.returncode == 0:
            print(f"  Done: Installed: {verify.stdout.strip()}", flush=True)
        else:
            print("  FAILED: LibreOffice install failed. .docx/.pptx/.xlsx will not work.", flush=True)
            if result.stderr:
                print(f"  Error: {result.stderr.strip()[:300]}", flush=True)

        # FIX 8: Clean apt cache to prevent disk bloat (~200MB savings)
        _run(f"{sudo_prefix}rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*")

    # Disable Java in LibreOffice config (prevents javaldx crash)
    _disable_libreoffice_java()

    # Install xvfb for headless rendering (optional, non-critical)
    sudo_prefix = "sudo -n " if _run("command -v sudo").returncode == 0 else ""
    _run(f"{sudo_prefix}apt-get install -y -qq xvfb 2>/dev/null")
    
    # =========================================================
    # STEP 6/6: Pre-download PaddleX DLA model
    # =========================================================
    print("[6/6] Pre-downloading PaddleX DLA model...", flush=True)
    _predownload_paddlex_model()

    # =========================================================
    # VERIFICATION
    # =========================================================
    print("\nVerification:", flush=True)
    r = _run(f'{sys.executable} -c "import cv2; print(cv2.__version__)"')
    print(f"  cv2: {r.stdout.strip()}" if r.returncode == 0 else "  FAILED: cv2", flush=True)

    r = _run("soffice --version")
    print(f"  soffice: {r.stdout.strip()}" if r.returncode == 0 else "  FAILED: soffice not found", flush=True)

    paddlex_home = os.environ.get("PADDLEX_HOME", os.path.expanduser("~/.paddlex"))
    model_ok = os.path.exists(os.path.join(paddlex_home, "official_models", "PP-DocLayout_plus-L", "inference.yml"))
    print(f"  DLA model: {'present' if model_ok else 'MISSING'}", flush=True)

    # Write deployment-specific marker
    with open(marker, "w") as f:
        f.write("ok")
    print(f"\n{'=' * 60}", flush=True)
    print(f" Setup complete! (marker: {os.path.basename(marker)})", flush=True)
    print(f"{'=' * 60}\n", flush=True)


# =============================================
# RUN SETUP BEFORE ANY IMPORTS
# =============================================
_setup()

# =============================================
# CLEAR LRU CACHES
# =============================================
try:
    import importlib.metadata
    if hasattr(importlib.metadata.distributions, 'cache_clear'):
        importlib.metadata.distributions.cache_clear()
except Exception:
    pass

# =============================================
# IMPORT THE REAL FASTAPI APP
# =============================================
app = None
try:
    from ocr_app import app
    print("[app.py] ocr_app loaded successfully", flush=True)
except Exception as e:
    print(f"[app.py] Failed to import ocr_app: {e}", flush=True)
    print(traceback.format_exc(), flush=True)
    from fastapi import FastAPI
    app = FastAPI(title="CustomOCR - Error")
    _err = str(e)
    @app.get("/")
    def show_error():
        return {"status": "import_error", "error": _err}