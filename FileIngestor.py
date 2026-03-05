import os
import sys
import shutil
import subprocess
import logging
from pathlib import Path
from typing import List, Union, Tuple
import json

import fitz  # PyMuPDF
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# --- Arabic Support Imports ---
try:
    import arabic_reshaper
    from bidi.algorithm import get_display
    HAS_ARABIC_SUPPORT = True
except ImportError:
    HAS_ARABIC_SUPPORT = False
    logging.warning("⚠️ Arabic libraries not found. Text may render incorrectly. Run: pip install arabic-reshaper python-bidi")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FileIngestor:
    """
    Handles file ingestion with added support for ARABIC text rendering.
    
    Capabilities:
    1. Office Docs -> PDF (LibreOffice)
    2. Text/Data (JSON/XML/TXT) -> PDF (ReportLab with Arabic support)
    3. PDF -> Images (PyMuPDF)
    4. Images -> Copy
    """

    def __init__(self, base_output_dir: str = "./output"):
        self.base_output_dir = Path(base_output_dir).resolve()
        self.is_windows = sys.platform.startswith("win")
        
        # --- 1. Locate Root Directory ---
        # Helper to find the project root (where 'resources' folder lives)
        self.root_dir = Path(__file__).resolve().parent
        
        # --- 2. Load Configuration ---
        self.dla_vars = {}
        self._load_config()

        # --- 3. Determine LibreOffice Command ---
        self.soffice_cmd = "soffice" # Default fallback
        
        if self.is_windows:
            # Extract path from JSON (e.g., "C:/Program Files/LibreOffice/program/")
            bin_folder = self.dla_vars.get("WIN_PATH_TO_SOFFICE_BIN_FOLDER", "")
            
            if bin_folder:
                # Construct full path: folder + soffice
                # The old script did: folder + "soffice". We should add .exe for safety on Windows.
                # We use Path to handle slashes correctly.
                
                # Check if the path in JSON is relative or absolute
                path_obj = Path(bin_folder)
                if not path_obj.is_absolute():
                     path_obj = self.root_dir / path_obj

                soffice_executable = path_obj / "soffice.exe"
                
                if soffice_executable.exists():
                    self.soffice_cmd = str(soffice_executable)
                else:
                    # Try without .exe extension or exactly as string provided
                    self.soffice_cmd = str(path_obj / "soffice")
                    
                logging.info(f"🔧 Configured LibreOffice Path: {self.soffice_cmd}")
            else:
                logging.warning("⚠️ WIN_PATH_TO_SOFFICE_BIN_FOLDER not found in dla.vars.json. Using system default 'soffice'.")

        # --- 4. Register Fonts ---
        self.font_name = self._register_fonts()

    def _load_config(self):
        """Loads configuration from resources/dla.vars.json"""
        json_file = self.root_dir / "resources" / "dla.vars.json"
        
        if json_file.exists():
            try:
                with open(json_file, 'r') as f:
                    self.dla_vars = json.load(f)
                logging.info(f"✅ Loaded config from {json_file}")
            except Exception as e:
                logging.error(f"Failed to load config file: {e}")
        else:
            logging.warning(f"⚠️ Config file not found at: {json_file}")

    def _register_fonts(self) -> str:
        """Registers a UTF-8 compatible font for Arabic support."""
        font_name = "Courier" # Default fallback
        try:
            # Common path for Arial on Windows
            if self.is_windows:
                font_path = Path("C:/Windows/Fonts/arial.ttf")
            else:
                # Linux/Mac fallback (adjust if needed)
                font_path = Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf")

            if font_path.exists():
                pdfmetrics.registerFont(TTFont('Arial', str(font_path)))
                font_name = "Arial"
                logging.info(f"✅ Loaded font for Arabic support: {font_path}")
            else:
                logging.warning(f"⚠️ Font not found at {font_path}. Arabic characters may not render.")
        except Exception as e:
            logging.error(f"Failed to register font: {e}")
        
        return font_name

    def process_input(self, input_path: Union[str, Path]) -> Tuple[Path, List[str]]:
        input_path = Path(input_path).resolve()
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input file not found: {input_path}")

        # Setup Directories
        project_name = f"{input_path.name}_dla"
        project_dir = self.base_output_dir / project_name
        project_dir.mkdir(parents=True, exist_ok=True)

        pages_dir = project_dir / "pages"
        pages_dir.mkdir(exist_ok=True)

        suffix = input_path.suffix.lower()
        image_paths = []

        try:
            # A. PDF
            if suffix == ".pdf":
                logging.info(f"📄 Processing PDF: {input_path.name}")
                image_paths = self._pdf_to_images_fitz(input_path, pages_dir)

            # B. Office Docs
            elif suffix in [".docx", ".doc", ".pptx", ".ppt", ".xlsx", ".odp", ".odt"]:
                logging.info(f"📑 Converting Office File: {input_path.name}")
                pdf_path = self._convert_office_to_pdf(input_path, project_dir)
                image_paths = self._pdf_to_images_fitz(pdf_path, pages_dir)

            # C. Text/Code (With Arabic Support)
            elif suffix in [".json", ".xml", ".txt", ".csv", ".py", ".md", ".html", ".css", ".js"]:
                logging.info(f"📜 Rendering Text File (Arabic Supported): {input_path.name}")
                pdf_path = self._convert_text_to_pdf(input_path, project_dir)
                image_paths = self._pdf_to_images_fitz(pdf_path, pages_dir)

            # D. Images
            elif suffix in [".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".webp"]:
                logging.info(f"🖼️ Processing Image: {input_path.name}")
                target_path = pages_dir / "page_0.jpg"
                shutil.copy(input_path, target_path)
                image_paths = [str(target_path)]

            else:
                raise ValueError(f"Unsupported file type: {suffix}")

        except Exception as e:
            logging.error(f"❌ Ingestion failed for {input_path.name}: {e}")
            raise

        return project_dir, image_paths

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _convert_office_to_pdf(self, input_path: Path, out_dir: Path) -> Path:
        expected_pdf = out_dir / input_path.with_suffix(".pdf").name
        if expected_pdf.exists(): return expected_pdf
        out_dir.mkdir(parents=True, exist_ok=True)
        
        cmd = [self.soffice_cmd, "--headless", "--norestore", "--convert-to", "pdf", "--outdir", str(out_dir), str(input_path)]
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"LibreOffice failed: {e.stderr.decode()}")
        
        if not expected_pdf.exists():
            raise FileNotFoundError(f"LibreOffice failed to create: {expected_pdf}")
        return expected_pdf

    def _convert_text_to_pdf(self, input_path: Path, out_dir: Path) -> Path:
        output_pdf = out_dir / input_path.with_suffix(".pdf").name
        if output_pdf.exists(): return output_pdf
        
        c = canvas.Canvas(str(output_pdf), pagesize=letter)
        width, height = letter
        margin = 40
        y = height - margin
        line_height = 14  # Slightly larger for Arabic legibility
        
        try:
            with open(input_path, "r", encoding="utf-8", errors="replace") as f:
                lines = f.readlines()
                
            c.setFont(self.font_name, 10)
            
            for line in lines:
                text_line = line.strip()
                
                # --- ARABIC LOGIC START ---
                if HAS_ARABIC_SUPPORT and text_line:
                    # 1. Reshape: Connects isolated letters (e.g., ب ت -> بت)
                    reshaped_text = arabic_reshaper.reshape(text_line)
                    # 2. Bidi: Reorders RTL logic (e.g., CBA -> ABC visual)
                    bidi_text = get_display(reshaped_text)
                    
                    # For strict RTL (Right-Alignment), calculate X position
                    # But for code/mixed files, keeping left-alignment is often safer
                    # to maintain code structure. We simply draw the fixed text.
                    clean_line = bidi_text
                else:
                    clean_line = text_line.replace("\t", "    ")
                # --- ARABIC LOGIC END ---

                if y < margin: 
                    c.showPage()
                    c.setFont(self.font_name, 10)
                    y = height - margin
                
                c.drawString(margin, y, clean_line)
                y -= line_height
            
            c.save()
            
        except Exception as e:
            logging.error(f"Failed to render text to PDF: {e}")
            raise
            
        return output_pdf

    def _pdf_to_images_fitz(self, pdf_path: Path, output_dir: Path, dpi: int = 200) -> List[str]:
        try:
            doc = fitz.open(pdf_path)
        except Exception as e:
            raise ValueError(f"Could not open PDF {pdf_path}: {e}")

        image_paths = []
        existing = list(output_dir.glob("page_*.jpg"))
        
        # Smart Resume
        if len(existing) == len(doc) and len(doc) > 0:
             logging.info("   -> Images already extracted.")
             return sorted([str(p) for p in existing], key=lambda x: int(Path(x).stem.split('_')[1]))

        mat = fitz.Matrix(dpi/72, dpi/72)
        logging.info(f"   -> Extracting {len(doc)} pages...")

        for i, page in enumerate(doc):
            save_path = output_dir / f"page_{i}.jpg"
            page.get_pixmap(matrix=mat).save(save_path)
            image_paths.append(str(save_path))
            
        doc.close()
        return image_paths

if __name__ == "__main__":
    # Test Block
    ingestor = FileIngestor("./output")
    print(f"Ingestor initialized. Font: {ingestor.font_name}")
