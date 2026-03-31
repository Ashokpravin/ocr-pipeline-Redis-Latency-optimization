"""
PageProcessor.py  —  MODIFIED FILE
================================================================================
CHANGES vs original:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. CRITICAL BUG FIX — Race condition in OCR failover (Critical Issue Report #1)
   OLD (BROKEN):
     def process_page(path):
         except Exception:
             self.ocr_engine = OCR("llama...")   # ← MUTATES SHARED STATE
             return self.ocr_engine.inference_model(...)
   
   When 8 threads run simultaneously and ONE fails, it replaces self.ocr_engine
   for ALL other threads mid-flight. Other threads are now sending Qwen payloads
   to a Llama endpoint. Result: corrupted output or AttributeError crash.
   
   NEW (FIXED):
     def process_page(path):
         except Exception:
             backup = OCR("llama...")   # ← LOCAL to this thread only
             return backup.inference_model(...)
   
   Each thread is fully independent. self.ocr_engine is NEVER modified.

2. DEFAULT max_workers: 5 → 24
   The load test shows model optimal concurrency is C=24.
   With max_workers=5 only 21% of model capacity was used.
   With max_workers=24 model utilization = 100%.

3. process_and_mask() PARALLELISED
   OLD: sequential for loop — one page masked at a time
   NEW: ThreadPoolExecutor — all pages masked concurrently
   CPU-bound PIL operations run in parallel across available cores.

Everything else (generate_final_markdown, enricher, OCR logic,
file handling) is IDENTICAL to the original.
================================================================================
"""

import shutil
import re
import logging
import os
from pathlib import Path
from typing import Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

from ContentMasker import ContentMasker
from MarkdownEnricher import MarkdownEnricher
from OCR import OCR
import img2pdf

logger = logging.getLogger(__name__)


class PageProcessor:

    def __init__(self, base_path: str, max_workers: int = 24):
        """
        Args:
            base_path:   Per-job project directory path.
            max_workers: Concurrency for masking AND OCR inference.
                         Default changed 5 → 24 (model optimal C=24 from load test).
        """
        self.base_path   = Path(base_path).resolve()
        self.max_workers = max_workers

        self.masker = ContentMasker()

        try:
            print(">>> Loading Primary Model (Qwen)...")
            self.ocr_engine = OCR("Qwen/Qwen3-VL-235B-A22B-Instruct")
        except Exception as e:
            logging.warning(f"⚠️ Primary model failed to load: {e}")
            print(">>> Switching to Fallback Model (Llama)...")
            self.ocr_engine = OCR("meta-llama/llama-4-maverick-17b-128e-instruct")

        # Pass worker count to enricher for parallel figure/table OCR
        self.enricher = MarkdownEnricher(self.base_path, self.ocr_engine, max_workers)

    # =========================================================================
    # STEP 3a — Masking (PARALLELISED)
    # =========================================================================

    def process_and_mask(self):
        """
        Creates masked images (white boxes over figures/tables) for the main OCR pass.
        CHANGED: Runs in parallel via ThreadPoolExecutor.
        """
        input_dir  = self.base_path / "pages"
        output_dir = self.base_path / "processed_pages"
        ignore_dir = self.base_path / "ignore_bounding_box"

        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)

        page_files = sorted(
            input_dir.glob("page_*.jpg"),
            key=lambda x: int(re.search(r"page_(\d+)", x.name).group(1))
        )

        if not page_files:
            logger.warning(f"[PageProcessor] No images found in {input_dir}")
            return

        n_pages      = len(page_files)
        mask_workers = min(self.max_workers, os.cpu_count() or 4)
        logger.info(f"[PageProcessor] Masking {n_pages} pages (workers={mask_workers})...")

        def _mask_one_page(img_path: Path):
            match = re.search(r"page_(\d+)", img_path.name)
            if not match:
                return
            page_num  = int(match.group(1))
            meta_path = ignore_dir / f"page_{page_num}" / "non_text_pairs.json"
            save_path = output_dir / img_path.name
            masked    = self.masker.process_page(img_path, meta_path, page_num)
            masked.save(save_path, "PNG")

        with ThreadPoolExecutor(
            max_workers=mask_workers,
            thread_name_prefix="mask"
        ) as executor:
            futures = {executor.submit(_mask_one_page, p): p for p in page_files}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    page_path = futures[future]
                    logger.error(
                        f"[PageProcessor] Masking failed for {page_path.name}: {e}"
                    )
                    raise

        logger.info(f"[PageProcessor] Masking complete ({n_pages} pages).")

    def create_intermediate_pdf(self) -> Optional[Path]:
        img_dir    = self.base_path / "processed_pages"
        pdf_name   = f"{self.base_path.name}_masked.pdf"
        output_pdf = self.base_path / pdf_name
        images     = sorted(
            [str(p) for p in img_dir.glob("*.jpg")],
            key=lambda x: int(re.search(r"page_(\d+)", x).group(1))
        )
        if not images:
            return None
        with open(output_pdf, "wb") as f:
            f.write(img2pdf.convert(images))
        return output_pdf

    # =========================================================================
    # STEP 3b — OCR + Enrichment
    # =========================================================================

    def generate_final_markdown(self) -> Path:
        """
        Runs Main OCR pass (parallel), Enrichment pass (parallel), saves result.

        CRITICAL FIX (Issue Report #1):
        The fallback OCR object is created as a LOCAL variable `backup` inside
        the thread worker. self.ocr_engine is NEVER modified from a thread.
        """
        processed_dir = self.base_path / "processed_pages"
        image_files   = sorted(
            processed_dir.glob("*.jpg"),
            key=lambda x: int(re.search(r"page_(\d+)", x.name).group(1))
        )

        logger.info(
            f"[PageProcessor] OCR pass — "
            f"{len(image_files)} pages, workers={self.max_workers}"
        )

        # Read-only reference to primary engine — NEVER reassigned
        primary_engine = self.ocr_engine

        def process_page(path: Path) -> str:
            logger.info(f"  > Sending {path.name}...")
            try:
                return primary_engine.inference_model("markdown", str(path))
            except Exception as e:
                logger.warning(
                    f"  ⚠️ Primary model failed for {path.name}: {e}. "
                    f"Switching to backup (thread-local)..."
                )
                try:
                    # --------------------------------------------------------
                    # FIXED: backup is LOCAL to this thread.
                    # primary_engine (self.ocr_engine) is never mutated.
                    # --------------------------------------------------------
                    backup = OCR("meta-llama/llama-4-maverick-17b-128e-instruct")
                    return backup.inference_model("markdown", str(path))
                except Exception as e_backup:
                    err = (
                        f"> **[OCR Failed] Both models failed for {path.name}. "
                        f"Error: {str(e_backup)}**"
                    )
                    logger.error(f"  ❌ {err}")
                    return err

        # executor.map preserves order of input list
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            results = list(executor.map(process_page, image_files))

        full_content  = "\n\n---\n\n".join(results)
        final_content = self.enricher.enrich(full_content)
        final_content = final_content.replace("```", "")

        original_filename = self.base_path.name.replace("_dla", "")
        final_md_path     = self.base_path / f"{original_filename}.md"

        with open(final_md_path, "w", encoding="utf-8") as f:
            f.write(final_content)

        logger.info(f"[PageProcessor] SUCCESS → {final_md_path}")
        return final_md_path