"""
MarkdownEnricher.py  —  MODIFIED FILE
================================================================================
CHANGES vs original:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CRITICAL BUG FIX — Race condition in OCR failover (Critical Issue Report #1)

OLD (BROKEN — line 104 in original):
    except Exception as e:
        self.ocr = OCR("meta-llama/llama-4-maverick-17b-128e-instruct")  # ← SHARED STATE MUTATION
        return f"\n{self.ocr.inference_model(obj_type, str(target_crop))}\n"

When multiple threads run _process_single_match() simultaneously and ONE fails,
it replaces self.ocr for ALL other threads mid-flight.
Other threads are now building Qwen API payloads but sending them to the Llama
endpoint (because self.ocr.model_url was just changed).
Result: corrupted figure/table OCR in the final markdown, or AttributeError crash.

NEW (FIXED):
    except Exception as e:
        backup = OCR("meta-llama/llama-4-maverick-17b-128e-instruct")  # ← THREAD-LOCAL
        return f"\n{backup.inference_model(obj_type, str(target_crop))}\n"

self.ocr is NEVER modified from inside a thread.
Each thread that needs a fallback creates its own local OCR instance.

Everything else (placeholder regex, path resolution, ThreadPoolExecutor,
as_completed logic, replacement callback) is IDENTICAL to the original.
================================================================================
"""

import re
from pathlib import Path
from OCR import OCR
from concurrent.futures import ThreadPoolExecutor, as_completed


class MarkdownEnricher:
    """
    Post-processing step:
    1. Parses rough Markdown from main OCR pass.
    2. Finds placeholders like "PAGE 1 figure_0: HERE".
    3. Locates corresponding cropped image from DLA.
    4. Runs specialised OCR on that crop.
    5. Injects result back into the Markdown.
    """

    def __init__(self, base_path: Path, ocr_engine: OCR, max_workers: int = 1):
        self.base_path   = base_path
        self.cropped_dir = base_path / "cropped_objects"
        self.ocr         = ocr_engine     # read-only — NEVER modified from threads
        self.max_workers = max_workers

        # Matches tags like "PAGE 5 figure_2: HERE"
        self.placeholder_pattern = re.compile(
            r"page\s+(\d+)\s+(figure|table)(?:[^\d\n]*(\d+))?.*?here",
            re.IGNORECASE
        )

    def enrich(self, markdown_content: str) -> str:
        print(f"\n--- Enriching Markdown (Workers: {self.max_workers}) ---")

        matches = list(self.placeholder_pattern.finditer(markdown_content))

        if not matches:
            print("No figures or tables found to enrich.")
            return markdown_content

        print(f"Found {len(matches)} items to process.")

        replacements = {}

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_match = {
                executor.submit(self._process_single_match, m): m.group(0)
                for m in matches
            }
            for future in as_completed(future_to_match):
                original_tag = future_to_match[future]
                try:
                    ocr_result = future.result()
                    replacements[original_tag] = ocr_result
                except Exception as e:
                    print(f"Error processing {original_tag}: {e}")
                    replacements[original_tag] = f"> **Error: {e}**"

        def replacement_callback(match):
            return replacements.get(match.group(0), match.group(0))

        return self.placeholder_pattern.sub(replacement_callback, markdown_content)

    def _process_single_match(self, match) -> str:
        """
        Worker function: resolves path and runs specialised OCR on a crop.

        CRITICAL FIX (Issue Report #1):
        The fallback OCR object `backup` is created as a LOCAL variable.
        self.ocr is NEVER reassigned from inside this method.
        """
        page_num  = match.group(1)
        obj_type  = match.group(2).lower()
        obj_index = match.group(3)

        page_folder_name = f"page_{page_num}.jpg"
        target_crop = (
            self.cropped_dir
            / page_folder_name
            / obj_type
            / f"{obj_type}_{obj_index}.png"
        )

        # Fallback for plural/singular folder naming inconsistency
        if not target_crop.exists():
            target_crop = (
                self.cropped_dir
                / page_folder_name
                / obj_type.rstrip("s")
                / f"{obj_type}_{obj_index}.png"
            )

        if not target_crop.exists():
            return (
                f"\n> **[Missing Crop] Could not find image for "
                f"{obj_type} {obj_index}**\n"
            )

        print(f"    - Processing {target_crop.name}...")

        # Read-only reference to primary engine
        primary_engine = self.ocr

        try:
            return f"\n{primary_engine.inference_model(obj_type, str(target_crop))}\n"

        except Exception as e:
            print(f"  ⚠️ Primary model failed for {obj_type}. Switching to backup (thread-local)...")
            try:
                # ----------------------------------------------------------------
                # FIXED: backup is LOCAL to this thread.
                # self.ocr is NEVER reassigned here.
                # ----------------------------------------------------------------
                backup = OCR("meta-llama/llama-4-maverick-17b-128e-instruct")
                return f"\n{backup.inference_model(obj_type, str(target_crop))}\n"

            except Exception as e_backup:
                error_msg = (
                    f"\n> **[OCR Failed] Both Primary and Backup models failed "
                    f"for {obj_type}. Final Error: {str(e_backup)}**\n"
                )
                print(f"  ❌ {error_msg}")
                return error_msg