from __future__ import annotations
from pathlib import Path

from libs.ipc.jsonio import read_json, read_jsonl
from libs.stats.delta_writer import StatsDeltaWriter

from .db_ops import FeatureDB
from .extract_basic import extract_basic


class ExtractService:
    def __init__(self, extractor_id: int, db: FeatureDB, stats: StatsDeltaWriter):
        self.extractor_id = extractor_id
        self.db = db
        self.stats = stats

    def process_folder(self, folder: Path):
        print(f"[extractor {self.extractor_id:02d}] start processing '{folder}'", flush=True)
        file_cnt = 0
        error = 0

        for f in folder.iterdir():
            if not f.is_file():
                continue

            if f.suffix == ".json":
                recs = [read_json(f)]
                file_cnt += 1
            elif f.suffix == ".jsonl":
                recs = read_jsonl(f)
                file_cnt += 1
            else:
                continue
            
            for rec in recs:
                try:
                    if rec.get("status") == "ok":
                        feat = extract_basic(rec)
                        self.db.process(feat)
                except Exception as e:
                    print(f"[extractor {self.extractor_id:02d}] ERROR: {e}", flush=True)
                    error += 1
        if error:
            self.stats.write(
                source="extractor",
                counters={
                    "error_count": error,
                    "extract_error": error,
                },
            )
        print(f"[extractor {self.extractor_id:02d}] finish processing '{folder}', {file_cnt} files", flush=True)

