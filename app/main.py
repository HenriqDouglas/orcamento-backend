import os
import re
import uuid
import hashlib
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_BUCKET = os.getenv("SUPABASE_BUCKET")
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError("Missing SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY env vars")

supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

app = FastAPI(title="orcamento-backend")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in ALLOWED_ORIGINS.split(",")] if ALLOWED_ORIGINS != "*" else ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _sanitize_filename(name: str) -> str:
    name = name.strip()
    name = re.sub(r"[^\w\-. ]+", "_", name)
    name = re.sub(r"\s+", " ", name)
    return name[:120] if len(name) > 120 else name

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/runs/upload")
async def upload_run(
    files: List[UploadFile] = File(...),
    name: Optional[str] = Form(None),
    notes: Optional[str] = Form(None),
    created_by: Optional[str] = Form(None),
):
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")

    run_id = str(uuid.uuid4())
    # cria o run (status queued por padrão)
    supabase.table("runs").insert({
        "id": run_id,
        "name": name,
        "notes": notes,
        "created_by": created_by,
        "status": "queued",
    }).execute()

    uploaded = []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for f in files:
        original = f.filename or "arquivo.xlsx"
        original_s = _sanitize_filename(original)
        file_id = str(uuid.uuid4())

        content = await f.read()
        if not content:
            continue

        sha = hashlib.sha256(content).hexdigest()
        storage_path = f"uploads/{today}/{file_id}_{original_s}"

        # upload para o bucket privado
        try:
            supabase.storage.from_(SUPABASE_BUCKET).upload(
                path=storage_path,
                file=content,
                file_options={"content-type": f.content_type or "application/octet-stream"},
            )
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Storage upload failed: {str(e)}")

        # registra no run_files
        supabase.table("run_files").insert({
            "id": file_id,
            "run_id": run_id,
            "bucket": SUPABASE_BUCKET,
            "storage_path": storage_path,
            "original_name": original,
            "sha256": sha,
            "size_bytes": len(content),
        }).execute()

        uploaded.append({
            "file_id": file_id,
            "bucket": SUPABASE_BUCKET,
            "path": storage_path,
            "original_name": original,
            "sha256": sha,
            "size_bytes": len(content),
        })

    if not uploaded:
        raise HTTPException(status_code=400, detail="All files were empty")

    return {"run_id": run_id, "files": uploaded, "status": "queued"}
