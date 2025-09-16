import os
import json
import base64
import tempfile
import logging
import urllib.request

from fastapi import FastAPI, Request
from google.cloud import storage, secretmanager
import psycopg2
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Clients
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()

# Load heavy model once at container startup
logger.info("⏳ Loading marker-pdf model (one-time at container start)...")
converter = PdfConverter(artifact_dict=create_model_dict())
logger.info("✅ marker-pdf model loaded successfully.")


def get_project_id():
    proj = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if proj:
        return proj
    try:
        req = urllib.request.Request(
            "http://metadata.google.internal/computeMetadata/v1/project/project-id"
        )
        req.add_header("Metadata-Flavor", "Google")
        with urllib.request.urlopen(req, timeout=2) as resp:
            return resp.read().decode()
    except Exception:
        raise RuntimeError("Project ID not found. Set GCP_PROJECT env var.")


def get_db_url():
    project_id = get_project_id()
    secret_name = "supabase-url"
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")


def ensure_table_exists(db_url: str):
    try:
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS cv_data (
            id BIGSERIAL PRIMARY KEY,
            filename TEXT NOT NULL,
            cv_text TEXT,
            metadata JSONB,
            created_at TIMESTAMPTZ DEFAULT now()
        );
        """)
        conn.commit()
        cur.close()
        conn.close()
        logger.info("✅ Ensured cv_data table exists.")
    except Exception as e:
        logger.warning(f"⚠️ Could not ensure table exists: {e}")


def save_to_db(filename: str, cv_data: str, metadata: dict):
    db_url = get_db_url()
    ensure_table_exists(db_url)
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO cv_data (filename, cv_text, metadata) VALUES (%s, %s, %s)",
        (filename, cv_data, json.dumps(metadata))
    )
    conn.commit()
    cur.close()
    conn.close()
    logger.info(f"✅ File saved to DB: {filename}")


def move_file(bucket_name: str, blob_name: str, new_blob_name: str):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    new_blob = bucket.blob(new_blob_name)

    # Copy then delete
    new_blob.rewrite(blob)
    blob.delete()
    logger.info(f"📂 Moved {blob_name} → {new_blob_name}")


def parse_pubsub_envelope(envelope: dict):
    """Handle Pub/Sub push message format"""
    if "message" in envelope:
        msg = envelope["message"]
        data_b64 = msg.get("data")
        if data_b64:
            return json.loads(base64.b64decode(data_b64).decode("utf-8"))
        return {}
    return envelope


@app.post("/")
async def process_pubsub(request: Request):
    try:
        envelope = await request.json()
        data = parse_pubsub_envelope(envelope)

        bucket_name = data.get("bucket")
        blob_name = data.get("name")
        if not bucket_name or not blob_name:
            logger.error(f"❌ Missing bucket/name in event: {data}")
            return {"status": "bad request"}

        logger.info(f"📥 Received file: {blob_name} in bucket: {bucket_name}")

        # Download PDF locally
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            blob.download_to_filename(tmp.name)
            local_pdf_path = tmp.name

        logger.info(f"📂 Downloaded file: {local_pdf_path}")

        # Convert
        rendered = converter(local_pdf_path)
        cv_data, metadata, images = text_from_rendered(rendered)

        logger.info(f"📝 Extracted markdown for {blob_name} (preview: {cv_data[:200]!r})")

        # Save results
        save_to_db(blob_name, cv_data, metadata)

        # Move original file into "processed/" folder
        processed_path = f"processed/{blob_name}"
        move_file(bucket_name, blob_name, processed_path)

        logger.info(f"✅ Processing complete for {blob_name}")
        return {"status": "ok"}

    except Exception as e:
        logger.exception("❌ Error during processing")
        return {"status": "error", "message": str(e)}
