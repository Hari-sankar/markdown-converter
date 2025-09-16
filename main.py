import os
import json
import tempfile
import base64

from flask import Flask, request
from google.cloud import storage, secretmanager
import psycopg2
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import text_from_rendered
import urllib.request

app = Flask(__name__)

# Clients
storage_client = storage.Client()
secret_client = secretmanager.SecretManagerServiceClient()

# Load heavy models once (container startup)
print("⏳ Loading marker-pdf models (this happens once per container)...")
converter = PdfConverter(artifact_dict=create_model_dict())
print("✅ marker-pdf models loaded")

def get_project_id():
    proj = os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if proj:
        return proj
    # Try metadata server (when running on GCP)
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
    """Fetch Supabase DB URL from Secret Manager (secret name: 'supabase-url')."""
    project_id = get_project_id()
    secret_name = "supabase-url"
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = secret_client.access_secret_version(request={"name": secret_path})
    return response.payload.data.decode("UTF-8")

def ensure_table_exists(db_url):
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
        print("✅ Ensured cv_data table exists (or was already present).")
    except Exception as e:
        print("⚠️ Could not ensure table exists:", str(e))

def save_to_db(filename, cv_data, metadata):
    db_url = get_db_url()
    # ensure table exists on first use (best-effort)
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
    print(f"✅ File saved to DB: {filename}")

def move_file(bucket_name, blob_name, new_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Copy file to new location
    new_blob = bucket.blob(new_blob_name)
    new_blob.rewrite(blob)

    # Delete original
    blob.delete()

    print(f"Moved {blob_name} → {new_blob_name}")


def parse_pubsub_envelope(envelope):
    """Support Pub/Sub push body structure and raw GCS notification JSON."""
    # Pub/Sub push format: { "message": { "data": "<base64 json>", ... } }
    if "message" in envelope:
        msg = envelope["message"]
        data_b64 = msg.get("data")
        if data_b64:
            payload = json.loads(base64.b64decode(data_b64).decode("utf-8"))
            return payload
        # no data -> fallback to attributes?
        return {}
    # If GCS notification posted the object directly (rare), check keys
    return envelope

@app.route("/", methods=["POST"])
def process():
    envelope = request.get_json(silent=True)
    if not envelope:
        print("❌ No JSON body received")
        return ("Bad Request: no JSON", 400)

    try:
        data = parse_pubsub_envelope(envelope)
        bucket_name = data.get("bucket")
        blob_name = data.get("name")
        if not bucket_name or not blob_name:
            print("❌ Received message does not contain bucket/name:", data)
            return ("Bad Request: missing bucket/name", 400)

        print(f"📂 Received event for file: {blob_name} (bucket: {bucket_name})")

        # Download the PDF to a temp file
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            blob.download_to_filename(tmp.name)
            local_pdf_path = tmp.name

        print(f"📥 Downloaded file locally: {blob_name} -> {local_pdf_path}")

        # Convert once per request (model already loaded)
        rendered = converter(local_pdf_path)

        cv_data, metadata, images = text_from_rendered(rendered)

        print(f"📝 Extracted markdown from: {blob_name}")
        print(f"🔎 Preview (first 200 chars): {cv_data[:200]!r}")

        # Save to DB
        save_to_db(blob_name, cv_data, metadata)

        processed_path = f"rsd-parser/md-converted/{blob_name}"
        move_file(bucket, blob_name, processed_path)
        print(f"✅ File {blob_name} processed and moved to {processed_path}")


        # Optionally: upload images to GCS or Supabase storage (not implemented here)
        print(f"🎯 Processing complete for file: {blob_name}")
        return ("OK", 200)

    except Exception as exc:
        print("❌ Error processing file:", str(exc))
        # Return 500 so Pub/Sub may retry (if transient). Adjust as needed.
        return (f"Internal Server Error: {str(exc)}", 500)

if __name__ == "__main__":
    # useful for local testing
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
