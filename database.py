"""
database.py — Clean Master v2
Matches actual users table schema:
  id (uuid), username (text), password (text), created_at (timestamp)
"""

import hashlib, os, secrets
from supabase import create_client

def _client():
    try:
        import streamlit as st
        url = st.secrets["SUPABASE_URL"]
        key = st.secrets["SUPABASE_KEY"]
    except Exception:
        from dotenv import load_dotenv
        load_dotenv()
        url = os.getenv("SUPABASE_URL", "")
        key = os.getenv("SUPABASE_KEY", "")
    return create_client(url, key)

def _hash(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

# ─── Auth ──────────────────────────────────────────────────────────────────────
def signup_user(username: str, password: str) -> dict:
    try:
        sb = _client()
        existing = sb.table("users").select("id").eq("username", username).execute()
        if existing.data:
            return {"success": False, "message": "Username already taken."}
        sb.table("users").insert({
            "username": username,
            "password": _hash(password)
        }).execute()
        return {"success": True}
    except Exception as e:
        return {"success": False, "message": str(e)}

def login_user(username: str, password: str) -> dict:
    try:
        sb = _client()
        res = (
            sb.table("users")
            .select("*")
            .eq("username", username)
            .eq("password", _hash(password))
            .execute()
        )
        if not res.data:
            return {"success": False, "message": "Invalid username or password."}
        return {"success": True, "user": res.data[0]}
    except Exception as e:
        return {"success": False, "message": str(e)}

# ─── Dataset records ───────────────────────────────────────────────────────────
def save_dataset_record(user_id, filename, rows, cols):
    try:
        sb = _client()
        res = sb.table("datasets").insert({
            "user_id": str(user_id),
            "filename": filename,
            "rows": rows,
            "cols": cols
        }).execute()
        return res.data[0]["id"] if res.data else None
    except Exception:
        return None

def save_processed_record(dataset_id, user_id, filename, out_filename, missing, outliers):
    try:
        sb = _client()
        sb.table("processed_files").insert({
            "dataset_id": dataset_id,
            "user_id": str(user_id),
            "original_filename": filename,
            "cleaned_filename": out_filename,
            "missing_filled": missing,
            "outliers_removed": outliers
        }).execute()
    except Exception:
        pass

# ─── Processing history ────────────────────────────────────────────────────────
def save_history_record(user_id, record: dict):
    try:
        import json
        sb = _client()
        sb.table("processing_history").insert({
            "user_id": str(user_id),
            "filename": record.get("filename", ""),
            "original_rows": record.get("original_rows", 0),
            "cleaned_rows": record.get("cleaned_rows", 0),
            "missing_filled": record.get("missing_values", 0),
            "outliers_removed": record.get("outliers_removed", 0),
            "pre_score": record.get("pre_quality_score", {}).get("score", 0),
            "post_score": record.get("post_quality_score", {}).get("score", 0),
            "impute_method": record.get("impute_method", "knn"),
            "stats_json": json.dumps(record),
        }).execute()
    except Exception:
        pass

def get_history(user_id) -> list:
    try:
        sb = _client()
        res = (
            sb.table("processing_history")
            .select("*")
            .eq("user_id", str(user_id))
            .order("created_at", desc=True)
            .limit(50)
            .execute()
        )
        return res.data or []
    except Exception:
        return []

def delete_history_record(record_id):
    try:
        sb = _client()
        sb.table("processing_history").delete().eq("id", record_id).execute()
    except Exception:
        pass

# ─── API Keys ──────────────────────────────────────────────────────────────────
def generate_api_key(user_id) -> str:
    try:
        sb = _client()
        key = "cm_" + secrets.token_hex(24)
        sb.table("api_keys").upsert({
            "user_id": str(user_id),
            "api_key": key,
            "active": True
        }).execute()
        return key
    except Exception:
        return ""

def get_api_key(user_id) -> str:
    try:
        sb = _client()
        res = (
            sb.table("api_keys")
            .select("api_key")
            .eq("user_id", str(user_id))
            .eq("active", True)
            .execute()
        )
        return res.data[0]["api_key"] if res.data else ""
    except Exception:
        return ""

def verify_api_key(key: str) -> dict:
    try:
        sb = _client()
        res = (
            sb.table("api_keys")
            .select("user_id")
            .eq("api_key", key)
            .eq("active", True)
            .execute()
        )
        if res.data:
            user_res = sb.table("users").select("*").eq("id", res.data[0]["user_id"]).execute()
            return {"valid": True, "user": user_res.data[0] if user_res.data else {}}
        return {"valid": False}
    except Exception:
        return {"valid": False}