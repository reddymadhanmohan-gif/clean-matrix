from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Form
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, JSONResponse
import shutil, os, uuid
from pipeline import run_pipeline, load_dataset, auto_eda, compute_quality_score
from database import (
    signup_user, login_user, save_history_record,
    get_history, generate_api_key, get_api_key
)

app = FastAPI(title="Clean Master v2")
os.makedirs("uploads", exist_ok=True)
os.makedirs("outputs", exist_ok=True)

# ── Session store ─────────────────────────────────────────────────────────────
sessions = {}

def get_session(request: Request):
    token = request.cookies.get("session")
    if token and token in sessions:
        return sessions[token]
    return None

# ── Pages ─────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    if not get_session(request):
        return RedirectResponse(url="/login")
    with open("index.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/login", response_class=HTMLResponse)
async def login_page():
    with open("login.html", "r", encoding="utf-8") as f:
        return HTMLResponse(f.read())

@app.get("/logout")
async def logout(request: Request):
    token = request.cookies.get("session")
    if token and token in sessions:
        del sessions[token]
    response = RedirectResponse(url="/login")
    response.delete_cookie("session")
    return response

# ── Auth ──────────────────────────────────────────────────────────────────────
@app.post("/api/signup")
async def api_signup(username: str = Form(...), password: str = Form(...)):
    if len(username) < 3:
        raise HTTPException(400, "Username must be at least 3 characters")
    if len(password) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")
    result = signup_user(username, password)
    if not result.get("success"):
        raise HTTPException(400, result.get("message", "Signup failed"))
    login_result = login_user(username, password)
    token = uuid.uuid4().hex
    sessions[token] = {"user_id": str(login_result["user"]["id"]), "username": username}
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie("session", token, httponly=True, max_age=86400)
    return response

@app.post("/api/login")
async def api_login(username: str = Form(...), password: str = Form(...)):
    result = login_user(username, password)
    if not result.get("success"):
        raise HTTPException(401, result.get("message", "Invalid credentials"))
    token = uuid.uuid4().hex
    sessions[token] = {"user_id": str(result["user"]["id"]), "username": username}
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie("session", token, httponly=True, max_age=86400)
    return response

@app.get("/api/me")
async def whoami(request: Request):
    session = get_session(request)
    if not session:
        raise HTTPException(401, "Not logged in")
    return {"username": session["username"], "user_id": session["user_id"]}

# ── EDA (called instantly after upload) ──────────────────────────────────────
@app.post("/api/eda")
async def eda_endpoint(request: Request, file: UploadFile = File(...)):
    if not get_session(request):
        raise HTTPException(401, "Not logged in")
    job_id  = uuid.uuid4().hex[:8]
    in_path = f"uploads/{job_id}_{file.filename}"
    with open(in_path, "wb") as f_out:
        shutil.copyfileobj(file.file, f_out)
    try:
        df = load_dataset(in_path)
        qs = compute_quality_score(df)
        eda = auto_eda(df)
        preview = df.head(10).fillna("").astype(str).to_dict("records")
        cols    = list(df.columns)
        num_cols = df.select_dtypes(include="number").columns.tolist()
        return {
            "filename"    : file.filename,
            "rows"        : int(df.shape[0]),
            "cols"        : int(df.shape[1]),
            "numeric_cols": len(num_cols),
            "missing"     : int(df.isnull().sum().sum()),
            "duplicates"  : int(df.duplicated().sum()),
            "quality"     : qs,
            "eda"         : eda,
            "preview"     : preview,
            "columns"     : cols,
            "num_cols"    : num_cols,
            "tmp_path"    : in_path,   # reused by /clean
        }
    except Exception as e:
        if os.path.exists(in_path): os.remove(in_path)
        raise HTTPException(500, str(e))

# ── Clean ─────────────────────────────────────────────────────────────────────
@app.post("/api/clean")
async def clean_endpoint(request: Request):
    session = get_session(request)
    if not session:
        raise HTTPException(401, "Not logged in")
    body = await request.json()
    in_path       = body.get("tmp_path")
    filename      = body.get("filename", "data")
    impute_method = body.get("impute_method", "knn")
    knn_neighbors = int(body.get("knn_neighbors", 5))
    contamination = float(body.get("contamination", 0.05))
    scale         = bool(body.get("scale", False))
    encode        = bool(body.get("encode", False))
    encode_method = body.get("encode_method", "label")
    run_automl    = bool(body.get("run_automl", False))
    target_col    = body.get("target_col") or None

    if not in_path or not os.path.exists(in_path):
        raise HTTPException(400, "File not found — please re-upload")

    out_name = os.path.splitext(filename)[0] + "_cleaned.csv"
    job_id   = uuid.uuid4().hex[:8]
    out_path = f"outputs/{job_id}_{out_name}"

    try:
        df_clean, stats, _ = run_pipeline(
            filepath      = in_path,
            output_dir    = "outputs",
            impute_method = impute_method,
            knn_neighbors = knn_neighbors,
            contamination = contamination,
            scale         = scale,
            encode        = encode,
            encode_method = encode_method,
            run_automl    = run_automl,
            target_col    = target_col,
        )
        df_clean.to_csv(out_path, index=False)
        stats["filename"] = filename
        # save history
        save_history_record(session["user_id"], {
            "user_id"         : session["user_id"],
            "filename"        : filename,
            "original_rows"   : stats["original_rows"],
            "cleaned_rows"    : stats["cleaned_rows"],
            "missing_filled"  : stats["missing_values"],
            "outliers_removed": stats["outliers_removed"],
            "pre_score"       : stats["pre_quality_score"]["score"],
            "post_score"      : stats["post_quality_score"]["score"],
            "impute_method"   : impute_method,
        })
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        if os.path.exists(in_path): os.remove(in_path)

    return {
        "status"          : "success",
        "original_rows"   : stats["original_rows"],
        "cleaned_rows"    : stats["cleaned_rows"],
        "missing_filled"  : stats["missing_values"],
        "outliers_removed": stats["outliers_removed"],
        "duplicates_removed": stats.get("duplicates_removed", 0),
        "pre_score"       : stats["pre_quality_score"]["score"],
        "post_score"      : stats["post_quality_score"]["score"],
        "pre_grade"       : stats["pre_quality_score"]["grade"],
        "post_grade"      : stats["post_quality_score"]["grade"],
        "outlier_info"    : stats.get("outlier_info", []),
        "automl"          : stats.get("automl", {}),
        "missing_by_col"  : stats.get("eda", {}).get("missing_by_col", {}),
        "download_url"    : f"/download/{job_id}_{out_name}",
    }

# ── History ───────────────────────────────────────────────────────────────────
@app.get("/api/history")
async def history_endpoint(request: Request):
    session = get_session(request)
    if not session:
        raise HTTPException(401, "Not logged in")
    return {"history": get_history(session["user_id"])}

# ── API key ───────────────────────────────────────────────────────────────────
@app.get("/api/key")
async def get_key(request: Request):
    session = get_session(request)
    if not session:
        raise HTTPException(401, "Not logged in")
    key = get_api_key(session["user_id"])
    return {"key": key}

@app.post("/api/key/generate")
async def gen_key(request: Request):
    session = get_session(request)
    if not session:
        raise HTTPException(401, "Not logged in")
    key = generate_api_key(session["user_id"])
    return {"key": key}

# ── Download ──────────────────────────────────────────────────────────────────
@app.get("/download/{filename}")
async def download(filename: str, request: Request):
    if not get_session(request):
        raise HTTPException(401, "Not logged in")
    path = f"outputs/{filename}"
    if not os.path.exists(path):
        raise HTTPException(404, "File not found")
    return FileResponse(path, media_type="text/csv", filename=filename)
