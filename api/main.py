from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import sqlite3
import os

app = FastAPI(title="API SIRENE RNA BAN")

# --- AJOUT DU CORS ICI ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Autorise toutes les interfaces web a interroger l'API
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_GOLD_DIR = os.path.join(BASE_DIR, "data", "parquet", "gold")
DB_DIR = os.path.join(BASE_DIR, "duckdb")
INDEX_HTML_PATH = os.path.join(BASE_DIR, "index.html").replace('\\', '/')
GOLDEN_RECORD_PATH = os.path.join(PARQUET_GOLD_DIR, "golden_record.parquet").replace('\\', '/')
STATS_VIEW_PATH = os.path.join(PARQUET_GOLD_DIR, "stats_view.parquet").replace('\\', '/')
CATALOG_DB_PATH = os.path.join(DB_DIR, "catalog.db")


@app.get("/")
async def read_index():
    return FileResponse(INDEX_HTML_PATH)

@app.get("/api/v1/ping")
async def healthcheck():
    return {"status": "ok"}

@app.get("/api/v1/siret/{siret}")
async def get_siret(siret: str):
    if len(siret) != 14 or not siret.isdigit():
        return JSONResponse(
            status_code=400,
            content={
                "error": "INVALID FORMAT", 
                "message": "Le siret doit contenir 14 chiffres."
            }
        )
        
    query = f"""
        SELECT * FROM read_parquet('{GOLDEN_RECORD_PATH}')
        WHERE siret = '{siret}'
    """
    
    try:
        # Ouverture d'une connexion isolée en lecture seule pour le multi-threading
        with duckdb.connect(database=':memory:', read_only=False) as conn:
            result = conn.execute(query).fetchone()
        
        if not result:
            return JSONResponse(
                status_code=404,
                content={
                    "error": "SIRET NOT FOUND", 
                    "message": f"Le siret {siret} est inconnu.",
                    "input": siret
                }
            )
            
        response_data = {
            "identity": {
                "siret": result[0],
                "nom_raison_sociale": result[2],
                "enseigne": None,
                "status": result[1]
            },
            "asso_id": {
                "id_rna": result[5] if result[5] else None
            },
            "location": {
                "code_postal": result[3],
                "commune": result[4],
                "latitude": result[6],
                "longitude": result[7],
                "is_ban_validated": result[8]
            }
        }
        return response_data
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/search")
async def search(q: str, dept: str = None, postal_code: str = None):
    try:
        conn = sqlite3.connect(CATALOG_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        base_query = "SELECT siret, name, city, is_association FROM search_view WHERE search_view MATCH ?"
        params = [q]
        
        if dept:
            base_query += " AND postal_code LIKE ?"
            params.append(f"{dept}%")
        elif postal_code:
            base_query += " AND postal_code = ?"
            params.append(postal_code)
            
        base_query += " LIMIT 10"
        
        cursor.execute(base_query, params)
        rows = cursor.fetchall()
        
        results = [dict(row) for row in rows]
        for r in results:
            r['is_association'] = True if r['is_association'] == 'true' else False
            
        return {
            "query": q,
            "count": len(results),
            "results": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        conn.close()

@app.get("/api/v1/stats/{postal_code}")
def get_stats(postal_code: str):
    query = f"""
        SELECT * FROM read_parquet('{STATS_VIEW_PATH}')
        WHERE code_postal = '{postal_code}'
    """
    
    try:
        # Ouverture d'une connexion isolée en lecture seule pour le multi-threading
        with duckdb.connect(database=':memory:', read_only=False) as conn:
            result = conn.execute(query).fetchone()
        
        if not result:
            return JSONResponse(
                status_code=404,
                content={"error": "Not Found", "message": "Aucune donnee pour ce code postal."}
            )
            
        return {
            "zone": result[0],
            "total_entites": result[1],
            "repartition": {
                "associations": result[2],
                "entreprises_pures": result[3]
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))