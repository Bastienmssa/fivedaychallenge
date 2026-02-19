import duckdb
import sqlite3
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_SILVER_DIR = os.path.join(BASE_DIR, "data", "parquet", "silver")
PARQUET_GOLD_DIR = os.path.join(BASE_DIR, "data", "parquet", "gold")
DB_DIR = os.path.join(BASE_DIR, "duckdb")

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def build_parquet_views():
    print("Creation des vues Parquet (Golden Record & Stats)...")
    start_time = time.time()
    
    ensure_dir(PARQUET_GOLD_DIR)
    
    sirene_silver = os.path.join(PARQUET_SILVER_DIR, "sirene", "sirene_silver.parquet").replace('\\', '/')
    ban_silver = os.path.join(PARQUET_SILVER_DIR, "ban", "ban_silver.parquet").replace('\\', '/')
    mapping = os.path.join(PARQUET_SILVER_DIR, "mapping_sirene_rna.parquet").replace('\\', '/')
    
    golden_file = os.path.join(PARQUET_GOLD_DIR, "golden_record.parquet").replace('\\', '/')
    stats_file = os.path.join(PARQUET_GOLD_DIR, "stats_view.parquet").replace('\\', '/')

    # 1. Creation du Golden Record (SIRENE + RNA + BAN) avec deduplication du mapping
    query_golden = f"""
        COPY (
            SELECT 
                s.siret,
                s.status,
                s.enseigne AS name,
                s.code_postal,
                s.commune AS city,
                m.id_rna AS rna,
                b.latitude,
                b.longitude,
                CASE WHEN b.latitude IS NOT NULL THEN true ELSE false END AS is_ban_validated
            FROM read_parquet('{sirene_silver}') s
            LEFT JOIN (
                SELECT siret, ANY_VALUE(id_rna) AS id_rna 
                FROM read_parquet('{mapping}') 
                GROUP BY siret
            ) m ON s.siret = m.siret
            LEFT JOIN (
                SELECT code_postal, ANY_VALUE(latitude) as latitude, ANY_VALUE(longitude) as longitude
                FROM read_parquet('{ban_silver}')
                GROUP BY code_postal
            ) b ON s.code_postal = b.code_postal
        ) TO '{golden_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    # 2. Creation de la vue Statistiques avec deduplication
    query_stats = f"""
        COPY (
            SELECT 
                s.code_postal,
                COUNT(s.siret) AS total_entites,
                COUNT(m.id_rna) AS associations,
                COUNT(s.siret) - COUNT(m.id_rna) AS entreprises_pures
            FROM read_parquet('{sirene_silver}') s
            LEFT JOIN (
                SELECT siret, ANY_VALUE(id_rna) AS id_rna 
                FROM read_parquet('{mapping}') 
                GROUP BY siret
            ) m ON s.siret = m.siret
            GROUP BY s.code_postal
        ) TO '{stats_file}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    try:
        duckdb.sql(query_golden)
        duckdb.sql(query_stats)
        print(f"Succes des vues Parquet en {time.time() - start_time:.2f} secondes.")
    except Exception as e:
        print(f"Erreur vues Parquet : {e}")

def build_sqlite_search():
    print("Creation de l'index de recherche SQLite FTS5...")
    start_time = time.time()
    
    ensure_dir(DB_DIR)
    sqlite_db_path = os.path.join(DB_DIR, "catalog.db")
    
    if os.path.exists(sqlite_db_path):
        os.remove(sqlite_db_path)
        
    sirene_silver = os.path.join(PARQUET_SILVER_DIR, "sirene", "sirene_silver.parquet").replace('\\', '/')
    mapping = os.path.join(PARQUET_SILVER_DIR, "mapping_sirene_rna.parquet").replace('\\', '/')

    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE VIRTUAL TABLE search_view USING fts5(
            siret UNINDEXED, 
            name, 
            postal_code, 
            city UNINDEXED,
            is_association UNINDEXED
        );
    """)
    
    # Requete avec deduplication
    query_data = f"""
        SELECT 
            s.siret, 
            COALESCE(s.enseigne, '') AS name, 
            s.code_postal AS postal_code,
            s.commune AS city,
            CASE WHEN m.id_rna IS NOT NULL THEN 'true' ELSE 'false' END AS is_association
        FROM read_parquet('{sirene_silver}') s
        LEFT JOIN (
            SELECT siret, ANY_VALUE(id_rna) AS id_rna 
            FROM read_parquet('{mapping}') 
            GROUP BY siret
        ) m ON s.siret = m.siret
        WHERE s.enseigne IS NOT NULL
    """
    
    try:
        results = duckdb.sql(query_data).fetchall()
        cursor.executemany(
            "INSERT INTO search_view (siret, name, postal_code, city, is_association) VALUES (?, ?, ?, ?, ?)", 
            results
        )
        conn.commit()
        print(f"Succes de l'index SQLite en {time.time() - start_time:.2f} secondes.")
    except Exception as e:
        print(f"Erreur SQLite : {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    build_parquet_views()
    build_sqlite_search()