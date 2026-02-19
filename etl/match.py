import duckdb
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_SILVER_DIR = os.path.join(BASE_DIR, "data", "parquet", "silver")
DB_DIR = os.path.join(BASE_DIR, "duckdb")

def run_matching():
    print("--- DEMARRAGE DU MATCHING EQUILIBRE ---")
    start_time = time.time()
    
    sirene_silver = os.path.join(PARQUET_SILVER_DIR, "sirene", "sirene_silver.parquet").replace('\\', '/')
    rna_silver = os.path.join(PARQUET_SILVER_DIR, "rna", "rna_silver.parquet").replace('\\', '/')
    
    if not os.path.exists(DB_DIR):
        os.makedirs(DB_DIR)
        
    db_path = os.path.join(DB_DIR, "catalog.db").replace('\\', '/')
    conn = duckdb.connect(db_path)
    
    # Requete SQL avec seuil Jaro-Winkler assoupli a 0.85 mais avec nettoyage Regex maintenu
    query = f"""
    CREATE OR REPLACE TABLE rna_sirene_mapping AS
    
    WITH sirene_clean AS (
        SELECT 
            siret, 
            code_postal,
            SUBSTRING(code_postal, 1, 2) AS departement,
            enseigne,
            TRIM(REGEXP_REPLACE(UPPER(COALESCE(enseigne, '')), '\\b(ASSOCIATION|AMICALE|CLUB|SYNDICAT|FEDERATION|DE|DU|DES|LA|LE|LES|ET|POUR|EN)\\b', '', 'g')) as name_clean
        FROM read_parquet('{sirene_silver}')
        WHERE enseigne IS NOT NULL AND code_postal IS NOT NULL
    ),

    rna_clean AS (
        SELECT 
            id_rna, 
            code_postal,
            SUBSTRING(code_postal, 1, 2) AS departement,
            nom_association,
            TRIM(REGEXP_REPLACE(UPPER(COALESCE(nom_association, '')), '\\b(ASSOCIATION|AMICALE|CLUB|SYNDICAT|FEDERATION|DE|DU|DES|LA|LE|LES|ET|POUR|EN)\\b', '', 'g')) as name_clean
        FROM read_parquet('{rna_silver}')
        WHERE nom_association IS NOT NULL AND code_postal IS NOT NULL
    )

    -- STRATEGIE 1 : Correspondance exacte sur le nom nettoye (tolere les CEDEX et variations locales)
    SELECT DISTINCT
        s.siret,
        r.id_rna
    FROM sirene_clean s
    INNER JOIN rna_clean r 
        ON s.departement = r.departement 
        AND s.name_clean = r.name_clean
    WHERE LENGTH(s.name_clean) > 3

    UNION

    -- STRATEGIE 2 : Correspondance floue assouplie (0.85) sur le meme code postal
    SELECT DISTINCT
        s.siret,
        r.id_rna
    FROM sirene_clean s
    INNER JOIN rna_clean r 
        ON s.code_postal = r.code_postal
    WHERE 
        s.name_clean != r.name_clean 
        -- On exige un minimum de 4 caracteres pour eviter que de simples sigles matchent par erreur
        AND LENGTH(s.name_clean) > 4 
        AND LENGTH(r.name_clean) > 4
        AND jaro_winkler_similarity(s.name_clean, r.name_clean) >= 0.85;
    """
    
    print("> Lecture des Parquets Silver et creation du mapping...")
    try:
        conn.execute(query)
        count = conn.execute("SELECT COUNT(*) FROM rna_sirene_mapping").fetchone()[0]
        print(f"> Matchings trouves (Equilibre Parfait) : {count}")
    except Exception as e:
        print(f"Erreur lors de l'execution de la requete : {e}")
    finally:
        print(f"Temps d'execution total : {time.time() - start_time:.2f} secondes")
        conn.close()

if __name__ == "__main__":
    run_matching()