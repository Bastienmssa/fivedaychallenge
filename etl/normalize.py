import duckdb
import os
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PARQUET_BRONZE_DIR = os.path.join(BASE_DIR, "data", "parquet", "bronze")
PARQUET_SILVER_DIR = os.path.join(BASE_DIR, "data", "parquet", "silver")

def ensure_dir(path):
    if not os.path.exists(path):
        os.makedirs(path)

def normalize_ban():
    print("Demarrage de la normalisation BAN...")
    start_time = time.time()
    
    ban_bronze = os.path.join(PARQUET_BRONZE_DIR, "ban", "ban_bronze.parquet").replace('\\', '/')
    ban_silver = os.path.join(PARQUET_SILVER_DIR, "ban", "ban_silver.parquet").replace('\\', '/')
    ensure_dir(os.path.join(PARQUET_SILVER_DIR, "ban"))
    
    query = f"""
        COPY (
            SELECT 
                numero,
                REPLACE(
                    REPLACE(
                        REPLACE(UPPER(nom_voie), 'AV ', 'AVENUE '), 
                        'ST ', 'SAINT '
                    ),
                    'BD ', 'BOULEVARD '
                ) AS nom_voie_normalise,
                code_postal,
                UPPER(nom_commune) AS commune,
                lon AS longitude,
                lat AS latitude
            FROM read_parquet('{ban_bronze}')
            WHERE code_postal IS NOT NULL 
              AND nom_commune IS NOT NULL
        ) TO '{ban_silver}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    try:
        duckdb.sql(query)
        print(f"Succes pour BAN Silver en {time.time() - start_time:.2f} secondes.")
    except Exception as e:
        print(f"Erreur BAN : {e}")

def normalize_sirene():
    print("Demarrage de la normalisation SIRENE...")
    start_time = time.time()
    
    sirene_bronze = os.path.join(PARQUET_BRONZE_DIR, "sirene", "sirene_bronze.parquet").replace('\\', '/')
    sirene_silver = os.path.join(PARQUET_SILVER_DIR, "sirene", "sirene_silver.parquet").replace('\\', '/')
    ensure_dir(os.path.join(PARQUET_SILVER_DIR, "sirene"))
    
    query = f"""
        COPY (
            SELECT 
                siret,
                siren,
                etatAdministratifEtablissement AS status,
                COALESCE(enseigne1Etablissement, denominationUsuelleEtablissement) AS enseigne,
                codePostalEtablissement AS code_postal,
                UPPER(libelleCommuneEtablissement) AS commune
            FROM read_parquet('{sirene_bronze}')
            WHERE siret IS NOT NULL
        ) TO '{sirene_silver}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    try:
        duckdb.sql(query)
        print(f"Succes pour SIRENE Silver en {time.time() - start_time:.2f} secondes.")
    except Exception as e:
        print(f"Erreur SIRENE : {e}")

def normalize_rna():
    print("Demarrage de la normalisation RNA...")
    start_time = time.time()
    
    rna_bronze = os.path.join(PARQUET_BRONZE_DIR, "rna", "rna_bronze.parquet").replace('\\', '/')
    rna_silver = os.path.join(PARQUET_SILVER_DIR, "rna", "rna_silver.parquet").replace('\\', '/')
    ensure_dir(os.path.join(PARQUET_SILVER_DIR, "rna"))
    
    # Correction : On selectionne 'id' et on le renomme en 'id_rna' pour la suite
    query = f"""
        COPY (
            SELECT 
                id AS id_rna,
                UPPER(titre) AS nom_association,
                adrs_codepostal AS code_postal,
                UPPER(adrs_libcommune) AS commune
            FROM read_parquet('{rna_bronze}')
            WHERE id IS NOT NULL 
              AND adrs_codepostal IS NOT NULL
        ) TO '{rna_silver}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    try:
        duckdb.sql(query)
        print(f"Succes pour RNA Silver en {time.time() - start_time:.2f} secondes.")
    except Exception as e:
        print(f"Erreur RNA : {{e}}")
def run_normalization():
    normalize_ban()
    normalize_sirene()
    normalize_rna()

if __name__ == "__main__":
    run_normalization()