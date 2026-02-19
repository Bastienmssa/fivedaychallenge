import duckdb
import os
import time
import glob

# Configuration des chemins
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw") # mock à changer avec raw
PARQUET_BRONZE_DIR = os.path.join(BASE_DIR, "data", "parquet", "bronze")

# Mode Test : Limite à 1000 lignes pour le développement rapide
TEST_MODE = False
LIMIT_CLAUSE = "LIMIT 1000" if TEST_MODE else ""

def ensure_dir(path):
    """Crée le dossier s'il n'existe pas."""
    if not os.path.exists(path):
        os.makedirs(path)

def convert_csv_to_parquet(source_pattern, target_file, table_name):
    """Utilise DuckDB pour lire un ou plusieurs CSV et l'écrire en Parquet."""
    print(f"Conversion en cours : {table_name} (Test Mode: {TEST_MODE})")
    
    # Remplacement des antislashs pour éviter les bugs DuckDB sous Windows
    source_pattern_safe = source_pattern.replace('\\', '/')
    target_file_safe = target_file.replace('\\', '/')
    
    start_time = time.time()
    
    # DuckDB lit les jokers (*) nativement pour concaténer les fichiers
    query = f"""
        COPY (
            SELECT * FROM read_csv_auto('{source_pattern_safe}', all_varchar=true, quote='"')
            {LIMIT_CLAUSE}
        ) TO '{target_file_safe}' (FORMAT PARQUET, COMPRESSION 'ZSTD');
    """
    
    try:
        duckdb.sql(query)
        elapsed = time.time() - start_time
        print(f"Succès pour {table_name} en {elapsed:.2f} secondes.\n")
    except Exception as e:
        print(f"Erreur lors de la conversion de {table_name} : {e}\n")

def run_ingestion():
    """Orchestre la conversion des trois sources obligatoires."""
    ensure_dir(os.path.join(PARQUET_BRONZE_DIR, "ban"))
    ensure_dir(os.path.join(PARQUET_BRONZE_DIR, "rna"))
    ensure_dir(os.path.join(PARQUET_BRONZE_DIR, "sirene"))

    # Définition des sources avec les bons noms de fichiers
    sources = [
        {
            "src": os.path.join(RAW_DIR, "ban", "adresses-france.csv"),
            "dest": os.path.join(PARQUET_BRONZE_DIR, "ban", "ban_bronze.parquet"),
            "name": "BAN"
        },
        {
            # L'utilisation du joker * permet de cibler les ~100 fichiers
            "src": os.path.join(RAW_DIR, "rna", "rna_waldec_*.csv"),
            "dest": os.path.join(PARQUET_BRONZE_DIR, "rna", "rna_bronze.parquet"),
            "name": "RNA"
        },
        {
            "src": os.path.join(RAW_DIR, "sirene", "StockEtablissement_utf8.csv"),
            "dest": os.path.join(PARQUET_BRONZE_DIR, "sirene", "sirene_bronze.parquet"),
            "name": "SIRENE"
        }
    ]

    for source in sources:
        # On vérifie la présence des fichiers. Si c'est un motif avec *, on utilise glob.
        if '*' in source["src"]:
            files = glob.glob(source["src"])
            if files:
                print(f"{len(files)} fichiers trouvés pour {source['name']}.")
                convert_csv_to_parquet(source["src"], source["dest"], source["name"])
            else:
                print(f"Aucun fichier trouvé pour le motif : {source['src']}")
        else:
            if os.path.exists(source["src"]):
                convert_csv_to_parquet(source["src"], source["dest"], source["name"])
            else:
                print(f"Fichier introuvable : {source['src']}")

if __name__ == "__main__":
    run_ingestion()