# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process
# .\.venv\Scripts\Activate.ps1
# =========================
# CONFIGURATION
# =========================

.PHONY: ingest normalize match views api all

ingest:
	# CSV -> Parquet (SIRENE, RNA, BAN)
	python etl/ingest.py

normalize:
	# Nettoyage adresses -> Tables Silver
	python etl/normalize.py

match:
	# Matching SIRENE/RNA -> Table de liens
	python etl/match.py

views:
	# Generation des vues finales (Golden, Search, Stats)
	python etl/views.py

api:
	# Lancement du serveur
	uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 4


all: ingest normalize match views