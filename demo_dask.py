import dask.bag as db
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster
import json
import os
import re
import string
import time
import glob
import shutil

# --- CONFIGURATION ---
INPUT_FOLDER = "./document_parses/pdf_json/*.json"
OUTPUT_FOLDER = "./resultats_nettoyage.parquet"

# --- NETTOYAGE ---
def extract_preprocessing(record):
    # 1. Extraction
    paper_id = record.get("paper_id", "")
    title = record.get("metadata", {}).get("title", "")
    
    abstract_list = record.get("abstract", [])
    abstract_text = " ".join([item["text"] for item in abstract_list if "text" in item])

    body_list = record.get("body_text", [])
    body_text = " ".join([item["text"] for item in body_list if "text" in item])
    
    # 2. Nettoyage immédiat (pour ne pas stocker le texte brut en mémoire)
    # si pas de texte, on retourne None (sera filtré)
    if not isinstance(body_text, str) or len(body_text) < 500:
        return None

    # Regex compilation (rapide)
    text = body_text.lower()
    text = re.sub(r'\S*@\S*\s?', '', text) # Emails
    text = re.sub(r'http\S+', '', text)    # URLs
    text = re.sub(f'[{re.escape(string.punctuation)}]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()

    return {
        "paper_id": paper_id,
        "title": title,
        "clean_text": text
    }

def main():
    # 1. SETUP DU CLUSTER
    # limiter mémoire pour éviter gel
    cluster = LocalCluster(memory_limit='3GB') 
    client = Client(cluster)
    
    print("="*50)
    print(f"DASK CLUSTER DÉMARRÉ")
    print(f"🔗 Dashboard monitoring: {client.dashboard_link}")
    print("="*50)

    # Nettoyage du dossier de sortie précédent si existant
    if os.path.exists(OUTPUT_FOLDER):
        shutil.rmtree(OUTPUT_FOLDER)

    print("1. Lecture des fichiers...")
    files = glob.glob(INPUT_FOLDER)
    print(f"-> {len(files)} fichiers détectés.")

    start_time = time.time()

    # 2. CRÉATION DU BAG
    # 200 est un nombre choisi judicieusement, à expliquer lors de la démo...
    b = db.from_sequence(files, partition_size=200)

    # Lecture JSON
    def safe_load(f_path):
        with open(f_path, 'r') as f:
            return json.load(f)
            
    b = b.map(safe_load)

    # 3. TRANSFORMATION & FILTRAGE
    b = b.map(extract_preprocessing)
    # retrait none
    b = b.filter(lambda x: x is not None)

    # 4. CONVERSION DATAFRAME
    meta = {
        'paper_id': 'object', 
        'title': 'object', 
        'clean_text': 'object'
    }
    ddf = b.to_dataframe(meta=meta)

    print("2. Lancement du traitement et écriture disque (Parquet)...")
    print("   (Regarde le Dashboard, c'est là que ça se passe !)")
    
    # to_parquet écrit au fur et à mesure.
    # RAM se vide après chaque écriture. Ça ne plantera pas.
    # pyarrow très performant sur Mac
    ddf.to_parquet(OUTPUT_FOLDER, engine='pyarrow', compression='snappy')

    duration = time.time() - start_time
    print("="*50)
    print(f"TERMINÉ EN {duration:.2f} SECONDES")
    print(f"Données nettoyées sauvegardées dans : {OUTPUT_FOLDER}")
    print("="*50)

    input("Appuyer sur Entrée pour fermer le cluster...")

if __name__ == "__main__":
    main()
