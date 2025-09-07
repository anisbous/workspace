# Fichier: scripts/telecharge_dvf.py

import pandas as pd
import requests
import os
import sys

# --- CONFIGURATION ---
# Construit le chemin vers le dossier 'data' à la racine du projet
# C'est la méthode la plus robuste pour que le script marche de n'importe où
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
ANNEE_A_TELECHARGER = 2023 # Modifiez cette valeur selon vos besoins

def telecharger_dvf(annee: int):
    """
    Télécharge et décompresse les données DVF pour une année donnée
    dans le dossier /data à la racine du projet.
    """
    if not os.path.exists(DATA_DIR):
        print(f"Création du dossier de données : {DATA_DIR}")
        os.makedirs(DATA_DIR)

    url = f"https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/full.csv.gz"
    nom_fichier = f"dvf_{annee}.csv.gz"
    chemin_destination = os.path.join(DATA_DIR, nom_fichier)

    if os.path.exists(chemin_destination):
        print(f"✅ Fichier '{nom_fichier}' déjà présent dans {DATA_DIR}.")
        return chemin_destination

    print(f"⬇️  Téléchargement pour l'année {annee}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        taille_totale = int(response.headers.get('content-length', 0))

        with open(chemin_destination, 'wb') as f:
            telecharge = 0
            for chunk in response.iter_content(chunk_size=8192):
                telecharge += len(chunk)
                f.write(chunk)
                # Affiche une barre de progression simple
                fait = int(50 * telecharge / taille_totale)
                sys.stdout.write(f"\r[{'=' * fait}{' ' * (50 - fait)}] {telecharge / (1024*1024):.1f} Mo")
                sys.stdout.flush()

        print("\n✅ Téléchargement terminé.")
        return chemin_destination

    except requests.exceptions.RequestException as e:
        print(f"\n❌ Erreur de téléchargement : {e}")
        return None

if __name__ == "__main__":
    print("--- Lancement du script de téléchargement DVF ---")
    telecharger_dvf(ANNEE_A_TELECHARGER)