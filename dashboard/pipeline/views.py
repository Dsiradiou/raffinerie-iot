import json
import subprocess
import os
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import psycopg2
import docker

# Stocke les références aux processus lancés
processus = {
    "simulateur" : None,
    "mqtt_kafka" : None,
}

# Chemin racine du projet raffinerie-iot
RACINE = os.path.dirname(settings.CONFIG_PATH)

def lire_config():
    """Lit et retourne le contenu de config.json."""
    with open(settings.CONFIG_PATH, 'r') as f:
        return json.load(f)

def ecrire_config(data):
    """Écrit les données dans config.json."""
    with open(settings.CONFIG_PATH, 'w') as f:
        json.dump(data, f, indent=4)

def pipeline_active():
    """Vérifie si les processus tournent encore."""
    sim  = processus["simulateur"]
    mqtt = processus["mqtt_kafka"]
    return sim is not None and sim.poll() is None and \
           mqtt is not None and mqtt.poll() is None

# ============================================================
# VUE PRINCIPALE
# ============================================================

def index(request):
    """Page principale du dashboard."""
    config = lire_config()
    return render(request, 'pipeline/index.html', {
        'config'  : config,
        'actif'   : pipeline_active(),
    })

# ============================================================
# CONTRÔLE DE LA PIPELINE
# ============================================================

@csrf_exempt
def pipeline_start(request):
    """Lance le simulateur et le pont MQTT→Kafka."""
    if request.method == 'POST':
        if pipeline_active():
            return JsonResponse({'status': 'error', 'message': 'Pipeline déjà active'})

        try:
            venv_python = os.path.join(RACINE, 'venv', 'Scripts', 'python.exe')

            processus["simulateur"] = subprocess.Popen(
                [venv_python, os.path.join(RACINE, 'simulateur_capteurs.py')],
                cwd=RACINE
            )
            processus["mqtt_kafka"] = subprocess.Popen(
                [venv_python, os.path.join(RACINE, 'mqtt_to_kafka.py')],
                cwd=RACINE
            )

            config = lire_config()
            config["pipeline"]["status"] = "running"
            ecrire_config(config)

            return JsonResponse({'status': 'ok', 'message': 'Pipeline démarrée'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

@csrf_exempt
def pipeline_stop(request):
    """Arrête le simulateur et le pont MQTT→Kafka."""
    if request.method == 'POST':
        try:
            for nom, proc in processus.items():
                if proc and proc.poll() is None:
                    proc.terminate()
                    processus[nom] = None

            config = lire_config()
            config["pipeline"]["status"] = "stopped"
            ecrire_config(config)

            return JsonResponse({'status': 'ok', 'message': 'Pipeline arrêtée'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

def pipeline_status(request):
    """Retourne l'état actuel de la pipeline."""
    return JsonResponse({
        'status' : 'running' if pipeline_active() else 'stopped'
    })

# ============================================================
# CONFIGURATION
# ============================================================

@csrf_exempt
def config_capteurs(request):
    """Met à jour la liste des capteurs dans config.json."""
    if request.method == 'POST':
        try:
            data   = json.loads(request.body)
            config = lire_config()
            action = data.get('action', 'remplacer')

            if action == 'ajouter':
                # Vérifie que le machine_id n'existe pas déjà
                ids_existants = [c["machine_id"] for c in config["capteurs"]]
                if data["capteur"]["machine_id"] in ids_existants:
                    return JsonResponse({'status': 'error',
                                        'message': 'Ce machine_id existe déjà'})
                config["capteurs"].append(data["capteur"])

            elif action == 'modifier':
                for c in config["capteurs"]:
                    if c["machine_id"] == data["capteur"]["machine_id"]:
                        c.update(data["capteur"])
                        break

            else:  # remplacer — comportement original
                config["capteurs"] = data["capteurs"]

            ecrire_config(config)
            return JsonResponse({'status': 'ok', 'message': 'Capteurs mis à jour'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

@csrf_exempt
def config_seuils(request):
    """Met à jour les seuils d'alerte dans config.json."""
    if request.method == 'POST':
        try:
            data   = json.loads(request.body)
            config = lire_config()
            config["seuils"].update(data)
            ecrire_config(config)
            return JsonResponse({'status': 'ok', 'message': 'Seuils mis à jour'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})

def capteurs(request):
    """Page de gestion des capteurs."""
    config = lire_config()
    return render(request, 'pipeline/capteurs.html', {
        'capteurs': config["capteurs"]
    })

@csrf_exempt
def supprimer_capteur(request):
    """Supprime un capteur par son machine_id."""
    if request.method == 'POST':
        try:
            data       = json.loads(request.body)
            machine_id = data["machine_id"]
            config     = lire_config()
            config["capteurs"] = [
                c for c in config["capteurs"]
                if c["machine_id"] != machine_id
            ]
            ecrire_config(config)
            return JsonResponse({'status': 'ok', 'message': f'{machine_id} supprimé'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})
        

def alertes(request):
    """
    Récupère les mesures hors seuils depuis TimescaleDB
    et les affiche sous forme de tableau.
    """
    config   = lire_config()
    capteurs = config["capteurs"]
    alertes  = []

    try:
        # Connexion à TimescaleDB
        conn = psycopg2.connect(
            host     = "localhost",
            port     = 5432,
            dbname   = "iotdb",
            user     = "admin",
            password = "admin"
        )
        cursor = conn.cursor()

        # Pour chaque capteur on cherche les valeurs hors seuils
        for capteur in capteurs:
            machine_id  = capteur["machine_id"]
            type_capteur= capteur["type_capteur"]
            seuil_bas   = capteur["seuil_alerte_bas"]
            seuil_haut  = capteur["seuil_alerte_haut"]
            unite       = capteur["unite"]

            cursor.execute("""
                SELECT timestamp, machine_id, type_capteur, valeur
                FROM mesures_filtrees
                WHERE machine_id    = %s
                AND   type_capteur  = %s
                AND   (valeur < %s OR valeur > %s)
                ORDER BY timestamp DESC
                LIMIT 50
            """, (machine_id, type_capteur, seuil_bas, seuil_haut))

            rows = cursor.fetchall()

            for row in rows:
                timestamp, mid, type_cap, valeur = row

                # Détermine si c'est une alerte haute ou basse
                if valeur > seuil_haut:
                    niveau  = "HAUT"
                    seuil   = seuil_haut
                else:
                    niveau  = "BAS"
                    seuil   = seuil_bas

                alertes.append({
                    "timestamp"   : timestamp,
                    "machine_id"  : mid,
                    "type_capteur": type_cap,
                    "valeur"      : round(valeur, 2),
                    "seuil"       : seuil,
                    "niveau"      : niveau,
                    "unite"       : unite,
                })

        cursor.close()
        conn.close()

        # Trier toutes les alertes par timestamp décroissant
        alertes.sort(key=lambda x: x["timestamp"], reverse=True)
        alertes = alertes[:100]  # garder les 100 plus récentes

    except Exception as e:
        print(f"Erreur DB alertes : {e}")

    return render(request, 'pipeline/alertes.html', {
        'alertes' : alertes,
        'total'   : len(alertes),
    })

def infrastructure(request):
    """Page miroir de la pipeline."""
    return render(request, 'pipeline/infrastructure.html')

def grafana(request):
    """Page Grafana — iframe + lien direct."""
    return render(request, 'pipeline/grafana.html')


def infrastructure_data(request):
    """
    Endpoint JSON appelé toutes les 10s par le JavaScript.
    Retourne l'état de tous les conteneurs et métriques.
    """
    data = {
        "conteneurs" : [],
        "pipeline"   : {},
        "metriques"  : {},
    }

    # --- Statut des conteneurs Docker ---
    try:
        docker_client = docker.from_env()

        # Liste des conteneurs attendus avec leur nom affiché
        conteneurs_attendus = [
            {"nom_affiche": "MQTT",           "nom_conteneur": "raffinerie-iot-mqtt-1"},
            {"nom_affiche": "Kafka",          "nom_conteneur": "kafka"},
            {"nom_affiche": "Zookeeper",      "nom_conteneur": "zookeeper"},
            {"nom_affiche": "Spark Master",   "nom_conteneur": "raffinerie-iot-spark-master-1"},
            {"nom_affiche": "Spark Worker 1", "nom_conteneur": "raffinerie-iot-spark-worker-1-1"},
            {"nom_affiche": "Spark Worker 2", "nom_conteneur": "raffinerie-iot-spark-worker-2-1"},
            {"nom_affiche": "TimescaleDB",    "nom_conteneur": "raffinerie-iot-timescaledb-1"},
            {"nom_affiche": "MinIO",          "nom_conteneur": "raffinerie-iot-minio-1"},
            {"nom_affiche": "Grafana",        "nom_conteneur": "raffinerie-iot-grafana-1"},
        ]

        for item in conteneurs_attendus:
            try:
                conteneur = docker_client.containers.get(item["nom_conteneur"])
                data["conteneurs"].append({
                    "nom"    : item["nom_affiche"],
                    "statut" : conteneur.status,  # running, exited, paused...
                    "details": conteneur.attrs["State"]["Status"],
                    "uptime" : conteneur.attrs["State"].get("StartedAt", "")[:19].replace("T", " "),
                })
            except docker.errors.NotFound:
                data["conteneurs"].append({
                    "nom"    : item["nom_affiche"],
                    "statut" : "absent",
                    "details": "Conteneur introuvable",
                    "uptime" : "",
                })

    except Exception as e:
        data["erreur_docker"] = str(e)

    # --- Statut des processus Python ---
    data["pipeline"] = {
        "simulateur": processus["simulateur"] is not None and
                      processus["simulateur"].poll() is None,
        "mqtt_kafka": processus["mqtt_kafka"] is not None and
                      processus["mqtt_kafka"].poll() is None,
    }

    # --- Métriques TimescaleDB ---
    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="iotdb", user="admin", password="admin"
        )
        cursor = conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM mesures_filtrees")
        data["metriques"]["mesures_filtrees"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM kpi_indicateurs")
        data["metriques"]["kpi_indicateurs"] = cursor.fetchone()[0]

        cursor.execute("""
            SELECT type_capteur, COUNT(*)
            FROM mesures_filtrees
            GROUP BY type_capteur
        """)
        data["metriques"]["par_type"] = {
            row[0]: row[1] for row in cursor.fetchall()
        }

        cursor.close()
        conn.close()

    except Exception as e:
        data["metriques"]["erreur"] = str(e)

    # --- Métriques Kafka ---
    try:
        admin = docker_client.containers.get("kafka")
        result = admin.exec_run(
            "/usr/bin/kafka-run-class kafka.tools.GetOffsetShell "
            "--broker-list localhost:9092 --topic sensor-data --time -1"
        )
        output = result.output.decode().strip()
        if ":" in output:
            offset = int(output.split(":")[-1])
            data["metriques"]["kafka_messages"] = offset
    except Exception:
        data["metriques"]["kafka_messages"] = "N/A"

    return JsonResponse(data)