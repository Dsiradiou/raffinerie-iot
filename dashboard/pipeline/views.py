import json
import subprocess
import os
import time
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
import psycopg2
import docker
import psutil

# Stocke les references aux processus lances par Django
processus = {
    "simulateur": None,
    "mqtt_kafka": None,
}

# Chemin racine du projet raffinerie-iot
RACINE = os.path.dirname(settings.CONFIG_PATH)

SCRIPTS = {
    "simulateur": "simulateur_capteurs.py",
    "mqtt_kafka": "mqtt_to_kafka.py",
}


def lire_config():
    with open(settings.CONFIG_PATH, 'r') as f:
        return json.load(f)


def ecrire_config(data):
    with open(settings.CONFIG_PATH, 'w') as f:
        json.dump(data, f, indent=4)


def trouver_processus():
    """Cherche les scripts dans tous les processus OS via psutil."""
    trouve = {"simulateur": False, "mqtt_kafka": False}
    for proc in psutil.process_iter(['cmdline']):
        try:
            cmdline = " ".join(proc.info['cmdline'] or [])
            for cle, nom in SCRIPTS.items():
                if nom in cmdline:
                    trouve[cle] = True
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return trouve


def pipeline_active():
    """Retourne True si les deux scripts tournent."""
    t = trouver_processus()
    return t["simulateur"] and t["mqtt_kafka"]


# ============================================================
# VUE PRINCIPALE
# ============================================================

def index(request):
    config = lire_config()
    return render(request, 'pipeline/index.html', {
        'config': config,
        'actif': pipeline_active(),
    })


# ============================================================
# CONTROLE DE LA PIPELINE
# ============================================================

@csrf_exempt
def pipeline_start(request):
    if request.method == 'POST':
        if pipeline_active():
            return JsonResponse({'status': 'error', 'message': 'Pipeline deja active'})

        try:
            venv_python = os.path.join(RACINE, 'venv', 'Scripts', 'python.exe')

            if not os.path.exists(venv_python):
                return JsonResponse({
                    'status': 'error',
                    'message': 'Interpreteur introuvable : ' + venv_python
                })

            processus["simulateur"] = subprocess.Popen(
                [venv_python, os.path.join(RACINE, 'simulateur_capteurs.py')],
                cwd=RACINE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            processus["mqtt_kafka"] = subprocess.Popen(
                [venv_python, os.path.join(RACINE, 'mqtt_to_kafka.py')],
                cwd=RACINE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Attend 2s puis verifie que les processus tournent vraiment
            time.sleep(2)

            sim_ok = processus["simulateur"].poll() is None
            mqtt_ok = processus["mqtt_kafka"].poll() is None

            if not sim_ok or not mqtt_ok:
                erreur = ""
                if not sim_ok:
                    erreur += processus["simulateur"].stderr.read().decode(errors='replace')
                if not mqtt_ok:
                    erreur += processus["mqtt_kafka"].stderr.read().decode(errors='replace')
                return JsonResponse({
                    'status': 'error',
                    'message': 'Echec au demarrage : ' + erreur[:300]
                })

            config = lire_config()
            config["pipeline"]["status"] = "running"
            ecrire_config(config)

            return JsonResponse({'status': 'ok', 'message': 'Pipeline demarree'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})


@csrf_exempt
def pipeline_stop(request):
    if request.method == 'POST':
        try:
            # Tue tous les processus correspondants via psutil
            # (peu importe s'ils ont ete lances par Django ou le terminal)
            for proc in psutil.process_iter(['pid', 'cmdline']):
                try:
                    cmdline = " ".join(proc.info['cmdline'] or [])
                    for nom in SCRIPTS.values():
                        if nom in cmdline:
                            proc.terminate()
                            break
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    pass

            # Reinitialise le dict interne
            processus["simulateur"] = None
            processus["mqtt_kafka"] = None

            config = lire_config()
            config["pipeline"]["status"] = "stopped"
            ecrire_config(config)

            return JsonResponse({'status': 'ok', 'message': 'Pipeline arretee'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})


def pipeline_status(request):
    t = trouver_processus()
    return JsonResponse({
        'status': 'running' if (t["simulateur"] and t["mqtt_kafka"]) else 'stopped',
        'simulateur': t["simulateur"],
        'mqtt_kafka': t["mqtt_kafka"],
    })


# ============================================================
# CONFIGURATION
# ============================================================

@csrf_exempt
def config_capteurs(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            config = lire_config()
            action = data.get('action', 'remplacer')

            if action == 'ajouter':
                ids_existants = [c["machine_id"] for c in config["capteurs"]]
                if data["capteur"]["machine_id"] in ids_existants:
                    return JsonResponse({'status': 'error', 'message': 'Ce machine_id existe deja'})
                config["capteurs"].append(data["capteur"])

            elif action == 'modifier':
                for c in config["capteurs"]:
                    if c["machine_id"] == data["capteur"]["machine_id"]:
                        c.update(data["capteur"])
                        break

            else:
                config["capteurs"] = data["capteurs"]

            ecrire_config(config)
            return JsonResponse({'status': 'ok', 'message': 'Capteurs mis a jour'})

        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})


@csrf_exempt
def config_seuils(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            config = lire_config()
            config["seuils"].update(data)
            ecrire_config(config)
            return JsonResponse({'status': 'ok', 'message': 'Seuils mis a jour'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})


def capteurs(request):
    config = lire_config()
    return render(request, 'pipeline/capteurs.html', {
        'capteurs': config["capteurs"]
    })


@csrf_exempt
def supprimer_capteur(request):
    if request.method == 'POST':
        try:
            data = json.loads(request.body)
            machine_id = data["machine_id"]
            config = lire_config()
            config["capteurs"] = [
                c for c in config["capteurs"]
                if c["machine_id"] != machine_id
            ]
            ecrire_config(config)
            return JsonResponse({'status': 'ok', 'message': machine_id + ' supprime'})
        except Exception as e:
            return JsonResponse({'status': 'error', 'message': str(e)})


# ============================================================
# ALERTES
# ============================================================

def alertes(request):
    config = lire_config()
    liste_capteurs = config["capteurs"]
    liste_alertes = []

    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="iotdb", user="admin", password="admin"
        )
        cursor = conn.cursor()

        for capteur in liste_capteurs:
            machine_id = capteur["machine_id"]
            type_capteur = capteur["type_capteur"]
            seuil_bas = capteur["seuil_alerte_bas"]
            seuil_haut = capteur["seuil_alerte_haut"]
            unite = capteur["unite"]

            cursor.execute("""
                SELECT timestamp, machine_id, type_capteur, valeur
                FROM mesures_filtrees
                WHERE machine_id   = %s
                AND   type_capteur = %s
                AND   (valeur < %s OR valeur > %s)
                ORDER BY timestamp DESC
                LIMIT 50
            """, (machine_id, type_capteur, seuil_bas, seuil_haut))

            for row in cursor.fetchall():
                timestamp, mid, type_cap, valeur = row
                if valeur > seuil_haut:
                    niveau = "HAUT"
                    seuil = seuil_haut
                else:
                    niveau = "BAS"
                    seuil = seuil_bas

                liste_alertes.append({
                    "timestamp": timestamp,
                    "machine_id": mid,
                    "type_capteur": type_cap,
                    "valeur": round(valeur, 2),
                    "seuil": seuil,
                    "niveau": niveau,
                    "unite": unite,
                })

        cursor.close()
        conn.close()

        liste_alertes.sort(key=lambda x: x["timestamp"], reverse=True)
        liste_alertes = liste_alertes[:100]

    except Exception as e:
        print("Erreur DB alertes : " + str(e))

    return render(request, 'pipeline/alertes.html', {
        'alertes': liste_alertes,
        'total': len(liste_alertes),
    })


# ============================================================
# KPI
# ============================================================

def kpi(request):
    """
    Affiche les indicateurs de performance depuis kpi_indicateurs.
    - resume : derniere valeur moyenne par type (pour les cartes)
    - historique : les 50 derniers enregistrements (pour le tableau)
    """
    resume = {}
    historique = []

    try:
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname="iotdb", user="admin", password="admin"
        )
        cursor = conn.cursor()

        # Derniere valeur par type de capteur pour les cartes resumé
        cursor.execute("""
            SELECT DISTINCT ON (type_kpi)
                type_kpi, valeur, unite, timestamp
            FROM kpi_indicateurs
            ORDER BY type_kpi, timestamp DESC
        """)
        for row in cursor.fetchall():
            type_kpi, valeur, unite, timestamp = row
            resume[type_kpi] = {
                "valeur"   : round(valeur, 3),
                "unite"    : unite,
                "timestamp": timestamp,
            }

        # 50 derniers enregistrements pour le tableau
        cursor.execute("""
            SELECT timestamp, type_kpi, valeur, unite
            FROM kpi_indicateurs
            ORDER BY timestamp DESC
            LIMIT 50
        """)
        for row in cursor.fetchall():
            timestamp, type_kpi, valeur, unite = row
            historique.append({
                "timestamp": timestamp,
                "type_kpi" : type_kpi,
                "valeur"   : round(valeur, 3),
                "unite"    : unite,
            })

        cursor.close()
        conn.close()

    except Exception as e:
        print("Erreur DB kpi : " + str(e))

    return render(request, 'pipeline/kpi.html', {
        'resume'    : resume,
        'historique': historique,
    })


# ============================================================
# INFRASTRUCTURE
# ============================================================

def infrastructure(request):
    return render(request, 'pipeline/infrastructure.html')


def grafana(request):
    return render(request, 'pipeline/grafana.html')


def infrastructure_data(request):
    data = {
        "conteneurs": [],
        "pipeline": {},
        "metriques": {},
    }

    docker_client = None

    # --- Statut des conteneurs Docker ---
    try:
        docker_client = docker.from_env()

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
                    "nom": item["nom_affiche"],
                    "statut": conteneur.status,
                    "details": conteneur.attrs["State"]["Status"],
                    "uptime": conteneur.attrs["State"].get("StartedAt", "")[:19].replace("T", " "),
                })
            except docker.errors.NotFound:
                data["conteneurs"].append({
                    "nom": item["nom_affiche"],
                    "statut": "absent",
                    "details": "Conteneur introuvable",
                    "uptime": "",
                })

    except Exception as e:
        data["erreur_docker"] = str(e)

    # --- Statut des processus Python via psutil ---
    data["pipeline"] = trouver_processus()

    # --- Metriques TimescaleDB ---
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

    # --- Metriques Kafka ---
    try:
        if docker_client:
            kafka = docker_client.containers.get("kafka")
            result = kafka.exec_run(
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
