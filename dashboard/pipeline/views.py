import json
import subprocess
import os
from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings

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