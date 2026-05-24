import time, json, random, math
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# ============================================================
# CONFIGURATION
# ============================================================
CONFIG_PATH = "config.json"

def charger_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)

# ============================================================
# CONNEXION MQTT
# ============================================================
client = mqtt.Client()
client.connect("localhost", 1883, 60)

# ============================================================
# FONCTIONS DE SIMULATION
# ============================================================

def ornstein_uhlenbeck(valeur, moyenne, theta, sigma):
    """
    Retour vers la moyenne avec bruit gaussien.
    theta faible = inertie forte = courbe lisse (pression)
    theta élevé  = retour rapide = plus réactif (vibration)
    """
    return valeur + theta * (moyenne - valeur) + sigma * random.gauss(0, 1)

def cycle_sinusoidal(amplitude, periode, t):
    """
    Cycle lent simulant les variations périodiques
    thermiques ou mécaniques d'un équipement industriel.
    """
    return amplitude * math.sin(2 * math.pi * t / periode)

# ============================================================
# ÉTAT INITIAL
# ============================================================
etats_capteurs       = {}
PROB_ANOMALIE        = 0.002
PROB_RECUPERATION    = 0.01
t                    = 0
dernier_rechargement = 0
config               = charger_config()

# ============================================================
# BOUCLE PRINCIPALE
# ============================================================
while True:
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    t  += 2

    # Rechargement config toutes les 10 secondes
    if t - dernier_rechargement >= 10:
        config               = charger_config()
        dernier_rechargement = t
        print("🔄 Configuration rechargée")

    for capteur in config["capteurs"]:
        machine_id      = capteur["machine_id"]
        type_capteur    = capteur["type_capteur"]
        valeur_nominale = capteur["valeur_nominale"]
        valeur_min      = capteur["valeur_min"]
        valeur_max      = capteur["valeur_max"]
        theta           = capteur["theta"]
        sigma           = capteur["sigma"]
        seuil_haut      = capteur["seuil_alerte_haut"]
        cycle_amp       = capteur["cycle_amplitude"]
        cycle_per       = capteur["cycle_periode"]

        # Initialisation à la première rencontre du capteur
        if machine_id not in etats_capteurs:
            etats_capteurs[machine_id] = {
                "valeur": valeur_nominale,
                "etat"  : "NORMAL"
            }

        valeur = etats_capteurs[machine_id]["valeur"]
        etat   = etats_capteurs[machine_id]["etat"]

        # --- Machine à états ---
        if etat == "NORMAL":
            if random.random() < PROB_ANOMALIE:
                etat = "ANOMALIE"
                print(f"⚠️  ANOMALIE {type_capteur.upper()} — {machine_id}")
            cible = valeur_nominale

        elif etat == "ANOMALIE":
            # Dérive vers 95% du seuil haut
            cible = seuil_haut * 0.95
            if random.random() < PROB_RECUPERATION:
                etat = "RECUPERATION"
                print(f"🔧 RÉCUPÉRATION {type_capteur.upper()} — {machine_id}")

        else:  # RECUPERATION
            cible = valeur_nominale
            tolerance = (valeur_max - valeur_min) * 0.02
            if abs(valeur - valeur_nominale) < tolerance:
                etat = "NORMAL"
                print(f"✅ {machine_id} revenue à la normale")

        # --- Calcul de la nouvelle valeur ---
        valeur  = ornstein_uhlenbeck(valeur, cible, theta, sigma)
        valeur += cycle_sinusoidal(cycle_amp, cycle_per, t)
        valeur  = round(max(valeur_min, min(valeur_max, valeur)), 2)

        # Sauvegarde de l'état
        etats_capteurs[machine_id] = {"valeur": valeur, "etat": etat}

        # Topic MQTT selon le type
        topics = {
            "temperature" : "raffinerie/temp",
            "vibration"   : "raffinerie/vib",
            "pression"    : "raffinerie/pression"
        }
        topic = topics.get(type_capteur, f"raffinerie/{type_capteur}")

        message = json.dumps({
            "machine_id"   : machine_id,
            "valeur"       : valeur,
            "timestamp"    : now,
            "type_capteur" : type_capteur,
            "unite"        : capteur["unite"],
            "etat"         : etat
        })

        client.publish(topic, message)
        print(f"[{now}] {machine_id} | {type_capteur}: {valeur} {capteur['unite']} ({etat})")

    time.sleep(2)