import time, json, random, math
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# ============================================================
# CHARGEMENT DE LA CONFIGURATION
# ============================================================

CONFIG_PATH = "config.json"

def charger_config():
    """Lit config.json et retourne le contenu."""
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
    Processus de retour vers la moyenne.
    Produit des courbes lisses et continues comme de vrais capteurs.
    """
    bruit = random.gauss(0, 1)
    return valeur + theta * (moyenne - valeur) + sigma * bruit

def cycle_sinusoidal(amplitude, periode, t):
    """
    Composante cyclique lente simulant les cycles
    thermiques ou de charge de la pompe.
    """
    return amplitude * math.sin(2 * math.pi * t / periode)

# ============================================================
# ÉTAT INITIAL
# ============================================================
# Dictionnaire qui garde l'état de chaque capteur en mémoire
# clé = machine_id, valeur = dict avec valeur courante et état
etats_capteurs = {}

PROB_ANOMALIE     = 0.002
PROB_RECUPERATION = 0.01

t = 0  # compteur de temps en secondes
dernier_rechargement = 0  # pour savoir quand recharger la config

# ============================================================
# BOUCLE PRINCIPALE
# ============================================================
while True:
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    t  += 2

    # --- Rechargement de la config toutes les 10 secondes ---
    if t - dernier_rechargement >= 10:
        config = charger_config()
        dernier_rechargement = t
        print("🔄 Configuration rechargée")

    seuils  = config["seuils"]
    capteurs = config["capteurs"]

    # --- Boucle sur chaque capteur défini dans config.json ---
    for capteur in capteurs:
        machine_id    = capteur["machine_id"]
        type_capteur  = capteur["type_capteur"]
        valeur_nom    = capteur["valeur_nominale"]
        theta         = capteur["theta"]
        sigma         = capteur["sigma"]

        # Initialiser l'état du capteur s'il est nouveau
        if machine_id not in etats_capteurs:
            etats_capteurs[machine_id] = {
                "valeur": valeur_nom,
                "etat"  : "NORMAL"
            }

        etat_actuel = etats_capteurs[machine_id]
        valeur      = etat_actuel["valeur"]
        etat        = etat_actuel["etat"]

        # --- Seuil d'anomalie selon le type de capteur ---
        if type_capteur == "temperature":
            seuil_anomalie = seuils["temperature_max"]
            cycle_amp      = 3.0
            cycle_per      = 600
        else:  # vibration
            seuil_anomalie = seuils["vibration_max"]
            cycle_amp      = 0.2
            cycle_per      = 300

        # --- Machine à états ---
        if etat == "NORMAL":
            if random.random() < PROB_ANOMALIE:
                etat = "ANOMALIE"
                print(f"⚠️  ANOMALIE {type_capteur.upper()} sur {machine_id} !")
            cible = valeur_nom

        elif etat == "ANOMALIE":
            cible = seuil_anomalie * 0.95  # dérive vers 95% du seuil
            if random.random() < PROB_RECUPERATION:
                etat = "RECUPERATION"
                print(f"🔧 RÉCUPÉRATION {type_capteur.upper()} sur {machine_id}...")

        else:  # RECUPERATION
            cible = valeur_nom
            if abs(valeur - valeur_nom) < (0.1 if type_capteur == "vibration" else 2.0):
                etat = "NORMAL"
                print(f"✅ {type_capteur.upper()} {machine_id} revenue à la normale")

        # --- Calcul de la nouvelle valeur ---
        valeur  = ornstein_uhlenbeck(valeur, cible, theta, sigma)
        valeur += cycle_sinusoidal(cycle_amp, cycle_per, t)
        valeur  = round(max(0.0, valeur), 2)

        # --- Sauvegarder l'état mis à jour ---
        etats_capteurs[machine_id] = {"valeur": valeur, "etat": etat}

        # --- Déterminer le topic MQTT selon le type ---
        topic = "raffinerie/temp" if type_capteur == "temperature" else "raffinerie/vib"

        # --- Publication MQTT ---
        message = json.dumps({
            "machine_id"   : machine_id,
            "valeur"       : valeur,
            "timestamp"    : now,
            "type_capteur" : type_capteur,
            "etat"         : etat
        })

        client.publish(topic, message)
        print(f"[{now}] {machine_id} | {type_capteur}: {valeur} ({etat})")

    time.sleep(2)