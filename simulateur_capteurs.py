import time, json, random, math
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

# ============================================================
# PARAMÈTRES DES CAPTEURS
# ============================================================

# Température (pipe-101)
# Une pipeline de raffinerie tourne normalement autour de 80°C
TEMP_MOYENNE   = 80.0   # valeur nominale (°C)
TEMP_THETA     = 0.08   # vitesse de retour vers la moyenne (0=aucun retour, 1=retour immédiat)
TEMP_SIGMA     = 0.6    # niveau de bruit (plus grand = plus agité)
TEMP_CYCLE_AMP = 3.0    # amplitude du cycle thermique (°C)
TEMP_CYCLE_PER = 600    # période du cycle (secondes) → 1 cycle toutes les 10 min

# Vibration (pump-303)
# Une pompe industrielle normale vibre entre 0.5 et 1.5 mm/s
VIB_MOYENNE   = 1.0
VIB_THETA     = 0.1
VIB_SIGMA     = 0.05
VIB_CYCLE_AMP = 0.2
VIB_CYCLE_PER = 300     # 1 cycle toutes les 5 min (cycle de la pompe)

# Machine à états
PROB_ANOMALIE      = 0.002  # ~0.2% de chance par mesure de déclencher une anomalie
PROB_RECUPERATION  = 0.01   # ~1% de chance de commencer la récupération

# ============================================================
# ÉTAT INITIAL
# ============================================================
temp       = TEMP_MOYENNE
vib        = VIB_MOYENNE
etat_temp  = "NORMAL"   # états possibles : NORMAL, ANOMALIE, RECUPERATION
etat_vib   = "NORMAL"
t          = 0           # compteur de temps en secondes

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
    Processus de retour vers la moyenne (modèle OU).
    Chaque nouvelle valeur = valeur précédente
                           + force de rappel vers la moyenne
                           + petit bruit gaussien
    Cela produit des courbes lisses et continues comme de vrais capteurs.
    """
    bruit = random.gauss(0, 1)
    return valeur + theta * (moyenne - valeur) + sigma * bruit

def cycle_sinusoidal(amplitude, periode, t):
    """
    Composante cyclique lente simulant les cycles thermiques
    ou les cycles de charge de la pompe.
    """
    return amplitude * math.sin(2 * math.pi * t / periode)

# ============================================================
# BOUCLE PRINCIPALE
# ============================================================
while True:
    now = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
    t += 2  # on avance de 2 secondes à chaque itération

    # --- Machine à états : TEMPÉRATURE ---
    if etat_temp == "NORMAL":
        if random.random() < PROB_ANOMALIE:
            etat_temp = "ANOMALIE"
            print("⚠️  ANOMALIE TEMPÉRATURE — surchauffe détectée !")
        cible_temp = TEMP_MOYENNE

    elif etat_temp == "ANOMALIE":
        cible_temp = 120.0  # la température dérive progressivement vers 120°C
        if random.random() < PROB_RECUPERATION:
            etat_temp = "RECUPERATION"
            print("🔧 RÉCUPÉRATION TEMPÉRATURE en cours...")

    else:  # RECUPERATION
        cible_temp = TEMP_MOYENNE
        if abs(temp - TEMP_MOYENNE) < 2.0:
            etat_temp = "NORMAL"
            print("✅ TEMPÉRATURE revenue à la normale")

    # --- Machine à états : VIBRATION ---
    if etat_vib == "NORMAL":
        if random.random() < PROB_ANOMALIE:
            etat_vib = "ANOMALIE"
            print("⚠️  ANOMALIE VIBRATION — usure détectée !")
        cible_vib = VIB_MOYENNE

    elif etat_vib == "ANOMALIE":
        cible_vib = 3.5  # vibration dérive vers 3.5 mm/s (roulement usé)
        if random.random() < PROB_RECUPERATION:
            etat_vib = "RECUPERATION"
            print("🔧 RÉCUPÉRATION VIBRATION en cours...")

    else:  # RECUPERATION
        cible_vib = VIB_MOYENNE
        if abs(vib - VIB_MOYENNE) < 0.1:
            etat_vib = "NORMAL"
            print("✅ VIBRATION revenue à la normale")

    # --- Calcul des nouvelles valeurs ---
    temp = ornstein_uhlenbeck(temp, cible_temp, TEMP_THETA, TEMP_SIGMA)
    temp += cycle_sinusoidal(TEMP_CYCLE_AMP, TEMP_CYCLE_PER, t)
    temp  = round(max(30.0, min(150.0, temp)), 2)  # on reste dans les limites physiques

    vib = ornstein_uhlenbeck(vib, cible_vib, VIB_THETA, VIB_SIGMA)
    vib += cycle_sinusoidal(VIB_CYCLE_AMP, VIB_CYCLE_PER, t)
    vib  = round(max(0.0, min(5.0, vib)), 2)

    # --- Publication MQTT ---
    msg_temp = json.dumps({
        "machine_id"   : "pipe-101",
        "valeur"       : temp,
        "timestamp"    : now,
        "type_capteur" : "temperature",
        "etat"         : etat_temp
    })

    msg_vib = json.dumps({
        "machine_id"   : "pump-303",
        "valeur"       : vib,
        "timestamp"    : now,
        "type_capteur" : "vibration",
        "etat"         : etat_vib
    })

    client.publish("raffinerie/temp", msg_temp)
    client.publish("raffinerie/vib", msg_vib)

    print(f"[{now}] Temp: {temp}°C ({etat_temp}) | Vib: {vib} mm/s ({etat_vib})")
    time.sleep(2)