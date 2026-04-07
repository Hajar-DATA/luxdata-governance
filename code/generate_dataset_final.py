"""
Maison Éclat — Génération du dataset produit synthétique (version finale)
Mission Adone Conseil — Audit qualité données produit
1 076 SKUs, 33 colonnes, 83 modèles, 7 types de problèmes injectés

Ce script génère le dataset tel qu'utilisé dans le Livrable 2 (Diagnostic).
Seed fixé pour reproductibilité exacte.
"""

import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

# ═══════════════════════════════════════════════════════════════════════
# 1. RÉFÉRENTIELS
# ═══════════════════════════════════════════════════════════════════════

COULEURS = [
    "Noir", "Cognac", "Camel", "Bordeaux", "Marine", "Olive",
    "Crème", "Gris Perle", "Bleu Nuit", "Terracotta", "Miel",
    "Châtaigne", "Sable", "Vert Sapin", "Rose Poudré",
    "Blanc Cassé", "Encre", "Grenat", "Tabac", "Brique"
]

MATIERES_PRINCIPALES = [
    "Veau grainé", "Veau lisse", "Chèvre souple", "Veau tannage végétal",
    "Taurillon", "Agneau", "Cuir Swift"
]

MATIERES_SECONDAIRES = ["Toile coton", "Lin naturel", "Daim", "Coton ciré", ""]

ATELIERS = ["Tarn 1", "Tarn 2", "Paris"]

FERMETURES = [
    "Fermoir magnétique", "Zip", "Fermoir tourniquet", "Boucle ardillon",
    "Pression", "Rabat sans fermeture", "Fermoir crochet"
]

TAILLES_CEINTURES = ["80", "85", "90", "95", "100", "105"]
TAILLES_CEINT_CAPSULE = ["85", "90", "95"]

FOURNISSEURS = [
    "Tannerie Degermann", "Tannerie Masure", "Tannerie d'Annonay",
    "Tannerie Roux", "Haas", "Tannerie du Puy"
]
ORIGINES_DIRTY = ["France", "FR", "Italie", "IT", "Italia", "Espagne", "ES", "España"]


# ═══════════════════════════════════════════════════════════════════════
# 2. DÉFINITION DES MODÈLES (~83 modèles)
# ═══════════════════════════════════════════════════════════════════════

models = []

# ─── SACS PERMANENTS (20 modèles) ───
sacs_permanents = [
    {"id": "ECL-1987", "nom": "Éclat 1987", "sous_cat": "Cabas", "ligne": "Permanente Sellier",
     "prix_base": 2200, "couleurs": 15, "tailles": ["PM", "MM", "GM"], "fermeture": "Fermoir tourniquet"},
    {"id": "SEL-CAB", "nom": "Sellier Cabas", "sous_cat": "Cabas", "ligne": "Permanente Sellier",
     "prix_base": 2800, "couleurs": 12, "tailles": ["PM", "GM"], "fermeture": "Fermoir magnétique"},
    {"id": "SEL-BND", "nom": "Sellier Bandoulière", "sous_cat": "Bandoulière", "ligne": "Permanente Sellier",
     "prix_base": 1900, "couleurs": 12, "tailles": ["PM", "MM"], "fermeture": "Fermoir tourniquet"},
    {"id": "SEL-SCU", "nom": "Sellier Seau", "sous_cat": "Sac seau", "ligne": "Permanente Sellier",
     "prix_base": 2100, "couleurs": 10, "tailles": ["Unique"], "fermeture": "Lien coulissant"},
    {"id": "NUI-SOI", "nom": "Nuit de Soirée", "sous_cat": "Pochette de soirée", "ligne": "Permanente Sellier",
     "prix_base": 1400, "couleurs": 12, "tailles": ["Unique"], "fermeture": "Fermoir magnétique"},
    {"id": "NUI-MIN", "nom": "Nuit Mini", "sous_cat": "Mini-sac", "ligne": "Permanente Sellier",
     "prix_base": 1200, "couleurs": 15, "tailles": ["Unique"], "fermeture": "Zip"},
    {"id": "CLS-TOT", "nom": "Classique Tote", "sous_cat": "Cabas", "ligne": "Permanente Classique",
     "prix_base": 1800, "couleurs": 12, "tailles": ["PM", "GM"], "fermeture": "Rabat sans fermeture"},
    {"id": "CLS-BND", "nom": "Classique Bandoulière", "sous_cat": "Bandoulière", "ligne": "Permanente Classique",
     "prix_base": 1600, "couleurs": 12, "tailles": ["PM", "MM"], "fermeture": "Fermoir magnétique"},
    {"id": "CLS-HBR", "nom": "Classique Hobo Rond", "sous_cat": "Bandoulière", "ligne": "Permanente Classique",
     "prix_base": 1500, "couleurs": 10, "tailles": ["Unique"], "fermeture": "Zip"},
    {"id": "CLS-PCH", "nom": "Classique Pochette", "sous_cat": "Pochette de soirée", "ligne": "Permanente Classique",
     "prix_base": 1350, "couleurs": 10, "tailles": ["Unique"], "fermeture": "Fermoir magnétique"},
    {"id": "CLS-MIN", "nom": "Classique Mini", "sous_cat": "Mini-sac", "ligne": "Permanente Classique",
     "prix_base": 1250, "couleurs": 12, "tailles": ["Unique"], "fermeture": "Pression"},
    {"id": "SEL-DOC", "nom": "Sellier Document", "sous_cat": "Cabas", "ligne": "Permanente Sellier",
     "prix_base": 2500, "couleurs": 8, "tailles": ["Unique"], "fermeture": "Zip"},
    {"id": "CLS-SCU", "nom": "Classique Seau", "sous_cat": "Sac seau", "ligne": "Permanente Classique",
     "prix_base": 1700, "couleurs": 10, "tailles": ["PM", "GM"], "fermeture": "Lien coulissant"},
    {"id": "SEL-VYG", "nom": "Sellier Voyage", "sous_cat": "Cabas", "ligne": "Permanente Sellier",
     "prix_base": 3200, "couleurs": 6, "tailles": ["Unique"], "fermeture": "Zip"},
    {"id": "CLS-ENV", "nom": "Classique Enveloppe", "sous_cat": "Pochette de soirée", "ligne": "Permanente Classique",
     "prix_base": 1300, "couleurs": 10, "tailles": ["Unique"], "fermeture": "Fermoir magnétique"},
    {"id": "SEL-BST", "nom": "Sellier Besace", "sous_cat": "Bandoulière", "ligne": "Permanente Sellier",
     "prix_base": 2050, "couleurs": 12, "tailles": ["PM", "GM"], "fermeture": "Fermoir magnétique"},
    {"id": "CLS-RND", "nom": "Classique Rond", "sous_cat": "Mini-sac", "ligne": "Permanente Classique",
     "prix_base": 1450, "couleurs": 10, "tailles": ["Unique"], "fermeture": "Zip"},
    {"id": "SEL-PLT", "nom": "Sellier Plat", "sous_cat": "Pochette de soirée", "ligne": "Permanente Sellier",
     "prix_base": 1550, "couleurs": 8, "tailles": ["PM", "GM"], "fermeture": "Zip"},
    {"id": "CLS-TRP", "nom": "Classique Trapèze", "sous_cat": "Cabas", "ligne": "Permanente Classique",
     "prix_base": 1950, "couleurs": 12, "tailles": ["PM", "MM", "GM"], "fermeture": "Fermoir tourniquet"},
    {"id": "SEL-CIT", "nom": "Sellier City", "sous_cat": "Bandoulière", "ligne": "Permanente Sellier",
     "prix_base": 1750, "couleurs": 10, "tailles": ["PM", "MM"], "fermeture": "Fermoir magnétique"},
]

# ─── SACS CAPSULES (13 modèles) ───
sacs_capsules = [
    {"id": "CAP-AH25-01", "nom": "Aurore Cabas", "sous_cat": "Cabas", "ligne": "Capsule AH25",
     "prix_base": 2400, "couleurs": 4, "tailles": ["PM", "GM"], "fermeture": "Fermoir tourniquet"},
    {"id": "CAP-AH25-02", "nom": "Aurore Bandoulière", "sous_cat": "Bandoulière", "ligne": "Capsule AH25",
     "prix_base": 1950, "couleurs": 4, "tailles": ["Unique"], "fermeture": "Fermoir magnétique"},
    {"id": "CAP-AH25-03", "nom": "Aurore Mini", "sous_cat": "Mini-sac", "ligne": "Capsule AH25",
     "prix_base": 1350, "couleurs": 5, "tailles": ["Unique"], "fermeture": "Zip"},
    {"id": "CAP-PE26-01", "nom": "Solstice Cabas", "sous_cat": "Cabas", "ligne": "Capsule PE26",
     "prix_base": 2300, "couleurs": 5, "tailles": ["PM", "MM"], "fermeture": "Fermoir tourniquet"},
    {"id": "CAP-PE26-02", "nom": "Solstice Seau", "sous_cat": "Sac seau", "ligne": "Capsule PE26",
     "prix_base": 2000, "couleurs": 4, "tailles": ["Unique"], "fermeture": "Lien coulissant"},
    {"id": "CAP-PE26-03", "nom": "Solstice Mini", "sous_cat": "Mini-sac", "ligne": "Capsule PE26",
     "prix_base": 1400, "couleurs": 5, "tailles": ["Unique"], "fermeture": "Pression"},
    {"id": "CAP-PE26-04", "nom": "Solstice Pochette", "sous_cat": "Pochette de soirée", "ligne": "Capsule PE26",
     "prix_base": 1500, "couleurs": 4, "tailles": ["Unique"], "fermeture": "Fermoir magnétique"},
    {"id": "COL-PE26-01", "nom": "Collab Keiko Cabas", "sous_cat": "Cabas", "ligne": "Capsule Collab Artiste PE26",
     "prix_base": 2600, "couleurs": 3, "tailles": ["PM", "GM"], "fermeture": "Fermoir tourniquet"},
    {"id": "COL-PE26-02", "nom": "Collab Keiko Mini", "sous_cat": "Mini-sac", "ligne": "Capsule Collab Artiste PE26",
     "prix_base": 1600, "couleurs": 3, "tailles": ["Unique"], "fermeture": "Zip"},
    {"id": "COL-PE26-03", "nom": "Collab Keiko Pochette", "sous_cat": "Pochette de soirée", "ligne": "Capsule Collab Artiste PE26",
     "prix_base": 1800, "couleurs": 3, "tailles": ["Unique"], "fermeture": "Fermoir crochet"},
    {"id": "PRE-AH26-01", "nom": "Crépuscule Cabas", "sous_cat": "Cabas", "ligne": "Pré-lancement AH26",
     "prix_base": 2350, "couleurs": 4, "tailles": ["PM", "GM"], "fermeture": "Fermoir tourniquet"},
    {"id": "PRE-AH26-02", "nom": "Crépuscule Bandoulière", "sous_cat": "Bandoulière", "ligne": "Pré-lancement AH26",
     "prix_base": 1850, "couleurs": 3, "tailles": ["PM", "MM"], "fermeture": "Fermoir magnétique"},
    {"id": "PRE-AH26-03", "nom": "Crépuscule Mini", "sous_cat": "Mini-sac", "ligne": "Pré-lancement AH26",
     "prix_base": 1350, "couleurs": 4, "tailles": ["Unique"], "fermeture": "Zip"},
]

for m in sacs_permanents:
    m["categorie"] = "Sacs"
    m["saison"] = "Permanent"
    m["statut"] = "Actif"
for m in sacs_capsules:
    m["categorie"] = "Sacs"
    if "AH25" in m["ligne"]:      m["saison"], m["statut"] = "AH25", "Actif"
    elif "Collab" in m["ligne"]:   m["saison"], m["statut"] = "PE26", "Actif"
    elif "PE26" in m["ligne"]:     m["saison"], m["statut"] = "PE26", "Actif"
    elif "Pré-lancement" in m["ligne"]: m["saison"], m["statut"] = "AH26", "Pré-lancement"

models.extend(sacs_permanents)
models.extend(sacs_capsules)

# ─── PETITE MAROQUINERIE (20 modèles) ───
pm_models = [
    {"id": "PM-PFE-SEL", "nom": "Portefeuille Sellier", "sous_cat": "Portefeuille", "ligne": "Permanente Sellier", "prix_base": 520, "couleurs": 12, "tailles": ["Unique"]},
    {"id": "PM-PFE-CLS", "nom": "Portefeuille Classique", "sous_cat": "Portefeuille", "ligne": "Permanente Classique", "prix_base": 450, "couleurs": 12, "tailles": ["Unique"]},
    {"id": "PM-PFE-CMP", "nom": "Portefeuille Compact", "sous_cat": "Portefeuille", "ligne": "Permanente Classique", "prix_base": 380, "couleurs": 10, "tailles": ["Unique"]},
    {"id": "PM-PCA-SEL", "nom": "Porte-cartes Sellier", "sous_cat": "Porte-cartes", "ligne": "Permanente Sellier", "prix_base": 280, "couleurs": 15, "tailles": ["Unique"]},
    {"id": "PM-PCA-CLS", "nom": "Porte-cartes Classique", "sous_cat": "Porte-cartes", "ligne": "Permanente Classique", "prix_base": 220, "couleurs": 12, "tailles": ["Unique"]},
    {"id": "PM-PCA-ZIP", "nom": "Porte-cartes Zippé", "sous_cat": "Porte-cartes", "ligne": "Permanente Classique", "prix_base": 250, "couleurs": 10, "tailles": ["Unique"]},
    {"id": "PM-PCH-SEL", "nom": "Pochette Sellier", "sous_cat": "Pochette", "ligne": "Permanente Sellier", "prix_base": 480, "couleurs": 12, "tailles": ["Unique"]},
    {"id": "PM-PCH-CLS", "nom": "Pochette Classique", "sous_cat": "Pochette", "ligne": "Permanente Classique", "prix_base": 420, "couleurs": 10, "tailles": ["Unique"]},
    {"id": "PM-ETU-LUN", "nom": "Étui Lunettes", "sous_cat": "Étui", "ligne": "Permanente Classique", "prix_base": 280, "couleurs": 8, "tailles": ["Unique"]},
    {"id": "PM-ETU-TEL", "nom": "Étui Téléphone", "sous_cat": "Étui", "ligne": "Permanente Classique", "prix_base": 320, "couleurs": 10, "tailles": ["Unique"]},
    {"id": "PM-PMN-SEL", "nom": "Porte-monnaie Sellier", "sous_cat": "Portefeuille", "ligne": "Permanente Sellier", "prix_base": 350, "couleurs": 10, "tailles": ["Unique"]},
    {"id": "PM-PMN-CLS", "nom": "Porte-monnaie Classique", "sous_cat": "Portefeuille", "ligne": "Permanente Classique", "prix_base": 290, "couleurs": 8, "tailles": ["Unique"]},
    {"id": "PM-CAP-AH25", "nom": "Aurore Porte-cartes", "sous_cat": "Porte-cartes", "ligne": "Capsule AH25", "prix_base": 260, "couleurs": 4, "tailles": ["Unique"]},
    {"id": "PM-CAP-PE26-01", "nom": "Solstice Pochette", "sous_cat": "Pochette", "ligne": "Capsule PE26", "prix_base": 450, "couleurs": 4, "tailles": ["Unique"]},
    {"id": "PM-CAP-PE26-02", "nom": "Solstice Porte-cartes", "sous_cat": "Porte-cartes", "ligne": "Capsule PE26", "prix_base": 240, "couleurs": 5, "tailles": ["Unique"]},
    {"id": "PM-COL-PE26", "nom": "Collab Keiko Pochette", "sous_cat": "Pochette", "ligne": "Capsule Collab Artiste PE26", "prix_base": 550, "couleurs": 3, "tailles": ["Unique"]},
    {"id": "PM-PRE-AH26-01", "nom": "Crépuscule Portefeuille", "sous_cat": "Portefeuille", "ligne": "Pré-lancement AH26", "prix_base": 490, "couleurs": 3, "tailles": ["Unique"]},
    {"id": "PM-PRE-AH26-02", "nom": "Crépuscule Porte-cartes", "sous_cat": "Porte-cartes", "ligne": "Pré-lancement AH26", "prix_base": 260, "couleurs": 3, "tailles": ["Unique"]},
    {"id": "PM-PFE-VYG", "nom": "Portefeuille Voyage", "sous_cat": "Portefeuille", "ligne": "Permanente Sellier", "prix_base": 580, "couleurs": 10, "tailles": ["Unique"]},
    {"id": "PM-PCH-ENV", "nom": "Pochette Enveloppe", "sous_cat": "Pochette", "ligne": "Permanente Classique", "prix_base": 390, "couleurs": 10, "tailles": ["Unique"]},
]

for m in pm_models:
    m["categorie"] = "Petite Maroquinerie"
    m["fermeture"] = random.choice(["Zip", "Pression", "Fermoir magnétique", "Rabat sans fermeture"])
    if "Pré-lancement" in m["ligne"]:  m["saison"], m["statut"] = "AH26", "Pré-lancement"
    elif "AH25" in m["ligne"]:         m["saison"], m["statut"] = "AH25", "Actif"
    elif "PE26" in m["ligne"] or "Collab" in m["ligne"]: m["saison"], m["statut"] = "PE26", "Actif"
    else:                              m["saison"], m["statut"] = "Permanent", "Actif"

models.extend(pm_models)

# ─── CEINTURES (12 modèles) ───
ceint_models = [
    {"id": "CE-SEL-30", "nom": "Ceinture Sellier 30mm", "sous_cat": "Sellier", "ligne": "Permanente Sellier", "prix_base": 480, "couleurs": 5},
    {"id": "CE-SEL-35", "nom": "Ceinture Sellier 35mm", "sous_cat": "Sellier", "ligne": "Permanente Sellier", "prix_base": 520, "couleurs": 5},
    {"id": "CE-CLS-25", "nom": "Ceinture Classique 25mm", "sous_cat": "Classique", "ligne": "Permanente Classique", "prix_base": 380, "couleurs": 6},
    {"id": "CE-CLS-30", "nom": "Ceinture Classique 30mm", "sous_cat": "Classique", "ligne": "Permanente Classique", "prix_base": 420, "couleurs": 5},
    {"id": "CE-REV-30", "nom": "Ceinture Réversible 30mm", "sous_cat": "Réversible", "ligne": "Permanente Classique", "prix_base": 450, "couleurs": 4},
    {"id": "CE-REV-35", "nom": "Ceinture Réversible 35mm", "sous_cat": "Réversible", "ligne": "Permanente Classique", "prix_base": 490, "couleurs": 4},
    {"id": "CE-TRS-20", "nom": "Ceinture Tressée 20mm", "sous_cat": "Classique", "ligne": "Permanente Classique", "prix_base": 350, "couleurs": 4},
    {"id": "CE-CAP-AH25", "nom": "Aurore Ceinture", "sous_cat": "Sellier", "ligne": "Capsule AH25", "prix_base": 460, "couleurs": 3},
    {"id": "CE-CAP-PE26", "nom": "Solstice Ceinture", "sous_cat": "Classique", "ligne": "Capsule PE26", "prix_base": 430, "couleurs": 3},
    {"id": "CE-COL-PE26", "nom": "Collab Keiko Ceinture", "sous_cat": "Sellier", "ligne": "Capsule Collab Artiste PE26", "prix_base": 520, "couleurs": 2},
    {"id": "CE-PRE-AH26-01", "nom": "Crépuscule Ceinture 30mm", "sous_cat": "Sellier", "ligne": "Pré-lancement AH26", "prix_base": 470, "couleurs": 3},
    {"id": "CE-PRE-AH26-02", "nom": "Crépuscule Ceinture Fine", "sous_cat": "Classique", "ligne": "Pré-lancement AH26", "prix_base": 360, "couleurs": 3},
]

for m in ceint_models:
    m["categorie"] = "Ceintures"
    m["tailles"] = TAILLES_CEINTURES if "Permanent" in m.get("ligne", "") else TAILLES_CEINT_CAPSULE
    m["fermeture"] = "Boucle ardillon"
    if "Pré-lancement" in m["ligne"]:  m["saison"], m["statut"] = "AH26", "Pré-lancement"
    elif "AH25" in m["ligne"]:         m["saison"], m["statut"] = "AH25", "Actif"
    elif "PE26" in m["ligne"] or "Collab" in m["ligne"]: m["saison"], m["statut"] = "PE26", "Actif"
    else:                              m["saison"], m["statut"] = "Permanent", "Actif"

models.extend(ceint_models)

# ─── BIJOUX & ACCESSOIRES (18 modèles) ───
bij_models = [
    {"id": "BJ-BRC-SEL", "nom": "Bracelet Sellier Simple", "sous_cat": "Bracelet cuir", "ligne": "Permanente Sellier", "prix_base": 280, "couleurs": 12, "tailles": ["S", "M", "L"]},
    {"id": "BJ-BRC-DBL", "nom": "Bracelet Sellier Double Tour", "sous_cat": "Bracelet cuir", "ligne": "Permanente Sellier", "prix_base": 380, "couleurs": 10, "tailles": ["S", "M", "L"]},
    {"id": "BJ-BRC-CLS", "nom": "Bracelet Classique", "sous_cat": "Bracelet cuir", "ligne": "Permanente Classique", "prix_base": 220, "couleurs": 12, "tailles": ["S", "M", "L"]},
    {"id": "BJ-FOU-90", "nom": "Foulard Soie 90", "sous_cat": "Foulard soie", "ligne": "Permanente Classique", "prix_base": 350, "couleurs": 8, "tailles": ["Unique"]},
    {"id": "BJ-FOU-70", "nom": "Foulard Soie 70", "sous_cat": "Foulard soie", "ligne": "Permanente Classique", "prix_base": 280, "couleurs": 8, "tailles": ["Unique"]},
    {"id": "BJ-FOU-TW", "nom": "Twilly Soie", "sous_cat": "Foulard soie", "ligne": "Permanente Classique", "prix_base": 180, "couleurs": 12, "tailles": ["Unique"]},
    {"id": "BJ-BOD-CLS", "nom": "Boucles d'oreilles Classique", "sous_cat": "Boucles d'oreilles", "ligne": "Permanente Classique", "prix_base": 320, "couleurs": 3, "tailles": ["Unique"]},
    {"id": "BJ-BOD-SEL", "nom": "Boucles d'oreilles Sellier", "sous_cat": "Boucles d'oreilles", "ligne": "Permanente Sellier", "prix_base": 420, "couleurs": 3, "tailles": ["Unique"]},
    {"id": "BJ-PCL-SEL", "nom": "Porte-clés Sellier", "sous_cat": "Porte-clés", "ligne": "Permanente Sellier", "prix_base": 190, "couleurs": 8, "tailles": ["Unique"]},
    {"id": "BJ-PCL-CLS", "nom": "Porte-clés Classique", "sous_cat": "Porte-clés", "ligne": "Permanente Classique", "prix_base": 150, "couleurs": 6, "tailles": ["Unique"]},
    {"id": "BJ-CHM-SEL", "nom": "Charm de Sac Sellier", "sous_cat": "Porte-clés", "ligne": "Permanente Sellier", "prix_base": 250, "couleurs": 6, "tailles": ["Unique"]},
    {"id": "BJ-BND-CLS", "nom": "Bandeau Soie", "sous_cat": "Foulard soie", "ligne": "Permanente Classique", "prix_base": 220, "couleurs": 5, "tailles": ["Unique"]},
    {"id": "BJ-CAP-AH25-01", "nom": "Aurore Bracelet", "sous_cat": "Bracelet cuir", "ligne": "Capsule AH25", "prix_base": 300, "couleurs": 3, "tailles": ["S", "M", "L"]},
    {"id": "BJ-CAP-AH25-02", "nom": "Aurore Foulard", "sous_cat": "Foulard soie", "ligne": "Capsule AH25", "prix_base": 320, "couleurs": 3, "tailles": ["Unique"]},
    {"id": "BJ-CAP-PE26", "nom": "Solstice Bracelet", "sous_cat": "Bracelet cuir", "ligne": "Capsule PE26", "prix_base": 290, "couleurs": 4, "tailles": ["S", "M", "L"]},
    {"id": "BJ-COL-PE26-01", "nom": "Collab Keiko Foulard", "sous_cat": "Foulard soie", "ligne": "Capsule Collab Artiste PE26", "prix_base": 420, "couleurs": 3, "tailles": ["Unique"]},
    {"id": "BJ-COL-PE26-02", "nom": "Collab Keiko Charm", "sous_cat": "Porte-clés", "ligne": "Capsule Collab Artiste PE26", "prix_base": 280, "couleurs": 3, "tailles": ["Unique"]},
    {"id": "BJ-PRE-AH26", "nom": "Crépuscule Bracelet", "sous_cat": "Bracelet cuir", "ligne": "Pré-lancement AH26", "prix_base": 310, "couleurs": 3, "tailles": ["S", "M", "L"]},
]

for m in bij_models:
    m["categorie"] = "Bijoux & Accessoires"
    m["fermeture"] = random.choice(["Fermoir magnétique", "Fermoir crochet", "Pression", ""])
    if "Pré-lancement" in m["ligne"]:  m["saison"], m["statut"] = "AH26", "Pré-lancement"
    elif "AH25" in m["ligne"]:         m["saison"], m["statut"] = "AH25", "Actif"
    elif "PE26" in m["ligne"] or "Collab" in m["ligne"]: m["saison"], m["statut"] = "PE26", "Actif"
    else:                              m["saison"], m["statut"] = "Permanent", "Actif"

models.extend(bij_models)

print(f"Modèles définis: {len(models)}")


# ═══════════════════════════════════════════════════════════════════════
# 3. GÉNÉRATION DES SKUs (données propres)
# ═══════════════════════════════════════════════════════════════════════

def make_sku_code(model_id, couleur, taille):
    c = couleur[:3].upper()
    t = taille[:3].upper() if taille != "Unique" else "UNI"
    return f"{model_id}-{c}-{t}"

def gen_description_fr(nom, couleur, matiere, cat):
    templates = {
        "Sacs": [
            f"{nom} en {matiere.lower()}, coloris {couleur.lower()}. Couture sellier à la main. Fabriqué en France.",
            f"Sac {nom} en {matiere.lower()} {couleur.lower()}. Finitions artisanales, couture sellier. Fabrication française.",
            f"{nom}, {matiere.lower()} teinté dans la masse, coloris {couleur.lower()}. Intérieur coton. Made in France."
        ],
        "Petite Maroquinerie": [
            f"{nom} en {matiere.lower()}, coloris {couleur.lower()}. Finitions à la cire d'abeille. Fabriqué en France.",
            f"{nom} en {matiere.lower()} {couleur.lower()}. Coutures sellier. Fabrication artisanale française."
        ],
        "Ceintures": [
            f"Ceinture {nom.split('Ceinture ')[-1] if 'Ceinture' in nom else nom} en {matiere.lower()}, coloris {couleur.lower()}. Boucle en laiton doré. Made in France.",
            f"{nom} en {matiere.lower()} {couleur.lower()}. Boucle artisanale. Fabrication France."
        ],
        "Bijoux & Accessoires": [
            f"{nom} en {matiere.lower()}, coloris {couleur.lower()}. Fabrication artisanale française.",
            f"{nom}, {matiere.lower()} {couleur.lower()}. Fabriqué à la main dans nos ateliers."
        ]
    }
    return random.choice(templates.get(cat, templates["Sacs"]))

def gen_description_en(nom, couleur, matiere, cat):
    color_en = {"Noir": "Black", "Cognac": "Cognac", "Camel": "Camel", "Bordeaux": "Burgundy",
                "Marine": "Navy", "Olive": "Olive", "Crème": "Cream", "Gris Perle": "Pearl Grey",
                "Bleu Nuit": "Night Blue", "Terracotta": "Terracotta", "Miel": "Honey",
                "Châtaigne": "Chestnut", "Sable": "Sand", "Vert Sapin": "Forest Green",
                "Rose Poudré": "Powder Pink", "Blanc Cassé": "Off-White", "Encre": "Ink",
                "Grenat": "Garnet", "Tabac": "Tobacco", "Brique": "Brick"}.get(couleur, couleur)
    mat_en = {"Veau grainé": "Grained calfskin", "Veau lisse": "Smooth calfskin",
              "Chèvre souple": "Supple goatskin", "Veau tannage végétal": "Vegetable-tanned calfskin",
              "Taurillon": "Bull calfskin", "Agneau": "Lambskin", "Cuir Swift": "Swift leather"}.get(matiere, matiere)
    return f"{nom} in {mat_en.lower()}, {color_en.lower()}. Saddle-stitched by hand. Made in France."

def gen_poids(cat, taille):
    base = {"Sacs": 600, "Petite Maroquinerie": 120, "Ceintures": 180, "Bijoux & Accessoires": 80}[cat]
    if taille == "GM": base *= 1.4
    elif taille == "PM": base *= 0.8
    return int(base + random.randint(-50, 50))

def gen_dimensions(cat, taille):
    dims = {
        "Sacs": {"PM": "25x18x10", "MM": "30x22x12", "GM": "38x28x15", "Unique": "28x20x11"},
        "Petite Maroquinerie": {"Unique": "12x9x2"},
        "Ceintures": {"80": "80x3", "85": "85x3", "90": "90x3", "95": "95x3", "100": "100x3", "105": "105x3", "Unique": "90x3"},
        "Bijoux & Accessoires": {"S": "16x1", "M": "18x1", "L": "20x1", "Unique": "90x90"}
    }
    return dims.get(cat, {}).get(taille, "20x10x5")

rows = []
for m in models:
    couleurs_pool = random.sample(COULEURS, min(m["couleurs"], len(COULEURS)))
    tailles = m.get("tailles", ["Unique"])
    matiere = random.choice(MATIERES_PRINCIPALES)

    for couleur in couleurs_pool:
        for taille in tailles:
            sku = make_sku_code(m["id"], couleur, taille)
            nom_complet = f"{m['nom']} {couleur}"
            if taille != "Unique":
                nom_complet += f" {taille}"

            poids = gen_poids(m["categorie"], taille)
            dims = gen_dimensions(m["categorie"], taille)

            prix_retail = m["prix_base"]
            if taille == "GM": prix_retail = int(prix_retail * 1.2)
            elif taille == "PM" and m["categorie"] == "Sacs": prix_retail = int(prix_retail * 0.85)
            prix_retail = round(prix_retail * random.uniform(0.97, 1.03), -1)
            prix_retail = max(prix_retail, 100)

            prix_ecom = prix_retail
            prix_wholesale = round(prix_retail * random.uniform(0.45, 0.55), -1)

            dispo_retail = True
            dispo_ecom = True
            dispo_wholesale = random.random() > 0.3
            if m["statut"] == "Pré-lancement":
                dispo_retail = dispo_ecom = dispo_wholesale = False

            desc_fr = gen_description_fr(m["nom"], couleur, matiere, m["categorie"])
            desc_en = gen_description_en(m["nom"], couleur, matiere, m["categorie"])
            desc_zh = ""

            photo = True
            nb_visuels = random.choice([3, 4, 5, 6])
            comp = ""
            mat_sec = random.choice(MATIERES_SECONDAIRES)
            atelier = random.choice(ATELIERS)

            rows.append({
                "model_id": m["id"], "sku": sku, "nom_produit": nom_complet,
                "categorie": m["categorie"], "sous_categorie": m["sous_cat"],
                "ligne": m["ligne"], "collection_saison": m["saison"], "statut": m["statut"],
                "couleur": couleur, "matiere_principale": matiere, "matiere_secondaire": mat_sec,
                "pays_fabrication": "France", "poids": str(poids) + "g", "dimensions": dims,
                "type_fermeture": m["fermeture"], "atelier": atelier,
                "taille": taille if taille != "Unique" else "",
                "description_FR": desc_fr, "description_EN": desc_en, "description_ZH": desc_zh,
                "prix_retail_EUR": prix_retail, "prix_ecom_EUR": prix_ecom,
                "prix_wholesale_EUR": prix_wholesale,
                "dispo_retail": dispo_retail, "dispo_ecommerce": dispo_ecom,
                "dispo_wholesale": dispo_wholesale,
                "photo_principale": photo, "nb_visuels": nb_visuels,
                "composition_matiere": comp,
            })

df = pd.DataFrame(rows)
n = len(df)
print(f"SKUs générés (propres): {n}")


# ═══════════════════════════════════════════════════════════════════════
# 4. INJECTION DES PROBLÈMES DE QUALITÉ
# ═══════════════════════════════════════════════════════════════════════

# ─── P1: Complétude ───
# description_EN vide ~20%
mask_en = np.random.random(n) < 0.15
for i in df.index:
    if "Capsule" in df.at[i, "ligne"] or "Pré-lancement" in df.at[i, "ligne"]:
        if np.random.random() < 0.40: mask_en[i] = True
    elif df.at[i, "categorie"] == "Bijoux & Accessoires":
        if np.random.random() < 0.30: mask_en[i] = True
df.loc[mask_en, "description_EN"] = ""

# description_ZH — remplie sélectivement (~55% restent vides)
for i in df.index:
    if df.at[i, "collection_saison"] == "Permanent" and np.random.random() < 0.6:
        df.at[i, "description_ZH"] = f"{df.at[i, 'nom_produit']}。法国手工制作。鞍针缝制。"
    elif np.random.random() < 0.15:
        df.at[i, "description_ZH"] = f"{df.at[i, 'nom_produit']}。法国制造。"

# composition_matiere vide ~80%
for i in df.index:
    if np.random.random() < 0.20:
        mat = df.at[i, "matiere_principale"]
        if "Veau" in mat or "Taurillon" in mat:
            df.at[i, "composition_matiere"] = f"Cuir de veau {random.randint(90,98)}%, Coton {random.randint(2,10)}%"
        elif "Chèvre" in mat:
            df.at[i, "composition_matiere"] = f"Cuir de chèvre {random.randint(92,98)}%, Lin {random.randint(2,8)}%"
        else:
            df.at[i, "composition_matiere"] = f"Cuir {random.randint(90,97)}%, Textile {random.randint(3,10)}%"

# photo_principale False sur ~12%
mask_photo = np.random.random(n) < 0.12
for i in df.index:
    if df.at[i, "statut"] == "Pré-lancement" and np.random.random() < 0.4: mask_photo[i] = True
df.loc[mask_photo, "photo_principale"] = False
df.loc[mask_photo, "nb_visuels"] = 0

# nb_visuels < 3 sur ~8%
mask_low_vis = (df["photo_principale"] == True) & (np.random.random(n) < 0.06)
df.loc[mask_low_vis, "nb_visuels"] = np.random.choice([1, 2], size=mask_low_vis.sum())

# description_FR vide sur ~5%
mask_fr = np.random.random(n) < 0.05
df.loc[mask_fr, "description_FR"] = ""

# ─── P2: Incohérences prix ───
mask_prix_ecom = np.random.random(n) < 0.10
for i in df.index[mask_prix_ecom]:
    ecart = random.choice([-0.10, -0.05, 0.05, 0.08, 0.12])
    df.at[i, "prix_ecom_EUR"] = round(df.at[i, "prix_retail_EUR"] * (1 + ecart), -1)

mask_ws = np.random.random(n) < 0.05
for i in df.index[mask_ws]:
    df.at[i, "prix_wholesale_EUR"] = round(df.at[i, "prix_retail_EUR"] * random.uniform(0.65, 0.95), -1)

# ─── P3: Doublons ───
dup_indices = np.random.choice(df.index, size=35, replace=False)
duplicates = []
for idx in dup_indices:
    row = df.loc[idx].copy()
    old_sku = row["sku"]
    row["sku"] = old_sku.replace("-", "").replace("_", "-")[:20] + "-DUP"
    if row["description_FR"]: row["description_FR"] = row["description_FR"].rstrip(".") + ". "
    if np.random.random() < 0.5:
        row["prix_retail_EUR"] = row["prix_retail_EUR"] + random.choice([-10, 10, -20, 20])
    duplicates.append(row)
df = pd.concat([df, pd.DataFrame(duplicates)], ignore_index=True)

# ─── P4: Catégorisation incohérente ───
n_new = len(df)
mask_cat = np.random.random(n_new) < 0.08
cat_variants = {
    "Petite Maroquinerie": ["Petite maroquinerie", "PM", "Petite maroq.", "petite maroquinerie"],
    "Bijoux & Accessoires": ["Bijoux & accessoires", "Bijoux et Accessoires", "BIJOUX", "Bijoux&Acc"],
    "Sacs": ["sacs", "SAC", "Sacs à main"],
    "Ceintures": ["ceintures", "CEINTURES", "Ceinture"]
}
for i in df.index:
    if mask_cat[i] and df.at[i, "categorie"] in cat_variants:
        df.at[i, "categorie"] = random.choice(cat_variants[df.at[i, "categorie"]])

mask_sous = np.random.random(n_new) < 0.06
sous_variants = {
    "Porte-cartes": ["Porte cartes", "Portecartes", "porte-cartes"],
    "Pochette de soirée": ["Pochette soirée", "pochette de soirée", "Pochette"],
    "Bracelet cuir": ["Bracelet Cuir", "bracelet cuir", "BRACELET"],
    "Foulard soie": ["Foulard Soie", "foulard", "FOULARD SOIE"]
}
for i in df.index:
    if mask_sous[i] and df.at[i, "sous_categorie"] in sous_variants:
        df.at[i, "sous_categorie"] = random.choice(sous_variants[df.at[i, "sous_categorie"]])

mask_coul = np.random.random(n_new) < 0.07
coul_variants = {
    "Noir": ["noir", "NOIR", "Black", "Nero"],
    "Cognac": ["cognac", "COGNAC", "Tan"],
    "Marine": ["marine", "Navy", "MARINE", "Bleu marine"],
    "Crème": ["crème", "Cream", "CREME", "Ecru"],
    "Bordeaux": ["bordeaux", "BORDEAUX", "Burgundy"]
}
for i in df.index:
    if mask_coul[i] and df.at[i, "couleur"] in coul_variants:
        df.at[i, "couleur"] = random.choice(coul_variants[df.at[i, "couleur"]])

# ─── P5: Formats hétérogènes ───
mask_poids = np.random.random(n_new) < 0.25
for i in df.index:
    if mask_poids[i]:
        val = df.at[i, "poids"]
        if val and val != "":
            num = ''.join(filter(str.isdigit, val))
            if num:
                fmt = random.choice(["plain", "kg", "space", "NC"])
                if fmt == "plain": df.at[i, "poids"] = num
                elif fmt == "kg": df.at[i, "poids"] = f"{int(num)/1000:.2f}kg"
                elif fmt == "space": df.at[i, "poids"] = f"{num} g"
                elif fmt == "NC": df.at[i, "poids"] = "NC"

mask_dims = np.random.random(n_new) < 0.20
for i in df.index:
    if mask_dims[i]:
        val = df.at[i, "dimensions"]
        if val and "x" in val:
            fmt = random.choice(["cm", "lhp", "slash", "space"])
            parts = val.split("x")
            if fmt == "cm": df.at[i, "dimensions"] = val + " cm"
            elif fmt == "lhp" and len(parts) >= 2:
                df.at[i, "dimensions"] = f"L{parts[0]} H{parts[1]}" + (f" P{parts[2]}" if len(parts) > 2 else "")
            elif fmt == "slash": df.at[i, "dimensions"] = "/".join(parts)
            elif fmt == "space": df.at[i, "dimensions"] = " x ".join(parts)

mask_taille = np.random.random(n_new) < 0.08
taille_variants = {"PM": ["Petit Modèle", "S", "pm"], "MM": ["Moyen Modèle", "M", "mm"], "GM": ["Grand Modèle", "L", "gm"]}
for i in df.index:
    if mask_taille[i] and df.at[i, "taille"] in taille_variants:
        df.at[i, "taille"] = random.choice(taille_variants[df.at[i, "taille"]])

# ─── P6: Prix aberrants ───
aberr_ceint = df[df["categorie"].str.contains("eintur", case=False, na=False)].sample(1).index[0]
df.at[aberr_ceint, "prix_retail_EUR"] = 12000
df.at[aberr_ceint, "prix_ecom_EUR"] = 12000

aberr_sac = df[df["categorie"].str.contains("Sac", case=False, na=False)].sample(1).index[0]
df.at[aberr_sac, "prix_retail_EUR"] = 0
df.at[aberr_sac, "prix_ecom_EUR"] = 0

for _ in range(2):
    idx = df.sample(1).index[0]
    df.at[idx, "prix_retail_EUR"] = -random.choice([350, 480, 220])

pc_idx = df[df["sous_categorie"].str.contains("carte", case=False, na=False)].sample(
    min(3, len(df[df["sous_categorie"].str.contains("carte", case=False, na=False)]))).index
for idx in pc_idx:
    df.at[idx, "prix_retail_EUR"] = random.choice([2200, 2500, 1800])
    df.at[idx, "prix_ecom_EUR"] = df.at[idx, "prix_retail_EUR"]

# ─── P7: Statuts incohérents ───
pe26_active = df[df["collection_saison"] == "PE26"].sample(min(10, len(df[df["collection_saison"] == "PE26"]))).index
df.loc[pe26_active, "statut"] = "Pré-lancement"

disc_idx = df.sample(5).index
df.loc[disc_idx, "statut"] = "Discontinué"
df.loc[disc_idx, "dispo_ecommerce"] = True
df.loc[disc_idx, "dispo_retail"] = False


# ═══════════════════════════════════════════════════════════════════════
# 5. AJOUT DATES (TTM) + TRAÇABILITÉ FOURNISSEUR
# ═══════════════════════════════════════════════════════════════════════

dates_creation = []
dates_mel = []

for i, row in df.iterrows():
    ligne = row['ligne']
    statut = row['statut']

    if 'Permanente' in ligne:
        days_ago = random.randint(180, 720)
        d_create = datetime(2026, 4, 1) - timedelta(days=days_ago)
        ttm = random.randint(3, 7)
        d_mel = d_create + timedelta(days=ttm)
    elif 'AH25' in ligne:
        d_create = datetime(2025, 7, 1) + timedelta(days=random.randint(0, 75))
        ttm = random.randint(10, 25)
        d_mel = d_create + timedelta(days=ttm)
    elif 'Collab' in ligne:
        d_create = datetime(2026, 2, 1) + timedelta(days=random.randint(0, 45))
        ttm = random.randint(18, 30)
        d_mel = d_create + timedelta(days=ttm)
    elif 'PE26' in ligne:
        d_create = datetime(2026, 1, 5) + timedelta(days=random.randint(0, 65))
        ttm = random.randint(14, 28)
        d_mel = d_create + timedelta(days=ttm)
    elif 'Pré-lancement' in ligne:
        d_create = datetime(2026, 3, 1) + timedelta(days=random.randint(0, 30))
        d_mel = None
    else:
        d_create = datetime(2025, 6, 1) + timedelta(days=random.randint(0, 200))
        ttm = random.randint(5, 15)
        d_mel = d_create + timedelta(days=ttm)

    dates_creation.append(d_create.strftime('%Y-%m-%d'))

    if d_mel is not None and random.random() < 0.12 and statut != 'Pré-lancement':
        d_mel = None

    dates_mel.append(d_mel.strftime('%Y-%m-%d') if d_mel else "")

df['date_creation_fiche'] = dates_creation
df['date_mise_en_ligne'] = dates_mel

# Formats de date incohérents (~8%)
for i in df.index:
    if random.random() < 0.08 and df.at[i, 'date_creation_fiche']:
        d = df.at[i, 'date_creation_fiche']
        parts = d.split('-')
        fmt = random.choice(['slash', 'fr', 'text'])
        if fmt in ('slash', 'fr'):
            df.at[i, 'date_creation_fiche'] = f"{parts[2]}/{parts[1]}/{parts[0]}"
        elif fmt == 'text':
            mois = ['', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            df.at[i, 'date_creation_fiche'] = f"{int(parts[2])} {mois[int(parts[1])]} {parts[0]}"

# Traçabilité fournisseur
fournisseurs = []
origines = []
for i, row in df.iterrows():
    if 'Permanente' in row['ligne'] and random.random() < 0.20:
        fournisseurs.append(random.choice(FOURNISSEURS))
        origines.append(random.choice(ORIGINES_DIRTY))
    elif random.random() < 0.05:
        fournisseurs.append(random.choice(FOURNISSEURS))
        origines.append(random.choice(ORIGINES_DIRTY))
    else:
        fournisseurs.append("")
        origines.append("")

df['fournisseur_cuir'] = fournisseurs
df['origine_matiere'] = origines


# ═══════════════════════════════════════════════════════════════════════
# 6. EXPORT
# ═══════════════════════════════════════════════════════════════════════

col_order = [
    'model_id', 'sku', 'nom_produit', 'categorie', 'sous_categorie',
    'ligne', 'collection_saison', 'statut',
    'couleur', 'matiere_principale', 'matiere_secondaire', 'pays_fabrication',
    'poids', 'dimensions', 'type_fermeture', 'atelier', 'taille',
    'description_FR', 'description_EN', 'description_ZH',
    'prix_retail_EUR', 'prix_ecom_EUR', 'prix_wholesale_EUR',
    'dispo_retail', 'dispo_ecommerce', 'dispo_wholesale',
    'photo_principale', 'nb_visuels', 'composition_matiere',
    'fournisseur_cuir', 'origine_matiere',
    'date_creation_fiche', 'date_mise_en_ligne'
]

df = df.sample(frac=1, random_state=42).reset_index(drop=True)
df = df[col_order]

df.to_excel("dataset_maison_eclat.xlsx", index=False, engine="openpyxl")
df.to_csv("dataset_maison_eclat.csv", index=False)

print(f"\n{'='*60}")
print(f"DATASET FINAL: {len(df)} lignes, {len(df.columns)} colonnes, {df['model_id'].nunique()} modèles")
print(f"Exporté: dataset_maison_eclat.xlsx + .csv")
print(f"{'='*60}")
