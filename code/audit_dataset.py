"""
Maison Éclat — Script d'audit qualité données produit
Mission Adone Conseil — Phase 1 : Diagnostic
Analyse le dataset et génère les 5 graphiques du Livrable 2.

Usage: python3 audit_dataset.py
Prérequis: dataset_maison_eclat.xlsx dans le même répertoire
Sortie: graph_01 à graph_05 (.png) + rapport console
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# ─── Style graphiques ───
plt.rcParams.update({
    'font.family': 'sans-serif', 'font.size': 10,
    'axes.spines.top': False, 'axes.spines.right': False,
    'figure.facecolor': 'white', 'axes.facecolor': 'white',
    'axes.grid': True, 'grid.alpha': 0.3, 'grid.linewidth': 0.5,
})

NAVY = '#1B2A4A'; ACCENT = '#2E5A88'; RED = '#C0392B'
ORANGE = '#E67E22'; GREEN = '#27AE60'; GREY = '#95A5A6'; LIGHT = '#ECF0F1'


# ─── Chargement ───
df = pd.read_excel('dataset_maison_eclat.xlsx')
n = len(df)


# ─── Helpers ───
def is_empty(s):
    return s.isna() | (s.astype(str).str.strip() == '') | (s.astype(str) == 'nan')

def clean_cat(v):
    v = str(v).strip().lower()
    if 'sac' in v: return 'Sacs'
    elif 'pm' in v or 'petite' in v or 'maroq' in v: return 'Petite Maroquinerie'
    elif 'ceint' in v: return 'Ceintures'
    else: return 'Bijoux & Accessoires'

df['cat_clean'] = df['categorie'].apply(clean_cat)


# ═══════════════════════════════════════════════════════════════════════
# FINDING 1 — COMPLÉTUDE
# ═══════════════════════════════════════════════════════════════════════
print("=" * 70)
print(f"AUDIT QUALITÉ DONNÉES PRODUIT — MAISON ÉCLAT")
print(f"Périmètre : {n} SKUs, {df['model_id'].nunique()} modèles")
print("=" * 70)

print("\n── FINDING 1 : COMPLÉTUDE DES FICHES PRODUIT ──")

completude_data = {}
champs = [
    ('description_FR', 'Description FR', 'CRITIQUE'),
    ('description_EN', 'Description EN', 'CRITIQUE'),
    ('photo_principale', 'Photo principale', 'CRITIQUE'),
    ('description_ZH', 'Description ZH', 'IMPORTANT'),
    ('nb_visuels', 'Visuels ≥3', 'IMPORTANT'),
    ('matiere_secondaire', 'Matière secondaire', 'IMPORTANT'),
    ('type_fermeture', 'Type fermeture', 'IMPORTANT'),
    ('composition_matiere', 'Composition matière', 'DPP'),
    ('fournisseur_cuir', 'Fournisseur cuir', 'DPP'),
    ('origine_matiere', 'Origine matière', 'DPP'),
]

for col, label, cat in champs:
    if col == 'photo_principale':
        filled = (df[col] == True).sum()
    elif col == 'nb_visuels':
        filled = (df[col] >= 3).sum()
    else:
        filled = (~is_empty(df[col])).sum()
    pct = filled / n * 100
    completude_data[label] = pct
    status = "✓" if pct > 90 else ("⚠" if pct > 60 else "✗")
    print(f"  {status} {label} ({cat}): {filled}/{n} = {pct:.1f}%")

# Score: % de fiches avec les 3 critiques OK
all_crit_ok = (~is_empty(df['description_FR'])) & (~is_empty(df['description_EN'])) & (df['photo_principale'] == True)
score_completude = round(all_crit_ok.sum() / n * 100)
has_issue = ~all_crit_ok | (df['nb_visuels'] < 3)
print(f"\n  Fiches avec ≥1 critique manquant : {has_issue.sum()} ({has_issue.sum()/n*100:.1f}%)")
print(f"  Score complétude vente : {score_completude}/100")


# ═══════════════════════════════════════════════════════════════════════
# FINDING 2 — PRIX
# ═══════════════════════════════════════════════════════════════════════
print("\n── FINDING 2 : INCOHÉRENCES PRIX CROSS-CANAL ──")

ecart_ecom = df[df['prix_ecom_EUR'] != df['prix_retail_EUR']]
print(f"  Prix ecom ≠ retail : {len(ecart_ecom)} SKUs ({len(ecart_ecom)/n*100:.1f}%)")

if len(ecart_ecom) > 0:
    ecart_pct = ((ecart_ecom['prix_ecom_EUR'] - ecart_ecom['prix_retail_EUR']) / ecart_ecom['prix_retail_EUR'] * 100).abs()
    print(f"  Écart moyen : {ecart_pct.mean():.1f}%, max : {ecart_pct.max():.1f}%")

ws_anormal = df[(df['prix_wholesale_EUR'] / df['prix_retail_EUR'] > 0.55) & (df['prix_retail_EUR'] > 0)]
print(f"  Wholesale > 55% retail : {len(ws_anormal)} SKUs")

prix_zero = df[df['prix_retail_EUR'] <= 0]
prix_extreme = df[df['prix_retail_EUR'] > 5000]
print(f"  Prix aberrants : {len(prix_zero)} ≤0€, {len(prix_extreme)} >5000€")

prix_ok = (df['prix_ecom_EUR'] == df['prix_retail_EUR']) & (df['prix_retail_EUR'] > 0)
score_prix = round(prix_ok.sum() / n * 100)
print(f"  Score cohérence prix : {score_prix}/100")


# ═══════════════════════════════════════════════════════════════════════
# FINDING 3 — TTM
# ═══════════════════════════════════════════════════════════════════════
print("\n── FINDING 3 : TIME-TO-MARKET ──")

df_d = df[
    df['date_creation_fiche'].astype(str).str.match(r'^\d{4}-') &
    df['date_mise_en_ligne'].astype(str).str.match(r'^\d{4}-')
].copy()
df_d['ttm'] = (pd.to_datetime(df_d['date_mise_en_ligne']) - pd.to_datetime(df_d['date_creation_fiche'])).dt.days
df_d = df_d[(df_d['ttm'] > 0) & (df_d['ttm'] < 60)]

ttm_perm = df_d[df_d['ligne'].str.contains('Permanente')]['ttm']
ttm_caps = df_d[df_d['ligne'].str.contains('Capsule')]['ttm']
ttm_collab = df_d[df_d['ligne'].str.contains('Collab')]['ttm']

print(f"  Fiches analysées : {len(df_d)}/{n}")
print(f"  TTM Permanentes : {ttm_perm.mean():.0f}j (médiane {ttm_perm.median():.0f}j)")
print(f"  TTM Capsules : {ttm_caps.mean():.0f}j (médiane {ttm_caps.median():.0f}j)")
print(f"  TTM Collab : {ttm_collab.mean():.0f}j")
print(f"  Ratio capsule/permanente : ×{ttm_caps.mean()/ttm_perm.mean():.1f}")

jamais = (is_empty(df['date_mise_en_ligne']) & (df['statut'] != 'Pré-lancement')).sum()
print(f"  Jamais mises en ligne (hors pré-lancement) : {jamais} ({jamais/n*100:.1f}%)")

non_iso = ~df['date_creation_fiche'].astype(str).str.match(r'^\d{4}-')
print(f"  Dates format non-standard : {non_iso.sum()} ({non_iso.sum()/n*100:.1f}%)")


# ═══════════════════════════════════════════════════════════════════════
# FINDING 4 — DPP
# ═══════════════════════════════════════════════════════════════════════
print("\n── FINDING 4 : PRÉPARATION DPP ──")

comp_ok = ~is_empty(df['composition_matiere'])
fourn_ok = ~is_empty(df['fournisseur_cuir'])
orig_ok = ~is_empty(df['origine_matiere'])
dpp_ready = comp_ok & fourn_ok & orig_ok

print(f"  Composition matière : {comp_ok.sum()} ({comp_ok.sum()/n*100:.1f}%)")
print(f"  Fournisseur cuir : {fourn_ok.sum()} ({fourn_ok.sum()/n*100:.1f}%)")
print(f"  Origine matière : {orig_ok.sum()} ({orig_ok.sum()/n*100:.1f}%)")
print(f"  DPP-ready (3 champs) : {dpp_ready.sum()} ({dpp_ready.sum()/n*100:.1f}%)")

score_dpp = round(dpp_ready.sum() / n * 100)
print(f"  Score préparation DPP : {score_dpp}/100")


# ═══════════════════════════════════════════════════════════════════════
# FINDING 5 — RÉFÉRENTIEL
# ═══════════════════════════════════════════════════════════════════════
print("\n── FINDING 5 : DOUBLONS & COHÉRENCE RÉFÉRENTIEL ──")

dups = df[df['sku'].str.contains('DUP', na=False)]
print(f"  Doublons : {len(dups)}")

cat_propres = ['Sacs', 'Petite Maroquinerie', 'Ceintures', 'Bijoux & Accessoires']
cat_sale = (~df['categorie'].isin(cat_propres)).sum()
print(f"  Catégories non-standard : {cat_sale} ({cat_sale/n*100:.1f}%)")
print(f"  Valeurs distinctes : {df['categorie'].nunique()} pour 4 catégories")

score_uniq = round((n - len(dups)) / n * 100)
score_norm = round(df['categorie'].isin(cat_propres).sum() / n * 100)
print(f"  Score unicité : {score_uniq}/100")
print(f"  Score normalisation : {score_norm}/100")


# ═══════════════════════════════════════════════════════════════════════
# SYNTHÈSE
# ═══════════════════════════════════════════════════════════════════════
scores = [score_completude, score_prix, score_dpp, score_uniq, score_norm]
score_global = round(np.mean(scores))

print(f"\n{'='*70}")
print(f"SCORE QUALITÉ GLOBAL : {score_global}/100")
print(f"  Complétude vente : {scores[0]}/100")
print(f"  Cohérence prix   : {scores[1]}/100")
print(f"  Préparation DPP  : {scores[2]}/100")
print(f"  Unicité          : {scores[3]}/100")
print(f"  Normalisation    : {scores[4]}/100")
print(f"{'='*70}")


# ═══════════════════════════════════════════════════════════════════════
# GRAPHIQUES
# ═══════════════════════════════════════════════════════════════════════
print("\nGénération des graphiques...")

# ─── GRAPH 1 : Complétude par champ ───
fig, ax = plt.subplots(figsize=(10, 6))
labels = list(completude_data.keys())
values = list(completude_data.values())
colors = [GREEN if v > 90 else ORANGE if v > 60 else RED for v in values]
bars = ax.barh(range(len(labels)), values, color=colors, height=0.6, edgecolor='white', linewidth=0.5)
ax.set_yticks(range(len(labels))); ax.set_yticklabels(labels, fontsize=9)
ax.set_xlim(0, 108); ax.set_xlabel('Taux de complétude (%)')
ax.set_title('Complétude des champs produit', fontsize=13, fontweight='bold', color=NAVY, pad=15)
ax.axvline(x=95, color=NAVY, linestyle='--', linewidth=1, alpha=0.5, label='Cible 95%')
ax.legend(loc='lower right', fontsize=8)
for bar, val in zip(bars, values):
    ax.text(val + 1, bar.get_y() + bar.get_height()/2, f'{val:.0f}%', va='center', fontsize=9, fontweight='bold')
plt.tight_layout(); plt.savefig('graph_01_completude.png', dpi=150, bbox_inches='tight'); plt.close()

# ─── GRAPH 2 : TTM par ligne ───
fig, ax = plt.subplots(figsize=(10, 5))
ttm_data, ttm_labels = [], []
for l in ['Permanente Sellier', 'Permanente Classique', 'Capsule AH25', 'Capsule PE26', 'Capsule Collab Artiste PE26']:
    sub = df_d[df_d['ligne'] == l]['ttm']
    if len(sub) > 5:
        ttm_data.append(sub.values)
        ttm_labels.append(l.replace('Permanente ', 'Perm.\n').replace('Capsule ', 'Cap.\n').replace('Artiste ', 'Art.\n'))
bp = ax.boxplot(ttm_data, labels=ttm_labels, patch_artist=True, widths=0.5,
    medianprops=dict(color=NAVY, linewidth=2), flierprops=dict(marker='o', markersize=4, alpha=0.5))
for patch, c in zip(bp['boxes'], [GREEN, GREEN, ORANGE, RED, RED]):
    patch.set_facecolor(c); patch.set_alpha(0.3)
ax.axhline(y=5, color=GREEN, linestyle='--', linewidth=1.5, alpha=0.7, label='Cible PIM : 5 jours')
ax.set_ylabel('Jours'); ax.set_title('Time-to-Market par ligne', fontsize=13, fontweight='bold', color=NAVY, pad=15)
ax.legend(loc='upper left', fontsize=9)
plt.tight_layout(); plt.savefig('graph_02_ttm.png', dpi=150, bbox_inches='tight'); plt.close()

# ─── GRAPH 3 : DPP ───
fig, ax = plt.subplots(figsize=(8, 5))
dpp_l = ['Composition\nmatière', 'Fournisseur\ncuir', 'Origine\nmatière', 'Les 3 champs\n(DPP-ready)']
dpp_f = [comp_ok.sum()/n*100, fourn_ok.sum()/n*100, orig_ok.sum()/n*100, dpp_ready.sum()/n*100]
dpp_e = [100-v for v in dpp_f]
ax.bar(range(4), dpp_f, color=ACCENT, label='Renseigné', width=0.5)
ax.bar(range(4), dpp_e, bottom=dpp_f, color=LIGHT, edgecolor=GREY, linewidth=0.5, label='Manquant', width=0.5)
for i, v in enumerate(dpp_f):
    ax.text(i, v/2, f'{v:.0f}%', ha='center', va='center', fontsize=11, fontweight='bold', color='white' if v > 15 else NAVY)
ax.set_xticks(range(4)); ax.set_xticklabels(dpp_l, fontsize=9)
ax.set_ylabel('% du catalogue'); ax.set_title('Préparation Digital Product Passport', fontsize=13, fontweight='bold', color=NAVY, pad=15)
ax.legend(loc='upper right', fontsize=9); ax.set_ylim(0, 110)
plt.tight_layout(); plt.savefig('graph_03_dpp.png', dpi=150, bbox_inches='tight'); plt.close()

# ─── GRAPH 4 : Heatmap ───
cats = ['Sacs', 'Petite Maroquinerie', 'Ceintures', 'Bijoux & Accessoires']
lignes = ['Permanente Sellier', 'Permanente Classique', 'Capsule AH25', 'Capsule PE26', 'Capsule Collab Artiste PE26', 'Pré-lancement AH26']
hm = np.full((len(lignes), len(cats)), np.nan)
for i, l in enumerate(lignes):
    for j, c in enumerate(cats):
        sub = df[(df['cat_clean'] == c) & (df['ligne'] == l)]
        if len(sub) > 0:
            s = ((~is_empty(sub['description_FR'])).sum() + (~is_empty(sub['description_EN'])).sum() + (sub['photo_principale'] == True).sum()) / (len(sub) * 3) * 100
            hm[i, j] = s

fig, ax = plt.subplots(figsize=(10, 5))
im = ax.imshow(hm, cmap='RdYlGn', aspect='auto', vmin=30, vmax=100)
ax.set_xticks(range(len(cats))); ax.set_xticklabels(cats, fontsize=9)
ax.set_yticks(range(len(lignes))); ax.set_yticklabels([l.replace('Permanente ', 'Perm. ').replace('Capsule ', 'Cap. ').replace('Artiste ', 'Art. ') for l in lignes], fontsize=9)
for i in range(len(lignes)):
    for j in range(len(cats)):
        v = hm[i, j]
        if not np.isnan(v):
            ax.text(j, i, f'{v:.0f}%', ha='center', va='center', fontsize=10, fontweight='bold', color='white' if v < 60 else NAVY)
        else:
            ax.text(j, i, '—', ha='center', va='center', fontsize=10, color=GREY)
ax.set_title('Score complétude par ligne × catégorie', fontsize=13, fontweight='bold', color=NAVY, pad=15)
plt.colorbar(im, ax=ax, shrink=0.8, label='% complétude')
plt.tight_layout(); plt.savefig('graph_04_heatmap.png', dpi=150, bbox_inches='tight'); plt.close()

# ─── GRAPH 5 : Radar ───
fig, ax = plt.subplots(figsize=(7, 7), subplot_kw=dict(projection='polar'))
cat_r = ['Complétude\nvente', 'Cohérence\nprix', 'Préparation\nDPP', 'Unicité\nréférentiel', 'Normalisation\ndonnées']
angles = np.linspace(0, 2 * np.pi, len(cat_r), endpoint=False).tolist()
sp = scores + [scores[0]]; ap = angles + [angles[0]]
ax.plot(ap, sp, 'o-', linewidth=2, color=ACCENT, markersize=6)
ax.fill(ap, sp, alpha=0.15, color=ACCENT)
ax.plot(ap, [95] * (len(cat_r) + 1), '--', linewidth=1, color=GREEN, alpha=0.5, label='Cible 95')
ax.set_xticks(angles); ax.set_xticklabels(cat_r, fontsize=9); ax.set_ylim(0, 100)
ax.set_title(f'Score qualité global : {score_global}/100', fontsize=14, fontweight='bold', color=NAVY, pad=30)
ax.legend(loc='lower right', fontsize=8)
plt.tight_layout(); plt.savefig('graph_05_radar.png', dpi=150, bbox_inches='tight'); plt.close()

print("5 graphiques générés ✓")
