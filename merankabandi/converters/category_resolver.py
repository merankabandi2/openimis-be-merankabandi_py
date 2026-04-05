"""
Shared category resolution for KoBo grievance form converters.

Resolves KoBo form values to single plain-string categories matching the
grievance module's configured category names. When multiple categories are
selected, picks the most restrictive one and returns the rest as additional.

Used by both v1 (old form) and v2 (new form) converters.
"""

# Restrictiveness ranking: lower = more restrictive = takes priority
RESTRICTIVENESS = {
    'violence_vbg': 1,
    'corruption': 2,
    'accident_negligence': 3,
    'discrimination_ethnie_religion': 4,
    'maladie_mentale': 5,
    'erreur_exclusion': 6,
    'erreur_inclusion': 7,
    'paiement': 8,
    'telephone': 9,
    'compte': 10,
    'information': 11,
    'uncategorized': 99,
}

VALID_CATEGORIES = set(RESTRICTIVENESS.keys())

# New form (atpoVbHXZCdLD9ETHTv6z4) KoBo slugs → module config categories
NEW_FORM_SLUG_MAP = {
    # Sensitive
    'eas_hs__exploitation__abus_sexuel___harc': 'violence_vbg',
    'pr_l_vements_de_fonds': 'corruption',
    'd_tournement_de_fonds___corruption': 'corruption',
    'conflit_familial': 'violence_vbg',
    'accident_grave_ou_n_gligence_professionn': 'accident_negligence',
    # Special
    'erreur_d_inclusion_potentielle': 'erreur_inclusion',
    'cibl__mais_pas_collect': 'erreur_exclusion',
    'cibl__et_collect': 'erreur_exclusion',
    'migration': 'erreur_exclusion',
    # Non-sensitive
    'probl_me_de_paiement__non_r_ception__mon': 'paiement',
    'carte_sim__bloqu_e__vol_e__perdue__etc': 'telephone',
    'probl_mes_de_t_l_phone__vol__endommag__n': 'telephone',
    'incoh_rence_des_donn_es_personnelles__nu': 'information',
    'probl_mes_de_compte_mobile_money__ecocas': 'compte',
}

# Sub-category values → parent category
SUB_CATEGORY_MAP = {
    # violence_vbg sub-categories
    'viol': 'violence_vbg',
    'mariage_force_precoce': 'violence_vbg',
    'violence_abus': 'violence_vbg',
    'sante_maternelle': 'violence_vbg',
    # erreur_exclusion sub-categories
    'demande_insertion': 'erreur_exclusion',
    'probleme_identification': 'erreur_exclusion',
    # paiement sub-categories
    'paiement_pas_recu': 'paiement',
    'paiement_en_retard': 'paiement',
    'paiement_incomplet': 'paiement',
    'vole': 'paiement',
    # telephone sub-categories
    'perdu': 'telephone',
    'pas_de_reseau': 'telephone',
    'allume_pas_batterie': 'telephone',
    'recoit_pas_tm': 'telephone',
    'mot_de_passe_oublie': 'telephone',
    # compte sub-categories
    'non_active': 'compte',
    'bloque': 'compte',
}


def resolve_value(val):
    """Resolve a single category value string to a valid module config category."""
    if not val:
        return None
    val = val.strip()
    if val in VALID_CATEGORIES:
        return val
    if val in NEW_FORM_SLUG_MAP:
        return NEW_FORM_SLUG_MAP[val]
    if val in SUB_CATEGORY_MAP:
        return SUB_CATEGORY_MAP[val]
    if val in ('autre', 'autre_'):
        return 'uncategorized'
    return None


def pick_most_restrictive(categories):
    """Given a list of resolved category strings, return the most restrictive one."""
    valid = [c for c in categories if c and c in RESTRICTIVENESS]
    if not valid:
        return 'uncategorized'
    return min(valid, key=lambda c: RESTRICTIVENESS[c])


def resolve_categories(raw_values):
    """
    Resolve a list of raw KoBo category values to (main_category, additional_categories, sub_category).

    Args:
        raw_values: list of raw string values from KoBo form fields

    Returns:
        (main_category, additional_categories, sub_category)
        - main_category: single string matching module config (most restrictive)
        - additional_categories: list of other resolved categories, or None
        - sub_category: most specific sub-category value for json_ext, or None
    """
    resolved = []
    sub_category = None

    for val in raw_values:
        if not val:
            continue
        # KoBo multi-select: space-separated values in a single string
        for part in str(val).split():
            mapped = resolve_value(part)
            if mapped and mapped not in resolved:
                resolved.append(mapped)
            # Track sub-category for detail
            if part in SUB_CATEGORY_MAP:
                sub_category = part

    if not resolved:
        return 'uncategorized', None, None

    main = pick_most_restrictive(resolved)
    others = [c for c in resolved if c != main]
    return main, others if others else None, sub_category


def derive_flags_from_category(main_category):
    """Derive grievance flags from the resolved main category."""
    if main_category in ('violence_vbg', 'corruption', 'accident_negligence',
                         'discrimination_ethnie_religion', 'maladie_mentale'):
        return 'SENSITIVE'
    if main_category in ('erreur_exclusion', 'erreur_inclusion'):
        return 'SPECIAL'
    return None
