import datetime

from django.db import transaction, connection
from django.db.models import Max


def generate_social_id(precollecte_instance):
    """
    Generate social ID in legacy format: YYPPRRNNNNNNNN
    YY = last 2 digits of year
    PP = last 2 chars of province code
    RR = targeting round (zero-padded)
    NNNNNNNN = sequential counter (zero-padded)

    Must be called inside a transaction with select_for_update.
    Legacy reference: kobo_phone_pmt_15.php5 lines 506-535.
    """
    from merankabandi.models import PreCollecte

    year = datetime.date.today().year
    yy = str(year)[-2:]

    # Walk up the location hierarchy until we reach the Province (type='D').
    # Hierarchy in Burundi is: Country 'R' (code=BUR) → Province 'D' → Commune 'W' → Colline 'V'.
    # Previous code walked all the way to the country root, producing 'UR' from 'BUR'
    # for every social_id. The spec (and legacy kobo_phone_pmt_15.php5) expects the
    # province D-level code (numeric, e.g. '04' for Ruyigi). Stop the walk at type='D'.
    node = precollecte_instance.location
    while node and node.type != 'D':
        node = node.parent
    province_code = node.code[-2:] if node and node.code else "00"

    rr = str(precollecte_instance.targeting_round).zfill(2)
    prefix = f"{yy}{province_code}{rr}"

    with transaction.atomic():
        # Serialize per-prefix sequence allocation. `select_for_update().aggregate()`
        # silently DROPS the row lock (Django runs the aggregate as a subquery), and
        # on the first record for a prefix there are no rows to lock anyway — so two
        # concurrent creates would read the same max and mint a DUPLICATE social_id.
        # A transaction-scoped advisory lock keyed on the prefix serializes them; it
        # is held until the enclosing transaction commits (after the save below).
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_advisory_xact_lock(hashtext(%s))",
                           [f"precollecte_social_id:{prefix}"])

        max_seq = (
            PreCollecte.objects
            .filter(social_id__startswith=prefix)
            .aggregate(max_seq=Max('social_id_seq'))
        )['max_seq'] or 0

        new_seq = max_seq + 1
        social_id = f"{prefix}{str(new_seq).zfill(8)}"

        precollecte_instance.social_id_seq = new_seq
        precollecte_instance.social_id = social_id
        # Save INSIDE the lock so the new row is committed before another waiter
        # reads max_seq. (Callers must not save again.)
        precollecte_instance.save()

    return social_id
