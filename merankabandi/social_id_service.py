import datetime

from django.db import transaction
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

    # Get province code from location hierarchy (colline -> commune -> province)
    location = precollecte_instance.location
    province = location
    while province and province.parent:
        province = province.parent
    province_code = province.code[-2:] if province else "00"

    rr = str(precollecte_instance.targeting_round).zfill(2)
    prefix = f"{yy}{province_code}{rr}"

    with transaction.atomic():
        max_seq = (
            PreCollecte.objects
            .select_for_update()
            .filter(social_id__startswith=prefix)
            .aggregate(max_seq=Max('social_id_seq'))
        )['max_seq'] or 0

        new_seq = max_seq + 1
        social_id = f"{prefix}{str(new_seq).zfill(8)}"

        precollecte_instance.social_id_seq = new_seq
        precollecte_instance.social_id = social_id

    return social_id
