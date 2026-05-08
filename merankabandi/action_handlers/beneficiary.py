from merankabandi.action_handlers.base import BaseActionHandler


class BeneficiaryDeactivateHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['confirmation', 'deactivation_reason']

    def validate(self, task, ticket):
        workflow = task.workflow
        verify_task = workflow.tasks.filter(
            step_template__action_type='verify_social_id', status='COMPLETED',
        ).first()
        if not verify_task or not (verify_task.result or {}).get('found'):
            raise ValueError('Cannot deactivate: no verified individual found')

    def execute(self, task, ticket, user, data=None):
        from social_protection.models import GroupBeneficiary

        data = data or {}
        workflow = task.workflow
        verify_task = workflow.tasks.filter(
            step_template__action_type='verify_social_id', status='COMPLETED',
        ).first()
        individual_id = (verify_task.result or {}).get('individual_id') if verify_task else None
        if not individual_id:
            return {'error': 'No individual to deactivate'}

        beneficiaries = GroupBeneficiary.objects.filter(
            group__groupindividuals__individual_id=individual_id,
            is_deleted=False,
        ).exclude(status='SUSPENDED')
        updated = 0
        for gb in beneficiaries:
            gb.status = 'SUSPENDED'
            gb.save(username=user.username)
            updated += 1

        return {
            'individual_id': individual_id,
            'deactivation_reason': data.get('deactivation_reason', ''),
            'status': 'deactivated',
            'beneficiaries_suspended': updated,
        }


class BeneficiaryReplaceHandler(BaseActionHandler):
    def get_required_fields(self):
        return ['confirmation']

    def execute(self, task, ticket, user, data=None):
        from merankabandi.workflow_models import ReplacementRequest
        replacement = ReplacementRequest.objects.filter(
            ticket=ticket, status=ReplacementRequest.STATUS_APPROVED,
        ).first()
        if not replacement:
            # Auto-approve pending replacement if the deactivate step already completed
            pending = ReplacementRequest.objects.filter(
                ticket=ticket, status=ReplacementRequest.STATUS_PENDING,
            ).first()
            if pending:
                pending.status = ReplacementRequest.STATUS_APPROVED
                pending.save()
                replacement = pending
        if not replacement:
            return {'error': 'No replacement request found'}

        # If create_replacement_request matched an existing household member,
        # link to them instead of creating a duplicate. The match was already
        # written to replacement.new_individual_id by the previous step.
        matched_existing = (replacement.json_ext or {}).get('matched_existing')
        if replacement.new_individual_id:
            return {
                'replacement_id': str(replacement.id),
                'new_individual_id': str(replacement.new_individual_id),
                'new_nom': replacement.new_nom,
                'new_prenom': replacement.new_prenom,
                'matched_existing': matched_existing,
                'status': 'matched_existing_member',
            }

        return {
            'replacement_id': str(replacement.id),
            'new_nom': replacement.new_nom, 'new_prenom': replacement.new_prenom,
            'status': 'new_individual_creation_pending',
        }


class CreateReplacementRequestHandler(BaseActionHandler):
    def is_automated(self):
        return True

    def execute(self, task, ticket, user, data=None):
        from merankabandi.workflow_models import ReplacementRequest
        json_ext = ticket.json_ext or {}
        replacement_data = json_ext.get('replacement') or {}
        new_recipient = replacement_data.get('new_recipient') or {}
        if not replacement_data.get('replaced_social_id'):
            return {'error': 'No replacement data in ticket'}
        existing = ReplacementRequest.objects.filter(ticket=ticket).first()
        if existing:
            return {'replacement_id': str(existing.id), 'status': existing.status}

        # Dedup: try to match the new recipient against existing household members
        # (handles name-order swaps "Minani Jean" vs "Jean Minani" and minor typos).
        match_info = _match_existing_household_member(
            replaced_social_id=replacement_data.get('replaced_social_id', ''),
            new_recipient=new_recipient,
        )

        rr_json_ext = {'attachments': replacement_data.get('attachments')}
        if match_info:
            rr_json_ext['matched_existing'] = match_info
        rr = ReplacementRequest.objects.create(
            ticket=ticket, task=task,
            replaced_social_id=replacement_data.get('replaced_social_id', ''),
            motif=replacement_data.get('motif', ''),
            relationship=replacement_data.get('relationship', ''),
            new_nom=new_recipient.get('nom', ''),
            new_prenom=new_recipient.get('prenom', ''),
            new_date_naissance=new_recipient.get('date_naissance'),
            new_sexe=new_recipient.get('sexe', ''),
            new_telephone=new_recipient.get('telephone'),
            new_cni=new_recipient.get('cni', ''),
            new_individual_id=match_info['individual_id'] if match_info else None,
            json_ext=rr_json_ext,
        )
        result = {'replacement_id': str(rr.id), 'status': rr.status}
        if match_info:
            result['matched_existing'] = match_info
        return result


def _normalize_name(s):
    """Lowercase, strip diacritics, collapse whitespace for fuzzy matching."""
    import unicodedata, re
    if not s:
        return ''
    nfkd = unicodedata.normalize('NFKD', str(s))
    no_diacritics = ''.join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r'\s+', ' ', no_diacritics.lower().strip())


def _match_existing_household_member(replaced_social_id, new_recipient):
    """Look up the household and try to match new_recipient against existing members.

    Returns {individual_id, first_name, last_name, score, match_type} or None.
    Uses normalized name comparison with fuzzy similarity to handle typos.
    Tries both name orders (first/last and last/first) since KoBo collectors
    sometimes swap them.
    """
    from individual.models import Group, GroupIndividual
    from difflib import SequenceMatcher

    if not replaced_social_id:
        return None

    group = (
        Group.objects.filter(code=replaced_social_id, is_deleted=False).first()
        or Group.objects.filter(json_ext__contains={'social_id': replaced_social_id}, is_deleted=False).first()
    )
    if not group:
        return None

    new_first = _normalize_name(new_recipient.get('prenom', ''))
    new_last = _normalize_name(new_recipient.get('nom', ''))
    if not new_first and not new_last:
        return None

    new_combined = f'{new_first} {new_last}'.strip()
    new_swapped = f'{new_last} {new_first}'.strip()

    best = None  # (score, match_type, gi)
    threshold = 0.85
    for gi in GroupIndividual.objects.filter(group=group, is_deleted=False).select_related('individual'):
        ind = gi.individual
        if not ind:
            continue
        e_first = _normalize_name(ind.first_name)
        e_last = _normalize_name(ind.last_name)
        e_combined = f'{e_first} {e_last}'.strip()

        scores = [
            (SequenceMatcher(None, new_combined, e_combined).ratio(), 'exact_order'),
            (SequenceMatcher(None, new_swapped, e_combined).ratio(), 'swapped_order'),
        ]
        score, match_type = max(scores)
        if score >= threshold and (best is None or score > best[0]):
            best = (score, match_type, gi)

    if not best:
        return None
    score, match_type, gi = best
    return {
        'individual_id': str(gi.individual.id),
        'first_name': gi.individual.first_name,
        'last_name': gi.individual.last_name,
        'role': gi.role,
        'score': round(score, 3),
        'match_type': match_type,
    }
