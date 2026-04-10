from decimal import Decimal
from django.db.models import Sum
from django.utils import timezone

from social_protection.models import GroupBeneficiary
from individual.models import Group
from payroll.models import BenefitConsumption
from .models import (
    Section, Indicator, IndicatorAchievement,
    ResultFrameworkSnapshot, IndicatorCalculationRule,
    MonetaryTransfer, SensitizationTraining,
    MicroProject, HOST_COMMUNES
)

# Refugee collines/camps for refugee/host community separation
REFUGEE_COLLINES = []

DEMOGRAPHIC_BREAKDOWNS = [
    {'key': 'women', 'label': 'Femmes'},
    {'key': 'twa', 'label': 'Ménages Twa'},
    {'key': 'disabled', 'label': 'Ménages avec handicap'},
    {'key': 'chronic_illness', 'label': 'Maladies chroniques'},
    {'key': 'refugees', 'label': 'Ménages réfugiés'},
    {'key': 'returnees', 'label': 'Retournés/rapatriés'},
    {'key': 'displaced', 'label': 'Déplacés'},
]


class ResultFrameworkService:
    """Service for result framework calculations and document generation"""

    def __init__(self):
        self.calculation_methods = {
            # Development indicators (sections 1-3)
            'count_households_registered': self._count_households_registered,
            'count_households_refugees': self._count_households_refugees,
            'count_households_host': self._count_households_host,
            'count_beneficiaries_social_protection': self._count_beneficiaries_social_protection,
            'count_beneficiaries_women': self._count_beneficiaries_women,
            'count_beneficiaries_unconditional_transfers': self._count_beneficiaries_unconditional_transfers,
            'count_beneficiaries_emergency_transfers': self._count_beneficiaries_emergency_transfers,
            'count_beneficiaries_refugees': self._count_beneficiaries_refugees,
            'count_beneficiaries_host_communities': self._count_beneficiaries_host_communities,
            'count_beneficiaries_employment': self._count_beneficiaries_employment,
            'count_beneficiaries_employment_women': self._count_beneficiaries_employment_women,
            'count_beneficiaries_employment_refugees': self._count_beneficiaries_employment_refugees,
            'count_beneficiaries_employment_host': self._count_beneficiaries_employment_host,
            'count_farmers_received_services': self._count_farmers_received_services,

            # Intermediate indicators (sections 4-8)
            'count_provinces_with_transfers': self._count_provinces_with_transfers,
            'calculate_payment_timeliness': self._calculate_payment_timeliness,
            'calculate_behavior_change_participation': self._calculate_behavior_change_participation,
            'count_approved_business_plans': self._count_approved_business_plans,
            'count_approved_business_plans_women': self._count_approved_business_plans_women,
            'count_approved_business_plans_batwa': self._count_approved_business_plans_batwa,
            'count_climate_resilient_activities': self._count_climate_resilient_activities,
            'calculate_digital_payment_percentage': self._calculate_digital_payment_percentage,
        }

    def _compute_breakdowns(self, benefit_plan_codes=None, location=None):
        """Compute standard demographic breakdowns from the vulnerable groups materialized view."""
        from django.db import connection

        conditions = []
        params = []

        if benefit_plan_codes:
            placeholders = ', '.join(['%s'] * len(benefit_plan_codes))
            conditions.append(f"benefit_plan_code IN ({placeholders})")
            params.extend(benefit_plan_codes)

        if location:
            conditions.append("province_id = %s")
            params.append(location.id if hasattr(location, 'id') else location)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        sql = f"""
            SELECT
                COALESCE(SUM(twa_households), 0),
                COALESCE(SUM(disabled_households), 0),
                COALESCE(SUM(chronic_illness_households), 0),
                COALESCE(SUM(refugee_households), 0),
                COALESCE(SUM(returnee_households), 0),
                COALESCE(SUM(displaced_households), 0)
            FROM dashboard_vulnerable_groups_summary
            {where_clause}
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
        except Exception:
            row = (0, 0, 0, 0, 0, 0)

        women_count = self._count_women_beneficiaries(benefit_plan_codes, location)

        view_keys = ['twa', 'disabled', 'chronic_illness', 'refugees', 'returnees', 'displaced']
        values = {'women': women_count}
        for i, key in enumerate(view_keys):
            values[key] = row[i] if row else 0

        return [
            {**bd, 'value': values.get(bd['key'], 0)}
            for bd in DEMOGRAPHIC_BREAKDOWNS
        ]

    def _count_women_beneficiaries(self, benefit_plan_codes=None, location=None):
        """Count female primary recipients for the given benefit plans."""
        query = GroupBeneficiary.objects.filter(
            is_deleted=False,
            status__in=['ACTIVE', 'VALIDATED', 'POTENTIAL'],
            group__groupindividuals__individual__json_ext__sexe='F',
            group__groupindividuals__recipient_type='PRIMARY',
        )
        if benefit_plan_codes:
            query = query.filter(benefit_plan__code__in=benefit_plan_codes)
        if location:
            query = query.filter(group__location__parent__parent=location)
        return query.distinct().count()

    def _count_snapshot_beneficiaries(self, table_name, date_from=None, date_to=None,
                                       location=None, extra_where='', extra_params=None):
        """Aggregate snapshot entities using latest-per-colline logic.

        For snapshot entities (BehaviorChangePromotion, MicroProject), the correct
        count is the sum of the latest report per colline within the period.
        """
        from django.db import connection

        conditions = ["validation_status = 'VALIDATED'"]
        params = []

        if date_from:
            conditions.append("report_date >= %s")
            params.append(date_from)
        if date_to:
            conditions.append("report_date <= %s")
            params.append(date_to)
        if location:
            conditions.append("""
                location_id IN (
                    SELECT l1."LocationId" FROM "tblLocations" l1
                    JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
                    WHERE l2."ParentLocationId" = %s
                )
            """)
            params.append(location.id if hasattr(location, 'id') else location)

        if extra_where:
            conditions.append(extra_where)
        if extra_params:
            params.extend(extra_params)

        where = ' AND '.join(conditions)

        sql = f"""
            WITH latest AS (
                SELECT DISTINCT ON (location_id)
                    location_id, male_participants, female_participants, twa_participants
                FROM {table_name}
                WHERE {where}
                ORDER BY location_id, report_date DESC
            )
            SELECT
                COALESCE(SUM(male_participants + female_participants), 0),
                COALESCE(SUM(male_participants), 0),
                COALESCE(SUM(female_participants), 0),
                COALESCE(SUM(twa_participants), 0)
            FROM latest
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
            return {'total': row[0], 'male': row[1], 'female': row[2], 'twa': row[3]}
        except Exception as e:
            return {'total': 0, 'male': 0, 'female': 0, 'twa': 0, 'error': str(e)}

    def _count_snapshot_beneficiaries(self, table_name, date_from=None, date_to=None,
                                       location=None, extra_where='', extra_params=None):
        """Aggregate snapshot entities using latest-per-colline logic.

        For snapshot entities (BehaviorChangePromotion, MicroProject), the correct
        count is the sum of the latest report per colline within the period.
        """
        from django.db import connection

        conditions = ["validation_status = 'VALIDATED'"]
        params = []

        if date_from:
            conditions.append("report_date >= %s")
            params.append(date_from)
        if date_to:
            conditions.append("report_date <= %s")
            params.append(date_to)
        if location:
            conditions.append("""
                location_id IN (
                    SELECT l1."LocationId" FROM "tblLocations" l1
                    JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
                    WHERE l2."ParentLocationId" = %s
                )
            """)
            params.append(location.id if hasattr(location, 'id') else location)

        if extra_where:
            conditions.append(extra_where)
        if extra_params:
            params.extend(extra_params)

        where = ' AND '.join(conditions)

        sql = f"""
            WITH latest AS (
                SELECT DISTINCT ON (location_id)
                    location_id, male_participants, female_participants, twa_participants
                FROM {table_name}
                WHERE {where}
                ORDER BY location_id, report_date DESC
            )
            SELECT
                COALESCE(SUM(male_participants + female_participants), 0),
                COALESCE(SUM(male_participants), 0),
                COALESCE(SUM(female_participants), 0),
                COALESCE(SUM(twa_participants), 0)
            FROM latest
        """

        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone()
            return {'total': row[0], 'male': row[1], 'female': row[2], 'twa': row[3]}
        except Exception as e:
            return {'total': 0, 'male': 0, 'female': 0, 'twa': 0, 'error': str(e)}

    def calculate_indicator_value(self, indicator_id, date_from=None, date_to=None, location=None):
        """Calculate indicator value based on its configuration"""
        try:
            indicator = Indicator.objects.get(id=indicator_id)
            rule = IndicatorCalculationRule.objects.filter(indicator=indicator, is_active=True).first()

            if not rule:
                # Default to manual if no rule exists
                return self._get_latest_achievement(indicator, date_from, date_to)

            if rule.calculation_type == 'MANUAL':
                return self._get_latest_achievement(indicator, date_from, date_to)

            elif rule.calculation_type == 'SYSTEM':
                method_name = rule.calculation_method
                if method_name in self.calculation_methods:
                    return self.calculation_methods[method_name](
                        indicator, date_from, date_to, location, rule.calculation_config
                    )
                else:
                    return {'value': 0, 'error': f'Unknown calculation method: {method_name}'}

            elif rule.calculation_type == 'MIXED':
                # Get system calculated value
                system_value = 0
                if rule.calculation_method in self.calculation_methods:
                    system_result = self.calculation_methods[rule.calculation_method](
                        indicator, date_from, date_to, location, rule.calculation_config
                    )
                    system_value = system_result.get('value', 0)

                # Get manual adjustment
                manual_result = self._get_latest_achievement(indicator, date_from, date_to)
                manual_value = manual_result.get('value', 0)

                # Combine based on config
                combine_method = rule.calculation_config.get('combine_method', 'add')
                if combine_method == 'add':
                    final_value = system_value + manual_value
                elif combine_method == 'max':
                    final_value = max(system_value, manual_value)
                elif combine_method == 'replace':
                    final_value = manual_value if manual_value > 0 else system_value
                else:
                    final_value = system_value

                return {
                    'value': final_value,
                    'system_value': system_value,
                    'manual_value': manual_value,
                    'calculation_type': 'MIXED'
                }

        except Exception as e:
            return {'value': 0, 'error': str(e)}

    def _get_latest_achievement(self, indicator, date_from=None, date_to=None):
        """Get the latest manual achievement entry"""
        query = IndicatorAchievement.objects.filter(indicator=indicator)

        if date_from:
            query = query.filter(date__gte=date_from)
        if date_to:
            query = query.filter(date__lte=date_to)

        latest = query.order_by('-date', '-timestamp').first()

        if latest:
            return {
                'value': float(latest.achieved),
                'date': latest.date,
                'comment': latest.comment,
                'calculation_type': 'MANUAL'
            }
        return {'value': 0, 'calculation_type': 'MANUAL'}

    # Calculation methods for each indicator type
    def _count_households_registered(self, indicator, date_from, date_to, location, config):
        """Count total households registered (Indicator 1)"""
        query = Group.objects.filter(
            is_deleted=False
        )

        if date_from:
            query = query.filter(date_created__gte=date_from)
        if date_to:
            query = query.filter(date_created__lte=date_to)
        if location:
            query = query.filter(location__parent__parent=location)

        count = query.count()
        return {'value': count, 'calculation_type': 'SYSTEM'}

    def _count_households_refugees(self, indicator, date_from, date_to, location, config):
        """Count refugee households (Indicator 2).

        Filter by the menage_refugie flag in json_ext rather than excluding
        host communes, which was incorrectly returning the total household
        count minus host-commune households.
        """
        query = Group.objects.filter(
            is_deleted=False,
            json_ext__menage_refugie='OUI',
        )

        if date_from:
            query = query.filter(date_created__gte=date_from)
        if date_to:
            query = query.filter(date_created__lte=date_to)
        if location:
            query = query.filter(location__parent__parent=location)

        count = query.count()

        # Get gender breakdown from the primary individual in each group
        from individual.models import GroupIndividual
        gender_data = {}
        primary_individuals = GroupIndividual.objects.filter(
            group__in=query,
            recipient_type=GroupIndividual.RecipientType.PRIMARY,
        ).select_related('individual')
        for gi in primary_individuals:
            if gi.individual and gi.individual.json_ext:
                gender = gi.individual.json_ext.get('sexe', 'unknown')
                gender_data[gender] = gender_data.get(gender, 0) + 1

        return {
            'value': count,
            'gender_breakdown': gender_data,
            'calculation_type': 'SYSTEM'
        }

    def _count_households_host(self, indicator, date_from, date_to, location, config):
        """Count host community households (Indicator 3)"""
        query = Group.objects.filter(
            is_deleted=False,
            location__parent__name__in=HOST_COMMUNES
        )

        if date_from:
            query = query.filter(date_created__gte=date_from)
        if date_to:
            query = query.filter(date_created__lte=date_to)
        if location:
            query = query.filter(location__parent__parent=location)

        count = query.count()
        return {'value': count, 'calculation_type': 'SYSTEM'}

    def _count_beneficiaries_social_protection(self, indicator, date_from, date_to, location, config):
        """Count total beneficiaries (Indicator 5)"""
        query = GroupBeneficiary.objects.filter(
            is_deleted=False,
            status__in=['ACTIVE', 'VALIDATED', 'POTENTIAL']
        )

        if date_from:
            query = query.filter(date_created__gte=date_from)
        if date_to:
            query = query.filter(date_created__lte=date_to)
        if location:
            query = query.filter(group__location__parent__parent=location)

        count = query.distinct().count()
        breakdowns = self._compute_breakdowns(
            benefit_plan_codes=config.get('benefit_plan_codes', ['1.1', '1.2', '1.4']),
            location=location,
        )
        return {'value': count, 'calculation_type': 'SYSTEM', 'breakdowns': breakdowns}

    def _count_beneficiaries_women(self, indicator, date_from, date_to, location, config):
        """Count female beneficiaries (Indicator 6)"""
        query = GroupBeneficiary.objects.filter(
            is_deleted=False,
            status__in=['ACTIVE', 'VALIDATED', 'POTENTIAL'],
            group__groupindividuals__individual__json_ext__sexe='F',
            group__groupindividuals__recipient_type='PRIMARY'
        )

        if date_from:
            query = query.filter(date_due__gte=date_from)
        if date_to:
            query = query.filter(date_due__lte=date_to)
        if location:
            query = query.filter(group__location__parent__parent=location)

        count = query.distinct().count()
        breakdowns = self._compute_breakdowns(
            benefit_plan_codes=config.get('benefit_plan_codes'),
            location=location,
        )
        return {'value': count, 'calculation_type': 'SYSTEM', 'breakdowns': breakdowns}

    def _count_beneficiaries_unconditional_transfers(self, indicator, date_from, date_to, location, config):
        """Count beneficiaries of unconditional transfers — programme 1.2 (Indicator 7)"""
        codes = config.get('benefit_plan_codes', ['1.2'])
        query = GroupBeneficiary.objects.filter(
            is_deleted=False,
            status__in=['ACTIVE', 'VALIDATED', 'POTENTIAL'],
            benefit_plan__code__in=codes,
        )

        if date_from:
            query = query.filter(date_created__gte=date_from)
        if date_to:
            query = query.filter(date_created__lte=date_to)
        if location:
            query = query.filter(group__location__parent__parent=location)

        count = query.distinct().count()
        breakdowns = self._compute_breakdowns(benefit_plan_codes=codes, location=location)
        return {'value': count, 'calculation_type': 'SYSTEM', 'breakdowns': breakdowns}

    def _count_beneficiaries_employment(self, indicator, date_from, date_to, location, config):
        """Count beneficiaries of employment interventions (Indicator 11).
        MicroProject is a snapshot — use latest-per-colline aggregation.
        """
        result = self._count_snapshot_beneficiaries(
            'merankabandi_microproject', date_from, date_to, location,
        )
        breakdowns = self._compute_breakdowns(
            benefit_plan_codes=config.get('benefit_plan_codes'), location=location,
        )
        return {'value': result['total'], 'calculation_type': 'SYSTEM', 'breakdowns': breakdowns}

    def _count_provinces_with_transfers(self, indicator, date_from, date_to, location, config):
        """Count provinces implementing transfers (Indicator 16).
        Counts distinct provinces that have active beneficiaries with payment records.
        Uses GroupBeneficiary → Group → Location → Province chain as the most reliable source.
        """
        from django.db import connection
        query = """
            SELECT COUNT(DISTINCT prov."LocationId")
            FROM social_protection_groupbeneficiary gb
            JOIN individual_group grp ON grp."UUID" = gb.group_id AND grp."isDeleted" = false
            JOIN "tblLocations" col ON col."LocationId" = grp.location_id
            JOIN "tblLocations" com ON com."LocationId" = col."ParentLocationId"
            JOIN "tblLocations" prov ON prov."LocationId" = com."ParentLocationId"
            WHERE gb."isDeleted" = false
              AND prov."LocationType" = 'D'
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
            count = cursor.fetchone()[0]
        return {'value': count or 0, 'calculation_type': 'SYSTEM'}

    def _calculate_payment_timeliness(self, indicator, date_from, date_to, location, config):
        """Calculate percentage of beneficiaries paid on time (Indicator 17)"""
        # This would need payment schedule data to calculate properly
        # For now, return manual value
        return self._get_latest_achievement(indicator, date_from, date_to)

    def _count_beneficiaries_emergency_transfers(self, indicator, date_from, date_to, location, config):
        codes = config.get('benefit_plan_codes', ['1.1'])
        query = GroupBeneficiary.objects.filter(
            is_deleted=False,
            status__in=['ACTIVE', 'VALIDATED', 'POTENTIAL'],
            benefit_plan__code__in=codes,
        )

        if date_from:
            query = query.filter(date_due__gte=date_from)
        if date_to:
            query = query.filter(date_due__lte=date_to)
        if location:
            query = query.filter(group__location__parent__parent=location)

        count = query.distinct().count()
        breakdowns = self._compute_breakdowns(benefit_plan_codes=codes, location=location)
        return {'value': count, 'calculation_type': 'SYSTEM', 'breakdowns': breakdowns}

    def _count_beneficiaries_refugees(self, indicator, date_from, date_to, location, config):
        """Count refugee beneficiaries using the menage_refugie flag."""
        query = GroupBeneficiary.objects.filter(
            is_deleted=False,
            status__in=['ACTIVE', 'VALIDATED', 'POTENTIAL'],
            group__json_ext__menage_refugie='OUI',
        )

        if date_from:
            query = query.filter(date_created__gte=date_from)
        if date_to:
            query = query.filter(date_created__lte=date_to)
        if location:
            query = query.filter(group__location__parent__parent=location)

        count = query.distinct().count()
        breakdowns = self._compute_breakdowns(
            benefit_plan_codes=config.get('benefit_plan_codes'),
            location=location,
        )
        return {'value': count, 'calculation_type': 'SYSTEM', 'breakdowns': breakdowns}

    def _count_beneficiaries_host_communities(self, indicator, date_from, date_to, location, config):
        query = GroupBeneficiary.objects.filter(
            is_deleted=False,
            status__in=['ACTIVE', 'VALIDATED', 'POTENTIAL'],
            group__location__parent__name__in=HOST_COMMUNES,
        )

        if date_from:
            query = query.filter(date_created__gte=date_from)
        if date_to:
            query = query.filter(date_created__lte=date_to)
        if location:
            query = query.filter(group__location__parent__parent=location)

        count = query.distinct().count()
        breakdowns = self._compute_breakdowns(
            benefit_plan_codes=config.get('benefit_plan_codes'),
            location=location,
        )
        return {'value': count, 'calculation_type': 'SYSTEM', 'breakdowns': breakdowns}

    def _count_beneficiaries_employment_women(self, indicator, date_from, date_to, location, config):
        """Count female employment beneficiaries — latest per colline."""
        result = self._count_snapshot_beneficiaries(
            'merankabandi_microproject', date_from, date_to, location,
        )
        return {'value': result['female'], 'calculation_type': 'SYSTEM'}

    def _count_beneficiaries_employment_refugees(self, indicator, date_from, date_to, location, config):
        """Count refugee employment beneficiaries — latest per colline in refugee areas."""
        if not REFUGEE_COLLINES:
            return {'value': 0, 'calculation_type': 'SYSTEM'}
        placeholders = ','.join(['%s'] * len(REFUGEE_COLLINES))
        result = self._count_snapshot_beneficiaries(
            'merankabandi_microproject', date_from, date_to, location=None,
            extra_where=f'location_id IN (SELECT "LocationId" FROM "tblLocations" WHERE "LocationName" IN ({placeholders}))',
            extra_params=REFUGEE_COLLINES,
        )
        return {'value': result['total'], 'calculation_type': 'SYSTEM'}

    def _count_beneficiaries_employment_host(self, indicator, date_from, date_to, location, config):
        """Count host community employment beneficiaries — latest per colline in host communes."""
        placeholders = ','.join(['%s'] * len(HOST_COMMUNES))
        result = self._count_snapshot_beneficiaries(
            'merankabandi_microproject', date_from, date_to, location=None,
            extra_where=f"""location_id IN (
                SELECT l1."LocationId" FROM "tblLocations" l1
                JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
                WHERE l2."LocationName" IN ({placeholders})
            )""",
            extra_params=HOST_COMMUNES,
        )
        return {'value': result['total'], 'calculation_type': 'SYSTEM'}

    def _count_farmers_received_services(self, indicator, date_from, date_to, location, config):
        # Count from microproject participants
        microproject_query = MicroProject.objects.filter(validation_status='VALIDATED')

        if date_from:
            microproject_query = microproject_query.filter(report_date__gte=date_from)
        if date_to:
            microproject_query = microproject_query.filter(report_date__lte=date_to)
        if location:
            microproject_query = microproject_query.filter(location__parent__parent=location)

        # Sum participants
        microproject_total = microproject_query.aggregate(
            total=Sum('agriculture_beneficiaries')
        )['total'] or 0

        return {'value': microproject_total, 'calculation_type': 'SYSTEM'}

    def _calculate_behavior_change_participation(self, indicator, date_from, date_to, location, config):
        """Calculate percentage of beneficiaries adopting behavior changes (Indicator 18).
        BCP is a snapshot — use latest per colline for the numerator.
        """
        bcp_result = self._count_snapshot_beneficiaries(
            'merankabandi_behaviorchangepromotion', date_from, date_to, location,
        )
        total_participants = bcp_result['total']

        beneficiary_query = GroupBeneficiary.objects.filter(
            is_deleted=False, status__in=['ACTIVE', 'VALIDATED']
        )
        if location:
            beneficiary_query = beneficiary_query.filter(group__location__parent__parent=location)
        total_beneficiaries = beneficiary_query.count()

        if total_beneficiaries > 0:
            percentage = (total_participants / total_beneficiaries) * 100
            return {'value': min(percentage, 100), 'calculation_type': 'SYSTEM'}
        return {'value': 0, 'calculation_type': 'SYSTEM'}

    def _count_approved_business_plans(self, indicator, date_from, date_to, location, config):
        """Count beneficiaries with approved business plans (Indicator 20).
        MicroProject is a snapshot — latest per colline.
        """
        result = self._count_snapshot_beneficiaries(
            'merankabandi_microproject', date_from, date_to, location,
        )
        return {'value': result['total'], 'calculation_type': 'MIXED'}

    def _count_approved_business_plans_women(self, indicator, date_from, date_to, location, config):
        """Count female beneficiaries with approved business plans (Indicator 21)."""
        result = self._count_snapshot_beneficiaries(
            'merankabandi_microproject', date_from, date_to, location,
        )
        return {'value': result['female'], 'calculation_type': 'MIXED'}

    def _count_approved_business_plans_batwa(self, indicator, date_from, date_to, location, config):
        """Count Batwa beneficiaries with approved business plans (Indicator 22)"""
        # This would need specific tracking of Batwa beneficiaries
        # For now, use manual entry
        return self._get_latest_achievement(indicator, date_from, date_to)

    def _count_climate_resilient_activities(self, indicator, date_from, date_to, location, config):
        """Count distinct collines with micro-project activity (Indicator 23).
        MicroProject is a snapshot — count distinct collines, not total records.
        """
        from django.db import connection as db_connection
        conditions = ["validation_status = 'VALIDATED'"]
        params = []
        if date_from:
            conditions.append("report_date >= %s")
            params.append(date_from)
        if date_to:
            conditions.append("report_date <= %s")
            params.append(date_to)
        if location:
            conditions.append("""location_id IN (
                SELECT l1."LocationId" FROM "tblLocations" l1
                JOIN "tblLocations" l2 ON l1."ParentLocationId" = l2."LocationId"
                WHERE l2."ParentLocationId" = %s
            )""")
            params.append(location.id if hasattr(location, 'id') else location)
        where = ' AND '.join(conditions)
        sql = f"SELECT COUNT(DISTINCT location_id) FROM merankabandi_microproject WHERE {where}"
        with db_connection.cursor() as cursor:
            cursor.execute(sql, params)
            count = cursor.fetchone()[0]
        return {'value': count or 0, 'calculation_type': 'MIXED'}

    def _calculate_digital_payment_percentage(self, indicator, date_from, date_to, location, config):
        """Calculate percentage of beneficiaries receiving digital payments (Indicator 28)"""
        # Count beneficiaries with digital payment method
        query = BenefitConsumption.objects.filter(
            individual__is_deleted=False,
            json_ext__payment_method='DIGITAL'
        )

        if date_from:
            query = query.filter(date_created__gte=date_from)
        if date_to:
            query = query.filter(date_created__lte=date_to)
        if location:
            query = query.filter(individual__group__location__parent__parent=location)

        digital_count = query.values('individual').distinct().count()

        # Get total beneficiaries who received payments
        total_query = BenefitConsumption.objects.filter(
            individual__is_deleted=False
        )

        if date_from:
            total_query = total_query.filter(date_created__gte=date_from)
        if date_to:
            total_query = total_query.filter(date_created__lte=date_to)
        if location:
            total_query = total_query.filter(individual__group__location__parent__parent=location)

        total_count = total_query.values('individual').distinct().count()

        if total_count > 0:
            percentage = (digital_count / total_count) * 100
            return {'value': percentage, 'calculation_type': 'SYSTEM'}

        return {'value': 0, 'calculation_type': 'SYSTEM'}

    def create_snapshot(self, name, description, user, date_from=None, date_to=None):
        """Create a complete snapshot of the result framework"""
        snapshot_data = {
            'sections': [],
            'metadata': {
                'created_date': timezone.now().isoformat(),
                'created_by': user.username if user else 'System',
                'date_from': date_from.isoformat() if date_from else None,
                'date_to': date_to.isoformat() if date_to else None,
            }
        }

        for section in Section.objects.all().prefetch_related('indicators'):
            section_data = {
                'id': section.id,
                'name': section.name,
                'indicators': []
            }

            for indicator in section.indicators.all():
                # Calculate current value
                result = self.calculate_indicator_value(
                    indicator.id,
                    date_from=date_from,
                    date_to=date_to
                )
                print([indicator.name, result])
                achieved_value = result.get('value', 0)
                target_value = float(indicator.target) if indicator.target else 0

                indicator_data = {
                    'id': indicator.id,
                    'name': indicator.name,
                    'pbc': indicator.pbc or '',
                    'baseline': float(indicator.baseline) if indicator.baseline else 0,
                    'target': target_value,
                    'achieved': achieved_value,
                    'percentage': (achieved_value / target_value * 100) if target_value > 0 else 0,
                    'calculation_type': result.get('calculation_type', 'MANUAL'),
                    'observation': indicator.observation or '',
                    'breakdowns': result.get('breakdowns', []),
                }

                # Save IndicatorAchievement record if value was calculated (not manual)
                if result.get('calculation_type') in ['SYSTEM', 'MIXED'] and achieved_value > 0:
                    achievement_date = date_to if date_to else timezone.now().date()

                    # Create or update achievement for this date
                    IndicatorAchievement.objects.create(
                        indicator=indicator,
                        date=achievement_date,
                        achieved=Decimal(str(achieved_value)),
                        comment=f'Auto-generated from snapshot: {name} (Calculation: {result.get("calculation_type")})',
                        breakdowns=result.get('breakdowns', []),
                    )

                # Add any additional data from calculation
                if 'gender_breakdown' in result:
                    indicator_data['gender_breakdown'] = result['gender_breakdown']
                if 'error' in result:
                    indicator_data['error'] = result['error']

                section_data['indicators'].append(indicator_data)

            snapshot_data['sections'].append(section_data)

        # Create snapshot record
        snapshot = ResultFrameworkSnapshot.objects.create(
            name=name,
            description=description,
            created_by=user,
            data=snapshot_data,
            status='DRAFT'
        )

        return snapshot
