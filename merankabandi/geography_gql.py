"""
GraphQL Schema for Geography Location Pages
Provides province summaries and location detail views.
"""

import graphene
from .geography_service import GeographyService


# GraphQL Types for Geography Data

class GeoLocationType(graphene.ObjectType):
    id = graphene.Int()
    uuid = graphene.String()
    code = graphene.String()
    name = graphene.String()
    type = graphene.String()
    parent = graphene.Field(lambda: GeoLocationType)


class GeoChildType(graphene.ObjectType):
    id = graphene.Int()
    uuid = graphene.String()
    code = graphene.String()
    name = graphene.String()
    type = graphene.String()
    total_households = graphene.Int()
    total_beneficiaries = graphene.Int()
    total_amount_disbursed = graphene.Float()
    payment_rate = graphene.Float()
    child_count = graphene.Int()


class GeoProgramType(graphene.ObjectType):
    id = graphene.String()
    name = graphene.String()
    code = graphene.String()
    beneficiary_count = graphene.Int()
    household_count = graphene.Int()
    amount_disbursed = graphene.Float()
    cycle_count = graphene.Int()
    status = graphene.String()


class GeoPaymentHistoryType(graphene.ObjectType):
    cycle_name = graphene.String()
    date = graphene.String()
    amount_planned = graphene.Float()
    amount_paid = graphene.Float()
    beneficiary_count = graphene.Int()
    payment_rate = graphene.Float()
    payment_source = graphene.String()


class GeoPaymentPointType(graphene.ObjectType):
    id = graphene.Int()
    payment_point_name = graphene.String()
    benefit_plan_name = graphene.String()
    is_inherited = graphene.Boolean()


class GeoHouseholdType(graphene.ObjectType):
    group_id = graphene.String()
    group_uuid = graphene.String()
    head_of_household_name = graphene.String()
    social_id = graphene.String()
    status = graphene.String()
    pmt_score = graphene.Float()
    member_count = graphene.Int()
    last_payment_date = graphene.String()
    last_payment_amount = graphene.Float()


class GeographyLocationDetailType(graphene.ObjectType):
    location = graphene.Field(GeoLocationType)
    total_households = graphene.Int()
    total_individuals = graphene.Int()
    total_beneficiaries = graphene.Int()
    total_amount_disbursed = graphene.Float()
    payment_cycle_count = graphene.Int()
    payment_rate = graphene.Float()
    children = graphene.List(GeoChildType)
    active_programs = graphene.List(GeoProgramType)
    payment_history = graphene.List(GeoPaymentHistoryType)
    payment_points = graphene.List(GeoPaymentPointType)
    households = graphene.List(GeoHouseholdType)


class GeoProvinceSummaryType(graphene.ObjectType):
    id = graphene.Int()
    uuid = graphene.String()
    code = graphene.String()
    name = graphene.String()
    total_households = graphene.Int()
    total_individuals = graphene.Int()
    total_beneficiaries = graphene.Int()
    total_amount_disbursed = graphene.Float()
    payment_cycle_count = graphene.Int()
    payment_rate = graphene.Float()
    agency_count = graphene.Int()


# Query Mixin

class GeographyQuery(graphene.ObjectType):
    """GraphQL queries for geography location pages."""

    geography_location_detail = graphene.Field(
        GeographyLocationDetailType,
        location_id=graphene.Int(required=True, description="Location ID"),
        benefit_plan_id=graphene.String(description="Filter by benefit plan UUID"),
        year=graphene.Int(description="Filter by year"),
    )

    geography_provinces_summary = graphene.List(
        GeoProvinceSummaryType,
        benefit_plan_id=graphene.String(description="Filter by benefit plan UUID"),
        year=graphene.Int(description="Filter by year"),
    )

    def resolve_geography_location_detail(self, info, location_id, benefit_plan_id=None, year=None):
        """Resolve location detail with KPIs, children, programs, payments, and households."""
        if not info.context.user.is_authenticated:
            raise PermissionError("Authentication required")

        data = GeographyService.get_location_detail(
            location_id=location_id,
            benefit_plan_id=benefit_plan_id,
            year=year,
        )

        if not data:
            return None

        location_data = data.get('location', {})
        parent_data = location_data.get('parent')
        parent_obj = None
        if parent_data:
            grandparent_data = parent_data.get('parent')
            grandparent_obj = None
            if grandparent_data:
                grandparent_obj = GeoLocationType(
                    id=grandparent_data.get('id'),
                    uuid=grandparent_data.get('uuid'),
                    code=grandparent_data.get('code'),
                    name=grandparent_data.get('name'),
                    type=grandparent_data.get('type'),
                    parent=None,
                )
            parent_obj = GeoLocationType(
                id=parent_data.get('id'),
                uuid=parent_data.get('uuid'),
                code=parent_data.get('code'),
                name=parent_data.get('name'),
                type=parent_data.get('type'),
                parent=grandparent_obj,
            )

        location_obj = GeoLocationType(
            id=location_data.get('id'),
            uuid=location_data.get('uuid'),
            code=location_data.get('code'),
            name=location_data.get('name'),
            type=location_data.get('type'),
            parent=parent_obj,
        )

        children = [
            GeoChildType(
                id=c.get('id'),
                uuid=c.get('uuid'),
                code=c.get('code'),
                name=c.get('name'),
                type=c.get('type'),
                total_households=c.get('total_households'),
                total_beneficiaries=c.get('total_beneficiaries'),
                total_amount_disbursed=c.get('total_amount_disbursed'),
                payment_rate=c.get('payment_rate'),
                child_count=c.get('child_count'),
            )
            for c in data.get('children', [])
        ]

        programs = [
            GeoProgramType(
                id=p.get('id'),
                name=p.get('name'),
                code=p.get('code'),
                beneficiary_count=p.get('beneficiary_count'),
                household_count=p.get('household_count'),
                amount_disbursed=p.get('amount_disbursed'),
                cycle_count=p.get('cycle_count'),
                status=p.get('status'),
            )
            for p in data.get('active_programs', [])
        ]

        payment_history = [
            GeoPaymentHistoryType(
                cycle_name=h.get('cycle_name'),
                date=h.get('date'),
                amount_planned=h.get('amount_planned'),
                amount_paid=h.get('amount_paid'),
                beneficiary_count=h.get('beneficiary_count'),
                payment_rate=h.get('payment_rate'),
                payment_source=h.get('payment_source'),
            )
            for h in data.get('payment_history', [])
        ]

        payment_points = [
            GeoPaymentPointType(
                id=pp.get('id'),
                payment_point_name=pp.get('payment_point_name'),
                benefit_plan_name=pp.get('benefit_plan_name'),
                is_inherited=pp.get('is_inherited'),
            )
            for pp in data.get('payment_points', [])
        ]

        households = [
            GeoHouseholdType(
                group_id=hh.get('group_id'),
                group_uuid=hh.get('group_uuid'),
                head_of_household_name=hh.get('head_of_household_name'),
                social_id=hh.get('social_id'),
                status=hh.get('status'),
                pmt_score=hh.get('pmt_score'),
                member_count=hh.get('member_count'),
                last_payment_date=hh.get('last_payment_date'),
                last_payment_amount=hh.get('last_payment_amount'),
            )
            for hh in data.get('households', [])
        ]

        return GeographyLocationDetailType(
            location=location_obj,
            total_households=data.get('total_households'),
            total_individuals=data.get('total_individuals'),
            total_beneficiaries=data.get('total_beneficiaries'),
            total_amount_disbursed=data.get('total_amount_disbursed'),
            payment_cycle_count=data.get('payment_cycle_count'),
            payment_rate=data.get('payment_rate'),
            children=children,
            active_programs=programs,
            payment_history=payment_history,
            payment_points=payment_points,
            households=households,
        )

    def resolve_geography_provinces_summary(self, info, benefit_plan_id=None, year=None):
        """Resolve list of all provinces with stats."""
        if not info.context.user.is_authenticated:
            raise PermissionError("Authentication required")

        data = GeographyService.get_provinces_summary(
            benefit_plan_id=benefit_plan_id,
            year=year,
        )

        return [
            GeoProvinceSummaryType(
                id=p.get('id'),
                uuid=p.get('uuid'),
                code=p.get('code'),
                name=p.get('name'),
                total_households=p.get('total_households'),
                total_individuals=p.get('total_individuals'),
                total_beneficiaries=p.get('total_beneficiaries'),
                total_amount_disbursed=p.get('total_amount_disbursed'),
                payment_cycle_count=p.get('payment_cycle_count'),
                payment_rate=p.get('payment_rate'),
                agency_count=p.get('agency_count'),
            )
            for p in data
        ]
