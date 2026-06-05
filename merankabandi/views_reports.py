"""REST endpoints for operational reports (read-only)."""
import datetime
import logging

from django.http import HttpResponse
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from core.views import check_user_rights
from social_protection.apps import SocialProtectionConfig

from merankabandi.reports.account_creation_report import AccountCreationReportService

logger = logging.getLogger(__name__)

XLSX_CONTENT_TYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'


@api_view(["GET"])
@permission_classes([check_user_rights(SocialProtectionConfig.gql_beneficiary_search_perms, )])
def account_creation_report_view(request):
    """GET account-creation report. Params: benefit_plan_id + (province_id XOR payment_agency_id)."""
    benefit_plan_id = request.query_params.get('benefit_plan_id')
    province_id = request.query_params.get('province_id')
    payment_agency_id = request.query_params.get('payment_agency_id')

    if not benefit_plan_id:
        return Response({'success': False, 'error': 'benefit_plan_id is required'},
                        status=status.HTTP_400_BAD_REQUEST)
    if bool(province_id) == bool(payment_agency_id):
        return Response(
            {'success': False, 'error': 'Provide exactly one of province_id or payment_agency_id'},
            status=status.HTTP_400_BAD_REQUEST)

    try:
        buf = AccountCreationReportService(request.user).build_workbook(
            benefit_plan_id, province_id=province_id, payment_agency_id=payment_agency_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("account_creation_report failed", exc_info=exc)
        return Response({'success': False, 'error': str(exc)},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    scope = 'province' if province_id else 'agency'
    ts = datetime.datetime.now().strftime('%Y%m%d')
    filename = f'comptes_finbank_{scope}_{ts}.xlsx'
    response = HttpResponse(buf.read(), content_type=XLSX_CONTENT_TYPE)
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response
