"""REST endpoints for operational reports (read-only)."""
import datetime
import logging
import os

from django.conf import settings
from django.http import FileResponse, Http404, HttpResponse
from rest_framework import status
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response

from core.views import check_user_rights
from social_protection.apps import SocialProtectionConfig

from merankabandi.reports.account_creation_report import AccountCreationReportService
from merankabandi.tasks import ACCOUNT_REPORT_SUBDIR, generate_account_creation_report

logger = logging.getLogger(__name__)

XLSX_CONTENT_TYPE = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

# Beneficiary count at/below which the report is built synchronously and streamed.
# Above it, the build is dispatched to Celery and the user is notified when ready
# (the N+1 per-row recipient lookup makes large scopes too slow for one HTTP request).
ACCOUNT_REPORT_SYNC_THRESHOLD = 5000


@api_view(["GET"])
@permission_classes([check_user_rights(SocialProtectionConfig.gql_beneficiary_search_perms, )])
def account_creation_report_view(request):
    """GET account-creation report. Params: benefit_plan_id + (province_id XOR payment_agency_id).

    Small scopes (<= ACCOUNT_REPORT_SYNC_THRESHOLD beneficiaries) stream the xlsx
    directly (200). Large scopes enqueue a Celery build and return 202; the user is
    notified (in-app + email) with a download link when it is ready.
    """
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

    service = AccountCreationReportService(request.user)
    try:
        size = service.estimate_size(
            benefit_plan_id, province_id=province_id, payment_agency_id=payment_agency_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("account_creation_report size estimate failed", exc_info=exc)
        return Response({'success': False, 'error': 'Failed to estimate report size'},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    # Large scope -> async build + notify.
    if size > ACCOUNT_REPORT_SYNC_THRESHOLD:
        try:
            generate_account_creation_report.delay(
                str(request.user.id), benefit_plan_id,
                province_id=province_id, payment_agency_id=payment_agency_id)
        except Exception as exc:  # broker unavailable etc. — don't 500 the request
            logger.error("account_creation_report enqueue failed", exc_info=exc)
            return Response(
                {'success': False,
                 'error': 'Report service temporarily unavailable, please retry later'},
                status=status.HTTP_503_SERVICE_UNAVAILABLE)
        return Response(
            {'success': True, 'async': True, 'count': size,
             'message': ('Le rapport est en cours de génération. Vous serez '
                         'notifié(e) par email et notification quand il sera prêt.')},
            status=status.HTTP_202_ACCEPTED)

    # Small scope -> build synchronously and stream.
    try:
        buf = service.build_workbook(
            benefit_plan_id, province_id=province_id, payment_agency_id=payment_agency_id)
    except Exception as exc:  # noqa: BLE001
        logger.error("account_creation_report failed", exc_info=exc)
        return Response({'success': False, 'error': 'Failed to generate report'},
                        status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    scope = 'province' if province_id else 'agency'
    ts = datetime.datetime.now().strftime('%Y%m%d')
    filename = f'comptes_finbank_{scope}_{ts}.xlsx'
    response = HttpResponse(buf.read(), content_type=XLSX_CONTENT_TYPE)
    response['Content-Disposition'] = f'attachment; filename="{filename}"'
    return response


@api_view(["GET"])
@permission_classes([check_user_rights(SocialProtectionConfig.gql_beneficiary_search_perms, )])
def account_creation_report_download_view(request, filename):
    """Serve a previously async-generated account-creation report by filename.
    The notification link points here. Auth-gated; path-traversal protected."""
    safe_name = os.path.basename(filename)
    if safe_name != filename or not safe_name.endswith('.xlsx'):
        raise Http404("Invalid file name")
    report_dir = os.path.join(
        getattr(settings, 'MEDIA_ROOT', 'file_storage'), ACCOUNT_REPORT_SUBDIR)
    filepath = os.path.join(report_dir, safe_name)
    if not os.path.exists(filepath):
        raise Http404("Report not found or expired")
    response = FileResponse(open(filepath, 'rb'), content_type=XLSX_CONTENT_TYPE)
    response['Content-Disposition'] = f'attachment; filename="{safe_name}"'
    return response
