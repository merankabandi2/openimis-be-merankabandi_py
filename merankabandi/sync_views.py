import json
import logging

from django.http import JsonResponse
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import IsAuthenticated

from merankabandi.sync_service import SyncService

logger = logging.getLogger(__name__)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def sync_pull(request):
    """
    WatermelonDB pull endpoint.

    Body (JSON):
      {
        "lastSyncTimestamp": <epoch-ms integer or null>,
        "tables": ["households", "grievances", ...]  // optional, omit for all
      }

    Returns:
      {
        "changes": {
          "locations":          { "created": [...], "updated": [...], "deleted": [...] },
          "households":         { "created": [...], "updated": [...], "deleted": [...] },
          "individuals":        { "created": [...], "updated": [...], "deleted": [...] },
          "beneficiaries":      { "created": [...], "updated": [...], "deleted": [...] },
          "activities":         { "created": [...], "updated": [...], "deleted": [...] },
          "grievances":         { "created": [...], "updated": [...], "deleted": [...] },
          "grievance_comments": { "created": [...], "updated": [...], "deleted": [...] },
          "payments":           { "created": [...], "updated": [...], "deleted": [...] }
        },
        "timestamp": <epoch-ms integer>
      }
    """
    try:
        body = json.loads(request.body) if request.body else {}
    except (json.JSONDecodeError, Exception) as exc:
        return JsonResponse({"error": f"Invalid JSON body: {exc}"}, status=400)

    last_sync_timestamp = body.get("lastSyncTimestamp")
    tables = body.get("tables")

    try:
        result = SyncService.pull(
            user=request.user,
            last_sync_timestamp=last_sync_timestamp,
            tables=tables,
        )
    except Exception as exc:
        logger.exception("sync_pull error for user %s", request.user)
        return JsonResponse({"error": str(exc)}, status=500)

    return JsonResponse(result)


@api_view(['POST'])
@permission_classes([IsAuthenticated])
def sync_push(request):
    """
    WatermelonDB push endpoint.

    Body (JSON):
      {
        "changes": {
          "grievances":         { "created": [...], "updated": [...] },
          "grievance_comments": { "created": [...], "updated": [...] }
        }
      }

    Field names in records use snake_case matching the mobile WatermelonDB schema.

    Returns:
      { "success": true|false, "errors": [...] }
    """
    try:
        body = json.loads(request.body) if request.body else {}
    except (json.JSONDecodeError, Exception) as exc:
        return JsonResponse({"error": f"Invalid JSON body: {exc}"}, status=400)

    changes = body.get("changes", {})

    try:
        result = SyncService.push(user=request.user, changes=changes)
    except Exception as exc:
        logger.exception("sync_push error for user %s", request.user)
        return JsonResponse({"error": str(exc)}, status=500)

    return JsonResponse(result)
