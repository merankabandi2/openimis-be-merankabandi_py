from oauth2_provider.oauth2_validators import OAuth2Validator
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class RestrictedScopeOAuth2Validator(OAuth2Validator):
    """
    Custom OAuth2 validator that restricts scopes based on application name
    """
    
    def validate_scopes(self, client_id, scopes, client, request, *args, **kwargs):
        """
        Validate that the client can request these scopes
        """
        # First check if scopes are valid in the system
        if not super().validate_scopes(client_id, scopes, client, request, *args, **kwargs):
            return False
        
        # Get application name
        app_name = client.name if hasattr(client, 'name') else None
        
        # Get scope restrictions from settings
        app_scopes = getattr(settings, 'OAUTH2_APPLICATION_SCOPES', {})
        
        # If application is not in our restricted list, allow all scopes (backwards compatibility)
        if app_name not in app_scopes:
            logger.warning(f"Application '{app_name}' not in OAUTH2_APPLICATION_SCOPES, allowing all scopes")
            return True
        
        # Check if requested scopes are allowed for this application
        allowed_scopes = app_scopes[app_name]
        requested_scopes = scopes
        
        # Check each requested scope
        for scope in requested_scopes:
            if scope not in allowed_scopes:
                logger.error(f"Application '{app_name}' requested unauthorized scope: {scope}")
                return False
        
        logger.info(f"Application '{app_name}' validated for scopes: {', '.join(requested_scopes)}")
        return True