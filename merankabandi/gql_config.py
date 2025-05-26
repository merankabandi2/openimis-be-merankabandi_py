"""
GraphQL Permission Configuration for Merankabandi Module

This module defines all the permissions used by the Merankabandi module.
Permission range: 160005-160016 (within social protection range)
"""

# Section Management Permissions (160005-160008)
GQL_SECTION_SEARCH_PERMS = ["160005"]  # Right to search/view sections
GQL_SECTION_CREATE_PERMS = ["160006"]  # Right to create new sections
GQL_SECTION_UPDATE_PERMS = ["160007"]  # Right to update existing sections
GQL_SECTION_DELETE_PERMS = ["160008"]  # Right to delete sections

# Indicator Management Permissions (160009-160012)
GQL_INDICATOR_SEARCH_PERMS = ["160009"]  # Right to search/view indicators
GQL_INDICATOR_CREATE_PERMS = ["160010"]  # Right to create new indicators
GQL_INDICATOR_UPDATE_PERMS = ["160011"]  # Right to update existing indicators
GQL_INDICATOR_DELETE_PERMS = ["160012"]  # Right to delete indicators

# Indicator Achievement Management Permissions (160013-160016)
GQL_INDICATOR_ACHIEVEMENT_SEARCH_PERMS = ["160013"]  # Right to search/view achievements
GQL_INDICATOR_ACHIEVEMENT_CREATE_PERMS = ["160014"]  # Right to create new achievements
GQL_INDICATOR_ACHIEVEMENT_UPDATE_PERMS = ["160015"]  # Right to update existing achievements
GQL_INDICATOR_ACHIEVEMENT_DELETE_PERMS = ["160016"]  # Right to delete achievements

# Permission Descriptions for Documentation
PERMISSION_DESCRIPTIONS = {
    "160005": "Search and view sections in results framework",
    "160006": "Create new sections in results framework",
    "160007": "Update existing sections in results framework",
    "160008": "Delete sections from results framework",
    "160009": "Search and view indicators",
    "160010": "Create new indicators",
    "160011": "Update existing indicators",
    "160012": "Delete indicators",
    "160013": "Search and view indicator achievements",
    "160014": "Create new indicator achievements",
    "160015": "Update existing indicator achievements",
    "160016": "Delete indicator achievements",
}