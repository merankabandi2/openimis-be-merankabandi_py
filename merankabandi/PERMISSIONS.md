# Merankabandi Module Permissions

This document describes the permission system used by the Merankabandi module.

## Permission Range

The Merankabandi module uses permissions in the range **160005-160016**, which is within the Social Protection module range but in a dedicated sub-range to avoid conflicts.

## Permission Structure

### Section Management (160005-160008)
- **160005** - `gql_section_search_perms`: Search and view sections in results framework
- **160006** - `gql_section_create_perms`: Create new sections in results framework
- **160007** - `gql_section_update_perms`: Update existing sections in results framework
- **160008** - `gql_section_delete_perms`: Delete sections from results framework

### Indicator Management (160009-160012)
- **160009** - `gql_indicator_search_perms`: Search and view indicators
- **160010** - `gql_indicator_create_perms`: Create new indicators
- **160011** - `gql_indicator_update_perms`: Update existing indicators
- **160012** - `gql_indicator_delete_perms`: Delete indicators

### Indicator Achievement Management (160013-160016)
- **160013** - `gql_indicator_achievement_search_perms`: Search and view indicator achievements
- **160014** - `gql_indicator_achievement_create_perms`: Create new indicator achievements
- **160015** - `gql_indicator_achievement_update_perms`: Update existing indicator achievements
- **160016** - `gql_indicator_achievement_delete_perms`: Delete indicator achievements

## How to Add Permissions to Database

### Option 1: SQL Script
Run the SQL script located at `merankabandi/sql/add_indicator_permissions.sql`:

```sql
-- This will add permissions for Administrator, Manager, and User roles
-- Adjust the script based on your role requirements
```

### Option 2: Django Admin
1. Log into Django Admin as a superuser
2. Navigate to Core > Role Rights
3. Add the permissions manually with the right IDs listed above

### Option 3: OpenIMIS UI
1. Log into OpenIMIS as Administrator
2. Navigate to Administration > User Roles
3. Edit the appropriate roles and add the permissions

## Configuration

Permissions are defined in:
- `merankabandi/gql_config.py` - Permission constants
- `merankabandi/apps.py` - Module configuration

The module automatically loads permissions from the configuration, allowing them to be overridden through the ModuleConfiguration system if needed.

## Best Practices

1. **Always check permissions exist**: The SQL script includes EXISTS checks to prevent duplicate entries
2. **Document new permissions**: If adding new permissions, update this document
3. **Avoid permission conflicts**: Check with other modules before claiming a permission range
4. **Use meaningful names**: Permission names should clearly indicate what they control

## Troubleshooting

### Users can't access indicators
1. Check if the user's role has permission 160009 (search indicators)
2. Verify permissions are loaded: Check tblRoleRight table
3. Clear Django cache after adding permissions

### Permission denied errors
1. Check the specific permission number in the error
2. Verify the user's role has that permission
3. Ensure the module configuration is loaded properly

## Integration with Frontend

The frontend module (openimis-fe-social_protection_js) uses these permission constants:
```javascript
export const RIGHT_SECTION_SEARCH = 160005;
export const RIGHT_SECTION_CREATE = 160006;
export const RIGHT_SECTION_UPDATE = 160007;
export const RIGHT_SECTION_DELETE = 160008;
export const RIGHT_INDICATOR_SEARCH = 160009;
export const RIGHT_INDICATOR_CREATE = 160010;
export const RIGHT_INDICATOR_UPDATE = 160011;
export const RIGHT_INDICATOR_DELETE = 160012;
export const RIGHT_INDICATOR_ACHIEVEMENT_SEARCH = 160013;
export const RIGHT_INDICATOR_ACHIEVEMENT_CREATE = 160014;
export const RIGHT_INDICATOR_ACHIEVEMENT_UPDATE = 160015;
export const RIGHT_INDICATOR_ACHIEVEMENT_DELETE = 160016;
```

Make sure both frontend and backend use the same permission numbers.