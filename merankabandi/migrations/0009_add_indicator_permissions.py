# Generated migration for Merankabandi indicator permissions

from django.db import migrations


def add_merankabandi_permissions(apps, schema_editor):
    """
    Add Merankabandi module permissions to the database.
    
    Permission ranges:
    - 160005-160008: Section management
    - 160009-160012: Indicator management 
    - 160013-160016: Indicator achievement management
    """
    db_alias = schema_editor.connection.alias
    
    # Define permissions to be created
    permissions = [
        # Section permissions
        {"right_id": 160005, "right_description": "Search and view sections in results framework"},
        {"right_id": 160006, "right_description": "Create new sections in results framework"},
        {"right_id": 160007, "right_description": "Update existing sections in results framework"},
        {"right_id": 160008, "right_description": "Delete sections from results framework"},
        
        # Indicator permissions
        {"right_id": 160009, "right_description": "Search and view indicators"},
        {"right_id": 160010, "right_description": "Create new indicators"},
        {"right_id": 160011, "right_description": "Update existing indicators"},
        {"right_id": 160012, "right_description": "Delete indicators"},
        
        # Indicator achievement permissions
        {"right_id": 160013, "right_description": "Search and view indicator achievements"},
        {"right_id": 160014, "right_description": "Create new indicator achievements"},
        {"right_id": 160015, "right_description": "Update existing indicator achievements"},
        {"right_id": 160016, "right_description": "Delete indicator achievements"},
    ]
    
    # This is a documentation migration - actual permission creation
    # should be done through OpenIMIS standard permission management
    # or SQL scripts in the database
    print("Merankabandi permissions documented:")
    for perm in permissions:
        print(f"  - {perm['right_id']}: {perm['right_description']}")


def remove_merankabandi_permissions(apps, schema_editor):
    """
    Remove Merankabandi permissions (reverse migration).
    """
    # This is a documentation migration
    print("Merankabandi permissions to be removed: 160005-160016")


class Migration(migrations.Migration):

    dependencies = [
        ('merankabandi', '0001_initial'),  # Update this based on your actual migrations
    ]

    operations = [
        migrations.RunPython(
            add_merankabandi_permissions,
            remove_merankabandi_permissions
        ),
    ]