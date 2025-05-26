# Merankabandi Indicators Fixtures

This directory contains Django fixtures for loading Development and Intermediate indicators data into the Merankabandi module.

## Files

1. **development_intermediate_indicators.json**: Contains sections and indicators
   - 5 sections (4 development + 1 intermediate)
   - 12 indicators distributed across sections

2. **sample_achievements.json**: Contains sample achievement data
   - 9 achievement records for various indicators
   - Includes both numeric and Yes/No achievements

## Usage

### Using the Management Command (Recommended)

```bash
# Load only sections and indicators
python manage.py load_indicators

# Clear existing data before loading
python manage.py load_indicators --clear

# Load sections, indicators, and sample achievements
python manage.py load_indicators --with-achievements

# Clear all and load everything fresh
python manage.py load_indicators --clear --with-achievements
```

### Using Django's loaddata Command

```bash
# Load sections and indicators
python manage.py loaddata merankabandi/fixtures/development_intermediate_indicators.json

# Load sample achievements (requires indicators to be loaded first)
python manage.py loaddata merankabandi/fixtures/sample_achievements.json
```

## Sections Included

### Development Sections
1. **Renforcer les capacités de gestion** (4 indicators)
2. **Renforcer les filets de sécurité** (1 indicator)
3. **Promouvoir l'inclusion productive et l'accès à l'emploi** (1 indicator)
4. **Apporter une réponse immédiate et efficace à une crise ou une urgence éligible** (1 indicator)

### Intermediate Section
5. **Indicateurs intermédiaires** (5 indicators)
   - System implementation (Yes/No)
   - Communication plan (Yes/No)
   - Beneficiary satisfaction (%)
   - Complaints resolution (%)
   - Skills development activities (Yes/No)

## Indicator Types

The fixtures include various types of indicators:
- **Numeric indicators**: With baseline and target values (e.g., number of beneficiaries)
- **Percentage indicators**: For rates and proportions (e.g., satisfaction rate)
- **Boolean indicators**: Yes/No indicators (e.g., system implementation status)

## Frontend Integration

Once loaded, these indicators will automatically appear in:
- **Development Indicators Tab**: Shows all indicators from development sections
- **Intermediate Indicators Tab**: Shows indicators from the intermediate section or matching keywords

Users can:
- View current achievement status with progress bars
- Add new achievements using the "Add" button
- Edit existing achievements using the "Edit" button
- Navigate to full indicator management

## Notes

- User IDs in fixtures default to 1 (admin user)
- Dates use ISO format (YYYY-MM-DD)
- UUIDs are provided but will be regenerated if they conflict
- The fixtures are idempotent - running them multiple times won't create duplicates