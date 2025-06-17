"""
Enhanced OpenSearch Documents for Materialized Views Self-Service Analytics
Includes comprehensive JSON_ext field dimensions
"""

from django_opensearch_dsl import Document, fields
from django_opensearch_dsl.indices import Index
from django.db import connection
import uuid
from datetime import datetime


# Index for dashboard analytics data
dashboard_analytics_index = Index('dashboard_analytics')
dashboard_analytics_index.settings(
    number_of_shards=1,
    number_of_replicas=0
)
