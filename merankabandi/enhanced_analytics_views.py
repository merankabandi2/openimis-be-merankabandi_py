"""
Enhanced Backend API endpoints for self-service analytics and data exploration
Includes comprehensive JSON_ext field dimensions
"""

from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.permissions import IsAuthenticated
from django.http import HttpResponse, JsonResponse
from django.db import connection
from django.conf import settings
import csv
import json
import pandas as pd
from io import StringIO, BytesIO
import datetime
from typing import Dict, List, Any