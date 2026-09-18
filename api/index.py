"""
FastAPI Serverless Function Proxy for Vercel
Re-exports the application and settings from main.py
"""
import os
import sys

# Ensure the root project directory is in the import path
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if root_dir not in sys.path:
    sys.path.insert(0, root_dir)

from main import app, settings, HyperliquidConnector, get_connector

__all__ = ["app", "settings", "HyperliquidConnector", "get_connector"]
