"""
Centralized paths and settings for the project
"""
from pathlib import Path 

# Root directory
ROOT_DIR = Path(__file__).resolve().parent

# Data directories
DATA_DIR = ROOT_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
VISUALS_DIR = DATA_DIR / "visuals"

