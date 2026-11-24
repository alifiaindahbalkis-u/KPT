
from pathlib import Path

# Lokasi utama project
BASE_DIR = Path(__file__).resolve().parent.parent

# Konfigurasi Master
MASTER_HOST = "0.0.0.0"
MASTER_PORT = 5000

# Konfigurasi Worker
WORKER_MAX_PROCESSES = 2

# Konfigurasi dataset
DATASET_DIR = BASE_DIR / "dataset" / "input" / "20newsgroups"
METADATA_FILE = DATASET_DIR / "metadata.csv"

# Konfigurasi pemrosesan
DEFAULT_SEARCH_TERM = "computer"
