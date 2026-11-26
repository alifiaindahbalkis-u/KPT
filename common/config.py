
import os
from pathlib import Path

# Lokasi utama project
BASE_DIR = Path(__file__).resolve().parent.parent

# Konfigurasi Master
MASTER_HOST = "0.0.0.0"
MASTER_PORT = 5000

# Jumlah minimal Worker aktif
MIN_WORKERS = int(os.getenv("MIN_WORKERS", "2"))

# Konfigurasi Worker
WORKER_MAX_PROCESSES = int(
    os.getenv("WORKER_MAX_PROCESSES", "2")
)

# Konfigurasi Dynamic Load Balancing
MIN_BATCH_SIZE = 1
MAX_BATCH_SIZE = 16
DEFAULT_BATCH_SIZE = 2

# Batas penggunaan CPU untuk pertimbangan beban
MAX_CPU_THRESHOLD = 85

# Interval pemeriksaan Worker (detik)
WORKER_TIMEOUT = 30

# Maksimal percobaan ulang tugas
MAX_TASK_RETRIES = 3

# Konfigurasi dataset
DATASET_DIR = BASE_DIR / "dataset" / "input" / "20newsgroups"
METADATA_FILE = DATASET_DIR / "metadata.csv"

# Konfigurasi pemrosesan
DEFAULT_SEARCH_TERM = "computer"
