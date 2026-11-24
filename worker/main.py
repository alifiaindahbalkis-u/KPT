
import csv
import re
from pathlib import Path
from collections import Counter


# Lokasi utama proyek
BASE_DIR = Path(__file__).resolve().parent.parent

# Lokasi dataset
DATASET_DIR = BASE_DIR / "dataset" / "input" / "20newsgroups"
METADATA_FILE = DATASET_DIR / "metadata.csv"


def load_metadata():
    """
    Membaca metadata dokumen dari CSV.
    """
    documents = []

    with open(METADATA_FILE, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            documents.append(row)

    return documents


def process_document(document, search_term=None):
    """
    Memproses satu dokumen teks.

    Operasi:
    1. Membaca dokumen.
    2. Menghitung jumlah kata.
    3. Menghitung frekuensi kata.
    4. Mencari kata tertentu.
    5. Mengambil kategori dokumen.
    """

    filename = document["filename"]
    document_id = document["document_id"]
    category = document["category"]

    file_path = DATASET_DIR / filename

    result = {
        "document_id": document_id,
        "filename": filename,
        "category": category,
        "status": "success",
        "word_count": 0,
        "search_term": search_term,
        "search_count": 0,
        "word_frequency": {}
    }

    try:
        # Membaca isi dokumen
        with open(file_path, "r", encoding="utf-8", errors="replace") as file:
            content = file.read()

        # Memecah teks menjadi kata
        words = re.findall(r"\b\w+\b", content.lower())

        # Menangani dokumen kosong
        if not words:
            result["status"] = "empty"
            return result

        # Menghitung jumlah kata
        result["word_count"] = len(words)

        # Menghitung frekuensi setiap kata
        frequency = Counter(words)

        # Mengambil 10 kata paling sering muncul
        result["word_frequency"] = dict(frequency.most_common(10))

        # Mencari kata tertentu
        if search_term:
            search_words = re.findall(
                r"\b\w+\b",
                search_term.lower()
            )

            result["search_count"] = sum(
                frequency[word] for word in search_words
            )

        return result

    except OSError as error:
        result["status"] = "failed"
        result["error"] = str(error)

        return result


def group_documents(results):
    """
    Mengelompokkan dokumen berdasarkan kategori.
    """
    groups = {}

    for result in results:
        category = result["category"]

        if category not in groups:
            groups[category] = []

        groups[category].append(result["filename"])

    return groups


if __name__ == "__main__":

    print("===== WORKER DOCUMENT PROCESSING =====")

    # Membaca metadata
    documents = load_metadata()

    print(f"Total dokumen tersedia: {len(documents)}")

    # Menguji 5 dokumen pertama
    test_documents = documents[:5]

    results = []

    for document in test_documents:
        result = process_document(
            document,
            search_term="computer"
        )

        results.append(result)

        print("\n------------------------------")
        print(f"ID       : {result['document_id']}")
        print(f"File     : {result['filename']}")
        print(f"Kategori : {result['category']}")
        print(f"Status   : {result['status']}")
        print(f"Jumlah kata: {result['word_count']}")
        print(f"Kata 'computer': {result['search_count']}")

        print("Frekuensi kata:")
        print(result["word_frequency"])

    # Menguji pengelompokan dokumen
    groups = group_documents(results)

    print("\n===== HASIL GROUPING =====")

    for category, filenames in groups.items():
        print(f"{category}: {len(filenames)} dokumen")

    print("\nPengujian Worker selesai.")
