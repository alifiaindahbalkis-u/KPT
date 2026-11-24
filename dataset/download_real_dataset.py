
import csv
from pathlib import Path
from sklearn.datasets import fetch_20newsgroups

# Lokasi penyimpanan dataset
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "input" / "20newsgroups"

def download_dataset():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Mengunduh dataset 20 Newsgroups...")
    print("Proses ini membutuhkan koneksi internet.")

    dataset = fetch_20newsgroups(
        subset="all",
        remove=("headers", "footers", "quotes"),
        data_home=str(BASE_DIR / "cache")
    )

    metadata = []

    print(f"Total dokumen: {len(dataset.data)}")

    for i, (content, target) in enumerate(
        zip(dataset.data, dataset.target), start=1
    ):
        category = dataset.target_names[target]

        filename = f"news_{i:05d}.txt"
        file_path = OUTPUT_DIR / filename

        # Simpan dokumen teks
        file_path.write_text(
            content,
            encoding="utf-8",
            errors="replace"
        )

        metadata.append({
            "document_id": f"NEWS-{i:05d}",
            "filename": filename,
            "category": category,
            "word_count": len(content.split())
        })

        if i % 1000 == 0 or i == len(dataset.data):
            print(f"Berhasil menyimpan {i}/{len(dataset.data)} dokumen")

    # Simpan metadata
    metadata_path = OUTPUT_DIR / "metadata.csv"

    with open(metadata_path, "w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "document_id",
                "filename",
                "category",
                "word_count"
            ]
        )

        writer.writeheader()
        writer.writerows(metadata)

    print("\n===== DOWNLOAD BERHASIL =====")
    print(f"Total dokumen : {len(dataset.data)}")
    print(f"Jumlah kategori: {len(dataset.target_names)}")
    print(f"Lokasi: {OUTPUT_DIR}")


if __name__ == "__main__":
    download_dataset()

