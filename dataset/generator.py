
import argparse
import csv
import random
from pathlib import Path

# Lokasi folder dataset/generated
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "generated"

# Kumpulan kategori dan materi dokumen
CATEGORIES = {
    "teknologi": {
        "keywords": [
            "komputer", "internet", "jaringan", "perangkat lunak",
            "keamanan", "data", "teknologi", "digital"
        ],
        "sentences": [
            "Perkembangan teknologi digital mengubah cara manusia bekerja.",
            "Komputer digunakan untuk mengolah dan menyimpan berbagai data.",
            "Internet memungkinkan pertukaran informasi melalui jaringan.",
            "Keamanan informasi diperlukan untuk melindungi data pengguna.",
            "Perangkat lunak membantu menyelesaikan berbagai pekerjaan.",
            "Sistem jaringan menghubungkan perangkat untuk berkomunikasi.",
            "Inovasi teknologi terus berkembang dalam kehidupan masyarakat.",
            "Pengolahan data menjadi bagian penting dalam sistem informasi."
        ]
    },

    "pendidikan": {
        "keywords": [
            "sekolah", "mahasiswa", "pembelajaran", "guru",
            "pendidikan", "kurikulum", "pengetahuan", "akademik"
        ],
        "sentences": [
            "Pendidikan berperan penting dalam meningkatkan kualitas sumber daya manusia.",
            "Mahasiswa memperoleh pengetahuan melalui kegiatan pembelajaran.",
            "Guru membantu siswa memahami materi di lingkungan sekolah.",
            "Kurikulum menjadi pedoman dalam pelaksanaan pendidikan.",
            "Pembelajaran dapat dilakukan secara langsung maupun digital.",
            "Kegiatan akademik membantu mengembangkan kemampuan berpikir.",
            "Pengetahuan dapat diperoleh melalui membaca dan berdiskusi.",
            "Institusi pendidikan menyediakan fasilitas untuk kegiatan belajar."
        ]
    },

    "kesehatan": {
        "keywords": [
            "kesehatan", "dokter", "pasien", "rumah sakit",
            "penyakit", "nutrisi", "olahraga", "pengobatan"
        ],
        "sentences": [
            "Kesehatan merupakan bagian penting dalam kehidupan manusia.",
            "Dokter memberikan pelayanan medis kepada pasien.",
            "Rumah sakit menyediakan fasilitas untuk pemeriksaan kesehatan.",
            "Pencegahan penyakit dapat dilakukan melalui pola hidup sehat.",
            "Nutrisi yang seimbang membantu menjaga kondisi tubuh.",
            "Olahraga secara teratur mendukung kebugaran tubuh.",
            "Pengobatan dilakukan berdasarkan kondisi dan kebutuhan pasien.",
            "Pemeriksaan kesehatan membantu mengetahui kondisi tubuh."
        ]
    },

    "lingkungan": {
        "keywords": [
            "lingkungan", "sampah", "hutan", "polusi",
            "air", "energi", "ekosistem", "daur ulang"
        ],
        "sentences": [
            "Lingkungan yang bersih mendukung kehidupan masyarakat.",
            "Pengelolaan sampah membantu mengurangi pencemaran.",
            "Hutan memiliki peran penting dalam menjaga keseimbangan alam.",
            "Polusi udara dapat memengaruhi kualitas lingkungan.",
            "Penghematan air perlu diterapkan dalam kehidupan sehari-hari.",
            "Energi terbarukan dapat menjadi alternatif sumber energi.",
            "Ekosistem terdiri atas berbagai makhluk hidup dan lingkungannya.",
            "Daur ulang membantu mengurangi jumlah sampah."
        ]
    },

    "ekonomi": {
        "keywords": [
            "ekonomi", "bisnis", "pasar", "investasi",
            "keuangan", "perdagangan", "produksi", "konsumen"
        ],
        "sentences": [
            "Kegiatan ekonomi melibatkan proses produksi dan distribusi.",
            "Bisnis membutuhkan perencanaan untuk mencapai tujuan.",
            "Pasar menjadi tempat terjadinya kegiatan jual beli.",
            "Investasi dapat digunakan untuk mengembangkan usaha.",
            "Pengelolaan keuangan membantu mengatur pengeluaran.",
            "Perdagangan mendukung pertukaran barang dan jasa.",
            "Produksi dilakukan untuk memenuhi kebutuhan masyarakat.",
            "Konsumen menggunakan barang dan jasa sesuai kebutuhannya."
        ]
    }
}


def generate_document(doc_id, category, rng):
    """Membuat satu dokumen teks berdasarkan kategori."""

    category_data = CATEGORIES[category]

    # Pilih beberapa kalimat secara acak
    selected_sentences = rng.sample(
        category_data["sentences"],
        k=len(category_data["sentences"])
    )

    title = f"Dokumen {doc_id} - {category.title()}"

    content = [
        title,
        "",
        f"Kategori: {category.title()}",
        ""
    ]

    # Susun kalimat menjadi beberapa paragraf
    for i in range(0, len(selected_sentences), 2):
        paragraph = " ".join(selected_sentences[i:i + 2])
        content.append(paragraph)
        content.append("")

    return "\n".join(content).strip()


def generate_dataset(total_files, seed):
    """Menghasilkan kumpulan dokumen dan metadata."""

    rng = random.Random(seed)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    categories = list(CATEGORIES.keys())
    metadata = []

    print("Memulai pembuatan dataset...")
    print(f"Jumlah dokumen: {total_files}")

    for i in range(1, total_files + 1):

        # Membagi dokumen secara seimbang ke lima kategori
        category = categories[(i - 1) % len(categories)]

        doc_id = f"DOC-{i:05d}"
        filename = f"dokumen_{i:05d}.txt"

        content = generate_document(doc_id, category, rng)

        file_path = OUTPUT_DIR / filename
        file_path.write_text(content, encoding="utf-8")

        metadata.append({
            "document_id": doc_id,
            "filename": filename,
            "category": category,
            "word_count": len(content.split())
        })

        if i % 100 == 0 or i == total_files:
            print(f"Berhasil membuat {i}/{total_files} dokumen")

    # Simpan metadata seluruh dokumen
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

    print("\n===== DATASET BERHASIL DIBUAT =====")
    print(f"Lokasi       : {OUTPUT_DIR}")
    print(f"Jumlah file  : {total_files}")
    print(f"Jumlah kategori: {len(categories)}")
    print(f"Metadata     : {metadata_path.name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generator dataset dokumen teks"
    )

    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Jumlah dokumen yang akan dibuat"
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Seed untuk menghasilkan dataset yang konsisten"
    )

    args = parser.parse_args()

    if args.count <= 0:
        parser.error("Jumlah dokumen harus lebih dari 0.")

    generate_dataset(args.count, args.seed)
