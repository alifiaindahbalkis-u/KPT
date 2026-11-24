
import csv
import queue

from common.config import METADATA_FILE, DEFAULT_SEARCH_TERM


def create_task_queue(limit=1000):
    """
    Membaca metadata dokumen dan memasukkannya
    ke dalam antrean tugas Master.
    """

    task_queue = queue.Queue()

    if not METADATA_FILE.exists():
        raise FileNotFoundError(
            f"File metadata tidak ditemukan: {METADATA_FILE}"
        )

    with open(METADATA_FILE, "r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)

        for index, document in enumerate(reader):
            if limit is not None and index >= limit:
                break

            task = {
                "task_id": document["document_id"],
                "document_id": document["document_id"],
                "filename": document["filename"],
                "category": document["category"],
                "search_term": DEFAULT_SEARCH_TERM
            }

            task_queue.put(task)

    total_tasks = task_queue.qsize()

    print("=" * 40)
    print("TASK QUEUE BERHASIL DIBUAT")
    print("=" * 40)
    print(f"Jumlah tugas: {total_tasks}")

    return task_queue, total_tasks


if __name__ == "__main__":
    tasks, total = create_task_queue()

    print("\nContoh tugas pertama:")
    if not tasks.empty():
        print(tasks.get())
