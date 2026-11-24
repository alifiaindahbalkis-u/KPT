
import time
import os
import multiprocessing

from main import load_metadata, process_document, group_documents


def worker_task(document):
    """
    Fungsi yang dijalankan oleh setiap proses.
    """
    return process_document(
        document,
        search_term="computer"
    )


def run_parallel(num_processes=4, limit=1000):
    """
    Memproses dokumen menggunakan multiprocessing.
    """

    print("\n===== PARALLEL PROCESSING =====")

    documents = load_metadata()[:limit]

    print(f"Total dokumen : {len(documents)}")
    print(f"Jumlah proses : {num_processes}")

    start_time = time.perf_counter()

    with multiprocessing.Pool(processes=num_processes) as pool:
        results = pool.map(worker_task, documents)

    end_time = time.perf_counter()

    execution_time = end_time - start_time

    success = sum(
        result["status"] == "success"
        for result in results
    )

    empty = sum(
        result["status"] == "empty"
        for result in results
    )

    failed = sum(
        result["status"] == "failed"
        for result in results
    )

    groups = group_documents(results)

    print("\n===== HASIL PEMROSESAN =====")
    print(f"Berhasil       : {success}")
    print(f"Dokumen kosong : {empty}")
    print(f"Gagal          : {failed}")

    print("\n===== HASIL GROUPING =====")

    for category, filenames in groups.items():
        print(f"{category}: {len(filenames)} dokumen")

    print("\n===== PERFORMA =====")
    print(f"Waktu eksekusi : {execution_time:.4f} detik")
    print(f"Throughput     : {len(results) / execution_time:.2f} dokumen/detik")

    return {
        "processes": num_processes,
        "documents": len(results),
        "execution_time": execution_time,
        "throughput": len(results) / execution_time
    }


if __name__ == "__main__":
    multiprocessing.freeze_support()
    print("Jumlah CPU:", os.cpu_count())
    run_parallel(num_processes=6, limit=10000)
