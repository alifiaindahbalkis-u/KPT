
import time

from main import load_metadata, process_document, group_documents


def run_sequential(limit=1000):

    print("\n===== SEQUENTIAL PROCESSING =====")

    documents = load_metadata()[:limit]

    print(f"Total dokumen: {len(documents)}")
    print("Jumlah proses: 1")

    start_time = time.perf_counter()

    results = []

    for document in documents:
        result = process_document(
            document,
            search_term="computer"
        )

        results.append(result)

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

    throughput = len(results) / execution_time

    print("\n===== PERFORMA =====")
    print(f"Waktu eksekusi : {execution_time:.4f} detik")
    print(f"Throughput     : {throughput:.2f} dokumen/detik")

    return {
        "processes": 1,
        "documents": len(results),
        "execution_time": execution_time,
        "throughput": throughput
    }


if __name__ == "__main__":
    run_sequential(limit=10000)
