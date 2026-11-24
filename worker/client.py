
import socket
import os
import time
from multiprocessing import Pool

from common.config import MASTER_PORT, WORKER_MAX_PROCESSES
from common.protocol import send_message, receive_message
from worker.main import process_document


MASTER_ADDRESS = os.getenv("MASTER_ADDRESS", "127.0.0.1")
WORKER_ID = f"WORKER-{socket.gethostname()}"
BATCH_SIZE = 2


def process_task(task):
    return process_document(
        task,
        search_term=task.get("search_term")
    )


def start_worker():

    connection = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    try:

        print("=" * 40)
        print(f"WORKER: {WORKER_ID}")
        print("=" * 40)

        print("Menghubungkan Worker ke Master...")

        connection.connect(
            (MASTER_ADDRESS, MASTER_PORT)
        )

        send_message(connection, {
            "type": "register",
            "worker_id": WORKER_ID
        })

        response = receive_message(connection)

        if not response or response.get("status") != "success":
            print("Registrasi Worker gagal.")
            return

        print("Worker berhasil terhubung!")
        print(f"Worker ID: {WORKER_ID}")
        print(f"Jumlah proses: {WORKER_MAX_PROCESSES}")

        total_processed = 0

        with Pool(processes=WORKER_MAX_PROCESSES) as pool:

            while True:

                send_message(connection, {
                    "type": "request_batch",
                    "worker_id": WORKER_ID,
                    "batch_size": BATCH_SIZE
                })

                response = receive_message(connection)

                if response is None:
                    print("Koneksi ke Master terputus.")
                    break

                message_type = response.get("type")

                # Menunggu Worker lainnya
                if message_type == "wait":

                    print(
                        "[WAIT] Menunggu Worker lainnya..."
                    )

                    time.sleep(1)
                    continue

                # Tidak ada tugas tersisa
                elif message_type == "no_task":

                    print("\nTidak ada tugas tersisa.")
                    print("Worker selesai bekerja.")
                    break

                # Menerima batch tugas
                elif message_type == "task_batch":

                    tasks = response.get("tasks", [])

                    if not tasks:
                        break

                    print(
                        f"\n[BATCH] {WORKER_ID} menerima "
                        f"{len(tasks)} dokumen"
                    )

                    batch_results = pool.map(
                        process_task,
                        tasks
                    )

                    for result in batch_results:

                        print(
                            f"[PROCESS] "
                            f"{result['document_id']} "
                            f"- {result['status']}"
                        )

                    send_message(connection, {
                        "type": "result_batch",
                        "worker_id": WORKER_ID,
                        "results": batch_results
                    })

                    acknowledgment = receive_message(connection)

                    if (
                        not acknowledgment
                        or acknowledgment.get("type") != "ack_batch"
                    ):

                        print("Konfirmasi hasil tidak diterima.")
                        break

                    total_processed += len(batch_results)

                    print(
                        f"[SUCCESS] {len(batch_results)} hasil "
                        f"diterima Master."
                    )

                    print(
                        f"Total diproses {WORKER_ID}: "
                        f"{total_processed}"
                    )

                else:

                    print("Respons tidak dikenal:", response)
                    break

        print("\n===================================")
        print(f"WORKER {WORKER_ID} SELESAI")
        print("===================================")
        print(f"Total dokumen: {total_processed}")

    except ConnectionRefusedError:
        print("Koneksi ditolak. Pastikan Master aktif.")

    except KeyboardInterrupt:
        print("\nWorker dihentikan.")

    except (ConnectionError, OSError) as error:
        print("Terjadi kesalahan:", error)

    finally:
        connection.close()
        print("Koneksi Worker ditutup.")


if __name__ == "__main__":
    start_worker()
