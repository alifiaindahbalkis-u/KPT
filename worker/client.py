
import socket
import os
import time
import psutil

from multiprocessing import Pool

from common.config import MASTER_PORT, WORKER_MAX_PROCESSES
from common.protocol import send_message, receive_message
from worker.main import process_document


MASTER_ADDRESS = os.getenv("MASTER_ADDRESS", "127.0.0.1")
WORKER_ID = f"WORKER-{socket.gethostname()}"


def process_task(task):
    """
    Memproses satu dokumen menggunakan fungsi
    dari worker.main.
    """

    return process_document(
        task,
        search_term=task.get("search_term")
    )


def get_worker_metrics():
    """
    Mengambil informasi kondisi komputer Worker.
    """

    return {
        "process_count": WORKER_MAX_PROCESSES,
        "cpu_percent": psutil.cpu_percent(interval=None),
        "memory_percent": psutil.virtual_memory().percent,
    }


def start_worker():

    connection = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    total_processed = 0

    # Informasi performa batch sebelumnya.
    last_batch_duration = 0
    last_batch_count = 0
    approval_wait_announced = False

    try:

        print("=" * 45)
        print(f"WORKER: {WORKER_ID}")
        print("=" * 45)

        print("Menghubungkan Worker ke Master...")
        print(f"Alamat Master: {MASTER_ADDRESS}")
        print(f"Port Master: {MASTER_PORT}")

        connection.connect(
            (MASTER_ADDRESS, MASTER_PORT)
        )

        # ----------------------------------
        # REGISTRASI WORKER
        # ----------------------------------

        send_message(connection, {
            "type": "register",
            "worker_id": WORKER_ID
        })

        response = receive_message(connection)

        if (
            not response
            or response.get("type") != "registered"
            or response.get("status") != "success"
        ):

            print("Registrasi Worker gagal.")
            return

        print("\nWorker berhasil terhubung!")
        print(f"Worker ID: {WORKER_ID}")
        print(f"Jumlah proses: {WORKER_MAX_PROCESSES}")

        # ----------------------------------
        # MULTIPROCESSING
        # ----------------------------------

        with Pool(processes=WORKER_MAX_PROCESSES) as pool:

            while True:

                # Mengambil kondisi komputer terkini.
                metrics = get_worker_metrics()

                # ----------------------------------
                # MEMINTA TUGAS KE MASTER
                # ----------------------------------

                send_message(connection, {
                    "type": "request_batch",
                    "worker_id": WORKER_ID,

                    "process_count": metrics["process_count"],
                    "cpu_percent": metrics["cpu_percent"],
                    "memory_percent": metrics["memory_percent"],

                    "last_batch_duration": last_batch_duration,
                    "last_batch_count": last_batch_count,
                })

                response = receive_message(connection)

                if response is None:

                    print("\nKoneksi ke Master terputus.")
                    break

                message_type = response.get("type")

                # ----------------------------------
                # MENUNGGU WORKER LAIN
                # ----------------------------------

                if message_type == "wait":

                    print(
                        "\n[WAIT] Menunggu tugas tersedia..."
                    )

                    time.sleep(1)
                    continue

                elif message_type == "wait_approval":

                    if not approval_wait_announced:
                        print(
                            "\n[APPROVAL] Menunggu persetujuan "
                            "Master sebelum mulai bekerja."
                        )
                        approval_wait_announced = True

                    time.sleep(1)
                    continue

                # ----------------------------------
                # TIDAK ADA TUGAS TERSISA
                # ----------------------------------

                elif message_type == "no_task":

                    print("\nTidak ada tugas tersisa.")
                    print("Worker selesai bekerja.")
                    break

                # ----------------------------------
                # MENERIMA BATCH TUGAS
                # ----------------------------------

                elif message_type == "task_batch":

                    tasks = response.get("tasks", [])

                    if not tasks:
                        continue

                    print(
                        f"\n[BATCH] {WORKER_ID} menerima "
                        f"{len(tasks)} tugas"
                    )

                    print(
                        f"[RESOURCE] CPU: "
                        f"{metrics['cpu_percent']}% | "
                        f"RAM: {metrics['memory_percent']}%"
                    )

                    # Mengukur waktu pemrosesan.
                    start_time = time.perf_counter()

                    batch_results = pool.map(
                        process_task,
                        tasks
                    )

                    last_batch_duration = (
                        time.perf_counter() - start_time
                    )

                    last_batch_count = len(batch_results)

                    # ----------------------------------
                    # MENAMPILKAN HASIL
                    # ----------------------------------

                    for result in batch_results:

                        print(
                            f"[PROCESS] "
                            f"{result['document_id']} "
                            f"- {result['status']}"
                        )

                    # ----------------------------------
                    # MENGIRIM HASIL KE MASTER
                    # ----------------------------------

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

                        print(
                            "Konfirmasi hasil tidak diterima."
                        )

                        break

                    total_processed += len(batch_results)

                    print(
                        f"\n[SUCCESS] {len(batch_results)} hasil "
                        f"dikonfirmasi Master."
                    )

                    print(
                        f"[PERFORMANCE] "
                        f"Durasi: {last_batch_duration:.4f} detik | "
                        f"Throughput: "
                        f"{last_batch_count / last_batch_duration:.2f} "
                        f"tugas/detik"
                    )

                    print(
                        f"Total diproses {WORKER_ID}: "
                        f"{total_processed}"
                    )

                else:

                    print(
                        "Respons tidak dikenal:",
                        response
                    )

                    break

        print("\n===================================")
        print(f"WORKER {WORKER_ID} SELESAI")
        print("===================================")
        print(f"Total dokumen: {total_processed}")

    except ConnectionRefusedError:

        print(
            "\nKoneksi ditolak. Pastikan Master aktif "
            "dan alamat IP sudah benar."
        )

    except KeyboardInterrupt:

        print("\nWorker dihentikan oleh pengguna.")

    except (ConnectionError, OSError) as error:

        print("\nTerjadi kesalahan:", error)

    finally:

        connection.close()
        print("Koneksi Worker ditutup.")


if __name__ == "__main__":
    start_worker()
