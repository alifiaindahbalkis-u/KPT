
import socket
import threading

from common.config import MASTER_HOST, MASTER_PORT
from common.protocol import send_message, receive_message
from master.task_manager import create_task_queue


# ==========================================
# KONFIGURASI
# ==========================================

MIN_WORKERS = 2

connected_workers = {}
workers_lock = threading.Lock()

task_queue = None
total_tasks = 0
completed_count = 0
results = []

results_lock = threading.Lock()


# ==========================================
# HANDLE WORKER
# ==========================================

def handle_worker(connection, address):
    global completed_count

    worker_id = None

    try:
        print(f"\nKoneksi masuk dari {address}")

        registration = receive_message(connection)

        if not registration or registration.get("type") != "register":
            print("Registrasi tidak valid.")
            return

        worker_id = registration.get("worker_id")

        if not worker_id:
            print("Worker ID tidak ditemukan.")
            return

        with workers_lock:
            connected_workers[worker_id] = {
                "address": address,
                "connection": connection
            }

            worker_count = len(connected_workers)

        print(f"Worker terdaftar: {worker_id}")
        print(f"Worker aktif: {worker_count}/{MIN_WORKERS}")

        send_message(connection, {
            "type": "registered",
            "status": "success"
        })

        while True:

            message = receive_message(connection)

            if message is None:
                break

            message_type = message.get("type")

            # Worker meminta batch tugas
            if message_type == "request_batch":

                with workers_lock:
                    worker_count = len(connected_workers)

                # Tunggu sampai minimal 2 Worker terhubung
                if worker_count < MIN_WORKERS:

                    send_message(connection, {
                        "type": "wait",
                        "message": "Menunggu Worker lainnya"
                    })

                    continue

                batch_size = message.get("batch_size", 2)
                batch = []

                for _ in range(batch_size):
                    try:
                        batch.append(task_queue.get_nowait())
                    except Exception:
                        break

                if batch:

                    send_message(connection, {
                        "type": "task_batch",
                        "tasks": batch
                    })

                    print(
                        f"[BATCH] {worker_id} menerima "
                        f"{len(batch)} tugas"
                    )

                else:

                    send_message(connection, {
                        "type": "no_task"
                    })

            # Worker mengirim hasil
            elif message_type == "result_batch":

                batch_results = message.get("results", [])

                with results_lock:

                    results.extend(batch_results)
                    completed_count += len(batch_results)

                    current_completed = completed_count

                print(
                    f"[RESULT] {worker_id} mengirim "
                    f"{len(batch_results)} hasil "
                    f"({current_completed}/{total_tasks})"
                )

                send_message(connection, {
                    "type": "ack_batch",
                    "status": "success"
                })

                if current_completed == total_tasks:

                    print("\n===================================")
                    print("SELURUH TUGAS BERHASIL DIPROSES")
                    print("===================================")
                    print(f"Total hasil: {len(results)}")

            # Heartbeat
            elif message_type == "heartbeat":

                send_message(connection, {
                    "type": "heartbeat_ack",
                    "status": "alive"
                })

            else:
                print(f"Pesan tidak dikenal: {message}")

    except (ConnectionError, OSError) as error:
        print(f"Koneksi Worker {worker_id} terputus: {error}")

    finally:

        if worker_id:

            with workers_lock:
                connected_workers.pop(worker_id, None)

            print(f"Worker {worker_id} terputus.")

        connection.close()


# ==========================================
# START MASTER
# ==========================================

def start_master():
    global task_queue, total_tasks, completed_count

    task_queue, total_tasks = create_task_queue(limit=1000)

    completed_count = 0
    results.clear()

    server = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server.bind((MASTER_HOST, MASTER_PORT))
    server.listen(10)

    print("\n" + "=" * 45)
    print(" DISTRIBUTED FILE PROCESSING - MASTER")
    print("=" * 45)

    print(f"Master berjalan pada port {MASTER_PORT}")
    print(f"Total tugas: {total_tasks}")
    print(f"Menunggu minimal {MIN_WORKERS} Worker...\n")

    try:

        while True:

            connection, address = server.accept()

            thread = threading.Thread(
                target=handle_worker,
                args=(connection, address),
                daemon=True
            )

            thread.start()

    except KeyboardInterrupt:
        print("\nMaster dihentikan.")

    finally:
        server.close()


if __name__ == "__main__":
    start_master()
