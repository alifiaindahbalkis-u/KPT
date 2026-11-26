
import socket
import threading
import time
import queue

from common.config import (
    MASTER_HOST,
    MASTER_PORT,
    MIN_WORKERS,
    MIN_BATCH_SIZE,
    MAX_BATCH_SIZE,
    DEFAULT_BATCH_SIZE,
    MAX_CPU_THRESHOLD,
    MAX_TASK_RETRIES,
)

from common.protocol import send_message, receive_message
from master.task_manager import create_task_queue


# ==========================================
# DATA MASTER
# ==========================================

connected_workers = {}

workers_lock = threading.Lock()
task_lock = threading.Lock()

task_queue = None
total_tasks = 0
completed_count = 0

results = []
completed_task_ids = set()
in_flight = {}
task_retries = {}

completion_announced = False


# ==========================================
# WORKER METRICS
# ==========================================

def update_worker_metrics(worker_id, message):
    """
    Memperbarui informasi kapasitas dan performa Worker.
    """

    with workers_lock:

        worker = connected_workers.get(worker_id)

        if worker is None:
            return

        process_count = message.get("process_count", 2)
        cpu_percent = message.get("cpu_percent")
        memory_percent = message.get("memory_percent")

        try:
            process_count = max(1, int(process_count))
        except (TypeError, ValueError):
            process_count = 2

        worker["process_count"] = process_count

        if cpu_percent is not None:
            worker["cpu_percent"] = max(
                0, min(100, float(cpu_percent))
            )

        if memory_percent is not None:
            worker["memory_percent"] = max(
                0, min(100, float(memory_percent))
            )

        # Menghitung kecepatan pemrosesan batch terakhir.
        duration = message.get("last_batch_duration", 0)
        batch_count = message.get("last_batch_count", 0)

        try:
            duration = float(duration)
            batch_count = int(batch_count)
        except (TypeError, ValueError):
            duration = 0
            batch_count = 0

        if duration > 0 and batch_count > 0:

            throughput = batch_count / duration

            previous = worker.get("throughput", 0)

            if previous <= 0:
                worker["throughput"] = throughput
            else:
                # Exponential Moving Average.
                worker["throughput"] = (
                    0.7 * previous + 0.3 * throughput
                )


# ==========================================
# DYNAMIC LOAD BALANCING
# ==========================================


def calculate_batch_size(worker_id):
    """
    Menentukan ukuran batch berdasarkan kapasitas Worker:
    jumlah proses, CPU, RAM, dan throughput.
    """

    with workers_lock:

        worker = connected_workers.get(worker_id)

        if worker is None:
            return DEFAULT_BATCH_SIZE

        process_count = worker.get("process_count", 2)
        cpu_percent = worker.get("cpu_percent")
        memory_percent = worker.get("memory_percent")
        throughput = worker.get("throughput", 0)

        worker_speeds = [
            item.get("throughput", 0)
            for item in connected_workers.values()
            if item.get("throughput", 0) > 0
        ]

    batch_size = process_count * 2

    # Penyesuaian penggunaan CPU.
    if cpu_percent is not None:

        if cpu_percent >= MAX_CPU_THRESHOLD:
            batch_size = max(MIN_BATCH_SIZE, batch_size // 2)

        elif cpu_percent < 50:
            batch_size += process_count

    # Penyesuaian penggunaan RAM.
    if memory_percent is not None:

        if memory_percent >= 90:
            batch_size = max(MIN_BATCH_SIZE, batch_size // 2)

        elif memory_percent >= 80:
            batch_size = max(MIN_BATCH_SIZE, batch_size - 1)

    # Penyesuaian berdasarkan throughput relatif.
    if throughput > 0 and worker_speeds:

        average_speed = sum(worker_speeds) / len(worker_speeds)

        if average_speed > 0:

            if throughput > average_speed * 1.2:
                batch_size += 2

            elif throughput < average_speed * 0.8:
                batch_size -= 1

    return max(
        MIN_BATCH_SIZE,
        min(MAX_BATCH_SIZE, batch_size)
    )


# ==========================================
# TASK MANAGEMENT
# ==========================================

def assign_tasks(worker_id, batch_size):
    """
    Mengambil tugas dari antrean dan mencatat
    tugas yang sedang dikerjakan Worker.
    """

    assigned = []

    with task_lock:

        for _ in range(batch_size):

            try:
                task = task_queue.get_nowait()
            except queue.Empty:
                break

            task_id = task["task_id"]

            in_flight[task_id] = {
                "task": task,
                "worker_id": worker_id,
                "assigned_at": time.time(),
            }

            assigned.append(task)

    return assigned


def requeue_worker_tasks(worker_id):
    """
    Mengembalikan tugas yang belum selesai
    ketika Worker terputus.
    """

    returned_count = 0

    with task_lock:

        worker = connected_workers.get(worker_id)

        if worker is None:
            return 0

        assigned_ids = list(worker["assigned_tasks"])

        for task_id in assigned_ids:

            assignment = in_flight.get(task_id)

            if assignment is None:
                continue

            if assignment["worker_id"] != worker_id:
                continue

            if task_id in completed_task_ids:
                continue

            task_queue.put(assignment["task"])
            in_flight.pop(task_id, None)

            returned_count += 1

    return returned_count


# ==========================================
# HANDLE WORKER
# ==========================================

def handle_worker(connection, address):

    global completed_count
    global completion_announced

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
                "connection": connection,
                "process_count": 2,
                "cpu_percent": None,
                "memory_percent": None,
                "throughput": 0,
                "assigned_tasks": set(),
            }

            worker_count = len(connected_workers)

        print(f"Worker terdaftar: {worker_id}")
        print(f"Worker aktif: {worker_count}/{MIN_WORKERS}")

        send_message(connection, {
            "type": "registered",
            "status": "success",
        })

        while True:

            message = receive_message(connection)

            if message is None:
                break

            message_type = message.get("type")

            # ----------------------------------
            # REQUEST BATCH
            # ----------------------------------

            if message_type == "request_batch":

                update_worker_metrics(worker_id, message)

                with workers_lock:
                    worker_count = len(connected_workers)

                if worker_count < MIN_WORKERS:

                    send_message(connection, {
                        "type": "wait",
                        "message": "Menunggu Worker lainnya",
                    })

                    continue

                batch_size = calculate_batch_size(worker_id)

                batch = assign_tasks(worker_id, batch_size)

                if batch:

                    with workers_lock:

                        worker = connected_workers.get(worker_id)

                        if worker:
                            for task in batch:
                                worker["assigned_tasks"].add(
                                    task["task_id"]
                                )

                    send_message(connection, {
                        "type": "task_batch",
                        "tasks": batch,
                        "batch_size": len(batch),
                    })

                    print(
                        f"[LOAD BALANCING] {worker_id} "
                        f"menerima {len(batch)} tugas "
                        f"(CPU: "
                        f"{connected_workers[worker_id]['cpu_percent']})"
                    )

                else:

                    with task_lock:

                        pending_count = len(in_flight)

                        all_completed = (
                            completed_count >= total_tasks
                        )

                    if all_completed:

                        send_message(connection, {
                            "type": "no_task",
                        })

                    elif pending_count > 0:

                        send_message(connection, {
                            "type": "wait",
                            "message": "Menunggu tugas Worker lain selesai",
                        })

                    else:

                        send_message(connection, {
                            "type": "no_task",
                        })

            # ----------------------------------
            # RESULT BATCH
            # ----------------------------------

            elif message_type == "result_batch":

                batch_results = message.get("results", [])
                accepted_count = 0
                retry_count = 0

                with task_lock:

                    for result in batch_results:

                        task_id = result.get("document_id")

                        if not task_id:
                            continue

                        if task_id in completed_task_ids:
                            continue

                        assignment = in_flight.get(task_id)

                        # Hasil harus berasal dari Worker
                        # yang menerima tugas tersebut.
                        if (
                            assignment is None
                            or assignment["worker_id"] != worker_id
                        ):
                            continue

                        in_flight.pop(task_id, None)

                        status = result.get("status")

                        if status == "failed":

                            attempts = task_retries.get(task_id, 0) + 1
                            task_retries[task_id] = attempts

                            if attempts <= MAX_TASK_RETRIES:

                                task_queue.put(
                                    assignment["task"]
                                )

                                retry_count += 1
                                continue

                        completed_task_ids.add(task_id)
                        results.append(result)
                        completed_count += 1
                        accepted_count += 1

                    current_completed = completed_count

                    all_completed = (
                        completed_count >= total_tasks
                    )

                with workers_lock:

                    worker = connected_workers.get(worker_id)

                    if worker:

                        for result in batch_results:

                            task_id = result.get("document_id")

                            if task_id:
                                worker["assigned_tasks"].discard(task_id)

                print(
                    f"[RESULT] {worker_id} mengirim "
                    f"{accepted_count} hasil "
                    f"({current_completed}/{total_tasks})"
                )

                if retry_count:
                    print(
                        f"[RETRY] {retry_count} tugas "
                        f"dimasukkan kembali ke antrean."
                    )

                send_message(connection, {
                    "type": "ack_batch",
                    "status": "success",
                })

                if all_completed and not completion_announced:

                    completion_announced = True

                    print("\n===================================")
                    print("SELURUH TUGAS BERHASIL DIPROSES")
                    print("===================================")
                    print(f"Total hasil: {len(results)}")

            # ----------------------------------
            # HEARTBEAT
            # ----------------------------------

            elif message_type == "heartbeat":

                send_message(connection, {
                    "type": "heartbeat_ack",
                    "status": "alive",
                })

            else:

                print(f"Pesan tidak dikenal: {message}")

    except (ConnectionError, OSError) as error:

        print(
            f"Koneksi Worker {worker_id} terputus: {error}"
        )

    finally:

        if worker_id:

            returned = requeue_worker_tasks(worker_id)

            with workers_lock:
                connected_workers.pop(worker_id, None)

            if returned:
                print(
                    f"[RECOVERY] {returned} tugas "
                    f"dikembalikan dari {worker_id}."
                )

            print(f"Worker {worker_id} terputus.")

        connection.close()


# ==========================================
# START MASTER
# ==========================================

def start_master():

    global task_queue
    global total_tasks
    global completed_count
    global completion_announced

    task_queue, total_tasks = create_task_queue(limit=1000)

    completed_count = 0
    completion_announced = False

    results.clear()
    completed_task_ids.clear()
    in_flight.clear()
    task_retries.clear()
    connected_workers.clear()

    server = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM,
    )

    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1,
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
                daemon=True,
            )

            thread.start()

    except KeyboardInterrupt:

        print("\nMaster dihentikan.")

    finally:

        server.close()


if __name__ == "__main__":
    start_master()
