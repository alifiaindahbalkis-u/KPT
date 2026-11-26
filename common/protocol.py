
import json
import threading


# Menyimpan buffer untuk setiap koneksi TCP
_receive_buffers = {}

# Melindungi akses buffer
_buffer_lock = threading.Lock()


def send_message(connection, data):
    """
    Mengirim data JSON melalui TCP Socket.
    Setiap pesan diakhiri newline.
    """

    message = json.dumps(data).encode("utf-8")
    message += b"\n"

    connection.sendall(message)


def receive_message(connection):
    """
    Menerima satu pesan JSON melalui TCP Socket.

    Buffer dipertahankan agar pesan berikutnya
    tidak hilang ketika beberapa pesan diterima
    dalam satu kali pembacaan socket.
    """

    with _buffer_lock:
        if connection not in _receive_buffers:
            _receive_buffers[connection] = bytearray()

        buffer = _receive_buffers[connection]

    while True:

        # Periksa apakah buffer sudah memiliki
        # satu pesan lengkap.
        with _buffer_lock:

            newline_position = buffer.find(b"\n")

            if newline_position != -1:

                message = bytes(buffer[:newline_position])

                # Hapus hanya pesan yang sudah dibaca.
                # Pesan berikutnya tetap disimpan.
                del buffer[:newline_position + 1]

                if not buffer:
                    _receive_buffers.pop(connection, None)

                return json.loads(message.decode("utf-8"))

        # Baca data dari socket.
        chunk = connection.recv(4096)

        # Koneksi terputus.
        if not chunk:

            with _buffer_lock:
                _receive_buffers.pop(connection, None)

            return None

        # Tambahkan data baru ke buffer.
        with _buffer_lock:
            buffer.extend(chunk)
