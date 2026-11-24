
import json

def send_message(connection, data):
    """
    Mengirim data JSON melalui TCP Socket.
    """
    message = json.dumps(data).encode("utf-8")
    message += b"\n"
    connection.sendall(message)


def receive_message(connection):
    """
    Menerima data JSON dari TCP Socket.
    """
    buffer = b""

    while b"\n" not in buffer:
        chunk = connection.recv(4096)

        if not chunk:
            return None

        buffer += chunk

    message, _, remaining = buffer.partition(b"\n")

    return json.loads(message.decode("utf-8"))
