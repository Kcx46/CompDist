import socket
import json
import threading
import queue
import time
from sm import ConcurrentStateMachine

# =========================
# CONFIG
# =========================
MY_ID = "S1"
SERVERS = ["127.0.0.1"]  # agrega más IPs si tienes más servidores
SERVER_PORT = 9000
CLIENT_PORT = 5001
TOTAL_SERVERS = len(SERVERS) + 1

request_queue = queue.Queue()
acks = {}
clock = 0
clock_lock = threading.Lock()

sm = ConcurrentStateMachine()

# =========================
# MULTICAST
# =========================
def multicast(msg):
    for srv in SERVERS:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((srv, SERVER_PORT))
            s.sendall((json.dumps(msg) + "\n").encode())
            s.close()
        except Exception as e:
            print("Error multicast:", e)

# =========================
# CLIENTES
# =========================
def handle_client():
    global clock

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", CLIENT_PORT))
    server.listen()

    while True:
        conn, _ = server.accept()
        data = conn.recv(4096)

        try:
            req = json.loads(data.decode())
        except:
            conn.close()
            continue

        comando = req.get("comando")
        key = req.get("id_empleado")
        value = req.get("nuevo_nombre")

        with clock_lock:
            clock += 1
            ts = clock

        msg_id = f"{MY_ID}_{ts}"
        acks[msg_id] = set([MY_ID])

        request_queue.put((ts, key, value, comando, conn, msg_id))

        multicast({
            "type": "MSG",
            "id": msg_id,
            "timestamp": ts,
            "sender": MY_ID,
            "comando": comando,
            "id_empleado": key,
            "nuevo_nombre": value
        })

# =========================
# SERVIDORES
# =========================
def receive():
    global clock

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("0.0.0.0", SERVER_PORT))
    s.listen()

    while True:
        conn, _ = s.accept()
        data = conn.recv(4096)

        try:
            req = json.loads(data.decode())
        except:
            conn.close()
            continue

        msg_id = req.get("id")
        sender = req.get("sender")

        if req.get("type") == "ACK":
            if msg_id not in acks:
                acks[msg_id] = set()
            acks[msg_id].add(sender)
            conn.close()
            continue

        # MSG
        ts = req.get("timestamp")
        comando = req.get("comando")
        key = req.get("id_empleado")
        value = req.get("nuevo_nombre")

        with clock_lock:
            clock = max(clock, ts) + 1
            local_ts = clock

        if msg_id not in acks:
            acks[msg_id] = set()

        acks[msg_id].add(MY_ID)
        acks[msg_id].add(sender)

        request_queue.put((local_ts, key, value, comando, None, msg_id))

        multicast({
            "type": "ACK",
            "id": msg_id,
            "sender": MY_ID
        })

        conn.close()

# =========================
# PRODUCER
# =========================
def producer():
    while True:
        item = request_queue.get()
        sm.produce(item)

# =========================
# CONSUMER
# =========================
def consumer():
    while True:
        with sm.lock:
            if not sm.buffer:
                time.sleep(0.01)
                continue

            ts, key, value, comando, conn, msg_id = sm.buffer[0]

            # CONSULTAR
            if comando == "consultar":
                sm.buffer.pop(0)
                res = sm.sm.consultar(key)

                if conn:
                    conn.sendall((json.dumps({"resultado": res}) + "\n").encode())
                    conn.close()
                continue

            # MODIFICAR (CONSENSO)
            if len(acks.get(msg_id, set())) == TOTAL_SERVERS:
                sm.buffer.pop(0)
                sm.sm.modificar(key, value)

                if conn:
                    conn.sendall((json.dumps({"status": "ok"}) + "\n").encode())
                    conn.close()

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    threading.Thread(target=handle_client).start()
    threading.Thread(target=receive).start()
    threading.Thread(target=producer).start()
    threading.Thread(target=consumer).start()