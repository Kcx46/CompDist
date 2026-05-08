import heapq
import socket
import json
import sys
from sm import ConcurrentStateMachine
import threading
import queue
import time
import sys 

MY_ID = "S1"
servers = [("192.168.1.78", 5001), ("192.168.1.78", 5002)]
TOTAL_SERVERS =  3

request_queue = queue.Queue()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

port = int(sys.argv[1]) 
origin = port
server.bind(('172.31.39.105', port))
server.listen()

clock = 0
clock_lock = threading.Lock()

acks = {}

sm = ConcurrentStateMachine()


def main():
    """
    This function opens a socket and listens for incoming connections from clients, not servers.
    """
    global clock
    while True:
        print("Esperando conexiones entrantes...")
        conn, addr = server.accept()
        print(f"Conexión aceptada de {addr}")

        data = b""
        while not data.endswith(b"\n"):
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk

        try:
            request = json.loads(data.decode().strip())
        except:
            print("JSON inválido")
            conn.close()
            continue

        comando = request.get("comando")
        id_empleado = request.get("id_empleado")
        nuevo_nombre = request.get("nuevo_nombre")

        if comando not in ["modificar", "consultar"]:
            conn.close()
            continue

        with clock_lock:
            clock += 1
            time_stamp = clock

        msg_id = f"{MY_ID}_{time_stamp}"
        acks[msg_id] = set([MY_ID])

        print("Nuevo request:", msg_id)
        replica = False

        request_queue.put((time_stamp, id_empleado, nuevo_nombre, comando, conn, msg_id))
        if comando == "modificar":
            multicast({
    "comando": comando,
    "id_empleado": id_empleado,
    "nuevo_nombre": nuevo_nombre,
    "sender": MY_ID,
    "id": msg_id,
    "timestamp": time_stamp,
    "origin": MY_ID,
    "replica": True
})
      
		
        


def multicast(msg):
    """
    This function sends a message to all other servers in the cluster.
    Args:
        msg (dict): The message to be sent. It should contain the following fields:
    Returns:
        None
    """

    for srv in servers:
        host, port = srv
        try:
            
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))  #  puerto corregido
            s.sendall((json.dumps(msg) + "\n").encode())  #  FIX encode
        except Exception as e:
            print("Error multicast:", e)
        finally:
            s.close()


def produce_items_from_client(sm):
    """
    This function produce items from the client request so our local machine can consume them.
    Args: sm (ConcurrentStateMachine): The state machine instance to produce items into.
    Returns: None
    """
    while True:
        item = request_queue.get()
        sm.produce(item)


def consume_items(sm):
    while True:
        with sm.lock:
            if not sm.buffer:
                pass
            else:
                # FORMATO CORRECTO 
                time_stamp, id_empleado, nuevo_nombre, comando, conn, msg_id = sm.buffer[0]

                # =====================
                # CONSULTAR (NO CONSENSO)
                # =====================
                if comando == "consultar":
                    heapq.heappop(sm.buffer)

                    res = sm.sm.consultar(id_empleado)

                    response = {
                        "id_empleado": id_empleado,
                        "nombre": res if res else None,
                        "status": 1 if res else 0
                    }

                    if conn:
                        try:
                            conn.sendall((json.dumps(response) + "\n").encode())
                            conn.close()
                        except Exception as e:
                            print("Error enviando respuesta:", e)

                    continue

                print("ACKS:", msg_id, acks.get(msg_id))

                # =====================
                # MODIFICAR (CONSENSO)
                # =====================
                if len(acks.get(msg_id, set())) == TOTAL_SERVERS:
                    heapq.heappop(sm.buffer)

                    print("Consuming:", msg_id)

                    res = sm.sm.modificar(id_empleado, nuevo_nombre)

                    response = {
                        "id_empleado": id_empleado,
                        "nombre": nuevo_nombre,
                        "status": 1 if res else 0
                    }

                    if conn:
                        try:
                            conn.sendall((json.dumps(response) + "\n").encode())
                            conn.close()
                        except Exception as e:
                            print("Error enviando respuesta:", e)

       


def receive_items():
    global clock

    receive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #port to receive from other servers is 9000
    receive_socket.bind(('172.31.39.105', 5000))
    receive_socket.listen()

    while True:
        print("Esperando mensajes de otros servidores...")
        conn, addr = receive_socket.accept()

        data = conn.recv(4096).decode()

        try:
            request = json.loads(data.strip())
        except:
            conn.close()
            continue

        msg_id = request.get("id")
        sender = request.get("sender")

        # =====================
        # CASO 1: ACK
        # =====================
        if request.get("type") == "ACK":
            if msg_id not in acks:
                acks[msg_id] = set()

            acks[msg_id].add(sender)

            conn.close()
            continue

        # =====================
        # CASO 2: MSG (REPLICA)
        # =====================
        comando = request.get("comando")
        id_empleado = request.get("id_empleado")
        nuevo_nombre = request.get("nuevo_nombre")
        time_stamp = request.get("timestamp")

        # Lamport
        with clock_lock:
            clock = max(clock, time_stamp) + 1
            local_time = clock

        print(f"Recibido {msg_id} de {sender}")

        #  inicializar ACKs
        if msg_id not in acks:
            acks[msg_id] = set()

        # agregar ACK propio + sender
        acks[msg_id].add(MY_ID)
        acks[msg_id].add(sender)

        #  meter al queue con FORMATO CORRECTO
        request_queue.put((
            local_time,
            id_empleado,
            nuevo_nombre,
            comando,
            None,  # no hay cliente
            msg_id
        ))

        # enviar ACK
        multicast({
            "type": "ACK",
            "id": msg_id,
            "sender": MY_ID
        })

        conn.close()
                
    

       


if __name__ == "__main__":
    threading.Thread(target=main).start()
    threading.Thread(target=produce_items_from_client, args=(sm,)).start()
    threading.Thread(target=receive_items).start()
    threading.Thread(target=consume_items, args=(sm,)).start()