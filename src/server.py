import heapq
import socket
import json
import sys
from sm import ConcurrentStateMachine
import threading
import queue
import time
import sys 


servers = [("3.143.252.144", 9010), ("3.16.161.143", 9001), ("98.91.209.135",9009)]
TOTAL_SERVERS =  4

request_queue = queue.Queue()

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

port = int(sys.argv[1]) 
origin = port

#T his information will be used only for clients, not servers.
server.bind(('172.31.39.105', port))
server.listen()

clock = 0
clock_lock = threading.Lock()

acks = {}

sm = ConcurrentStateMachine()

# We will create a log file to store the information about the execution
with open("log.txt", "w") as log:
    log.write("New Execution:")
    


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
        
        # Simple review about the operations allowed
        if comando not in ["modificar", "consultar"]:
            conn.close()
            continue
        
        # This clock is not affecting my modifica 
        with clock_lock:
            clock += 1
            time_stamp = clock
			
        msg_id = f"{origin}_{time_stamp}"
        acks[msg_id] = set([origin])

        print("Nuevo request:", msg_id)

        # Here we store the request in a queue so we can send the acks after that
        request_queue.put((time_stamp, id_empleado, nuevo_nombre, comando, conn, msg_id))
        
        # If the command is "modificar", we should let know the other servers about it and send our ack.
        if comando == "modificar":
           
        

            multicast({
                "comando": comando,
                "id_empleado": id_empleado,
                "nuevo_nombre": nuevo_nombre,
                "sender": origin,
                "timestamp": time_stamp,
                "origin": origin,
                "replica": True
                })
            
            multicast({
            "type": "ack",
            "timestamp": time_stamp,
            "origin": origin,
            "sender": origin
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
            s.sendall((json.dumps(msg) + "\n").encode("UTF8"))  #  FIX encode
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
                # Correct unpack of the buffer, in last versions this was failing because of different formats. 
                time_stamp, id_empleado, nuevo_nombre, comando, conn, msg_id = sm.buffer[0]

                # =====================
                # CONSULTAR 
                # =====================
                if comando == "consultar":
                    heapq.heappop(sm.buffer)

                    res = sm.sm.consultar(id_empleado)

                    response = {
                        "id_empleado": id_empleado,
                        "nombre": res if res else None,
                        "status": 0 if res else 1
                    }

                    if conn:
                        try:
                            conn.sendall((json.dumps(response) + "\n").encode("UTF8"))
                            conn.close()
                        except Exception as e:
                            print("Error enviando respuesta:", e)

                    continue

                #print("ACKS:", msg_id, acks.get(msg_id))

                # =====================
                # MODIFICAR
                # =====================
                if len(acks.get(msg_id, set())) >= TOTAL_SERVERS:
                    # Only if it gets the acks of all the servers, executes it. 
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
                            conn.sendall((json.dumps(response) + "\n").encode("UTF8"))

                            conn.close()
                        except Exception as e:
                            print("Error enviando respuesta:", e)

                    # We write in the log already created at the beggining of the program. 
                    with open("log.txt", "a") as log:
                        log.write(msg_id + id_empleado + nuevo_nombre + "\n")

       


def receive_items():
    """
    This function is the responsible of receiving information from
    other servers.
    """
    global clock

    receive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    receive_socket.bind(('172.31.39.105', 5001))
    receive_socket.listen()

    while True:
        print("Esperando mensajes de otros servidores...")
        conn, addr = receive_socket.accept()

        data = b""
        while not data.endswith(b"\n"):
                chunk = conn.recv(4096)
                if not chunk:
                    break
                data += chunk


        try:
            request = json.loads(data.strip())
        except:
            conn.close()
            continue

        sender = request.get("sender")
        time_stamp = request.get("timestamp")
        origin_msg = request.get("origin")
        msg_id = f"{origin_msg}_{time_stamp}"

       
        # CASE 1: ACK
      
        if request.get("type") == "ack":

            msg_id = f"{request['origin']}_{request['timestamp']}"
            sender = request["sender"]

            if msg_id not in acks:
                acks[msg_id] = set()

            acks[msg_id].add(sender)
            print(acks)
            conn.close()    
            continue

     
        # CASE 2: MSG (REPLICA)
      
        comando = request.get("comando")
        id_empleado = request.get("id_empleado")
        nuevo_nombre = request.get("nuevo_nombre")
        time_stamp = request.get("timestamp")

        # Lamport
        with clock_lock:
            clock = max(clock, time_stamp) + 1
            local_time = clock

        print(f"Recibido {msg_id} de {sender}")

        #  Initialize ACKs
        if msg_id not in acks:
            acks[msg_id] = set()

        # agregar ACK propio 
        acks[msg_id].add(origin)
        
        request_queue.put((
            local_time,
            id_empleado,
            nuevo_nombre,
            comando,
            None, 
            msg_id
        ))

        # send ACK ONLY after putting the item in my queue
        multicast({
            "type": "ack",
            "timestamp": time_stamp,
            "origin": request.get("origin"),
            "sender": origin
            })

        conn.close()
                
    

       


if __name__ == "__main__":
    """
    Starting 4 different threads for every function declared in our code (this can be blocked very easly)
    """
    threading.Thread(target=main).start()
    threading.Thread(target=produce_items_from_client, args=(sm,)).start()
    threading.Thread(target=receive_items).start()
    threading.Thread(target=consume_items, args=(sm,)).start()
