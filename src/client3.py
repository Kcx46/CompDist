import socket
import json
import time

HOST = "18.225.167.72"
PORT = 8000


def enviar_mensaje(mensaje):
    with socket.create_connection((HOST, PORT), timeout=3) as s:
        # Enviar JSON con delimitador newline
        s.sendall((json.dumps(mensaje) + "\n").encode("utf-8"))

        # Leer hasta recibir newline
        data = b""
        while not data.endswith(b"\n"):
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk

    return json.loads(data.decode("utf-8").strip())


def consultar(id_empleado):
    mensaje = {
        "comando": "consultar",
        "id_empleado": id_empleado
    }
    return enviar_mensaje(mensaje)


def modificar(id_empleado, nuevo_nombre):
    mensaje = {
        "comando": "modificar",
        "id_empleado": id_empleado,
        "nuevo_nombre": nuevo_nombre
    }
    return enviar_mensaje(mensaje)


# Ejemplo de uso
if __name__ == "__main__":
    print(HOST, PORT)
  
    print(modificar("E03", "Francisco Gomez"))
 

   
   