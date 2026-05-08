import socket, json

s = socket.socket()
s.connect(("127.0.0.1", 5001))

msg = {
    "comando": "modificar",
    "id_empleado": "E01",
    "nuevo_nombre": "Juan Pro"
}

s.sendall((json.dumps(msg) + "\n").encode())

print(s.recv(1024).decode())