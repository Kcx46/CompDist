import socket
import json #importamos json para manejar formato JSON.
import threading
"""
Cliente para conectarse al servidor de sockets y enviar mensajes.
"""


def main():
	
	print("Conectando...")
	client = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #crea un socket TCP/IP
	client.connect(('192.168.101.118', 8080)) #conecta al servidor en la dirección IP y puerto especificados
	#client.sendall(b"x,10,set") #envía un mensaje al servidor con la clave "x", el valor 10 y la operación "set"
	#la b delante de la cadena indica que es un byte string, necesario para enviar datos a través de sockets
	"""
	un byte string es una secuencia de bytes, que es la forma en que los datos se representan en la memoria.
	Al enviar datos a través de sockets, es necesario convertir las cadenas de texto en byte strings para que puedan ser transmitidos correctamente.
	"x,10,set" seria algo asi: "0x78,0x2c,0x31,0x30,0x2c,0x73,0x65,0x74"
	que al mismo tiempo cada hexadecimal representa un caracter en ASCII, por ejemplo:
	0x78 = 'x'
	"""
	#enviamos un mensaje JSON al servidor
	message = {
		"comando": "consultar",
		"id_empleado": "E00",
		"nuevo_nombre": "John Doe"
	}
	client.sendall(json.dumps(message).encode())

	response = client.recv(1024).decode() #espera una respuesta del servidor (hasta 1024 bytes)
	response = json.loads(response) #cargamos la respuesta como JSON
	id_empleado = response.get("id_empleado") #obtenemos el id_empleado de la respuesta del servidor
	nombre = response.get("nombre") #obtenemos el nombre de la respuesta del servidor
	status = response.get("status") #obtenemos el status de la respuesta del servidor, esto puede ser 0 o 1 dependiendo de si la operación fue exitosa o no.	
	
	print(f"Respuesta del servidor: id_empleado={id_empleado}, nombre={nombre}, status={status}")
	client.close() #cierra la conexión con el servidor
	

if __name__=="__main__":
	main()
