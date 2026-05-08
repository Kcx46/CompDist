import heapq
from http import server
import socket
import json
import socketserver
from sm import ConcurrentStateMachine
import threading
import queue
import sm 



"""
Servidor con sockets para manejar conexiones concurrentes 
El servidor escucha en el puerto 8000 y maneja solicitudes de los clientes para modificar o consultar información de empleados.
El servidor utiliza una máquina de estados concurrente para manejar las solicitudes de manera segura en un entorno multihilo.
"""
#ID de este servidor para manejar acks
MY_ID = "S1" 
servers = ['192.168.101.108'] 
#Num total de servers en el sistema
TOTAL_SERVERS = len(servers) + 1 
request_queue = queue.Queue() #cola para almacenar las solicitudes entrantes de los clientes, esto es para manejar las solicitudes de manera ordenada y evitar problemas de concurrencia.

#primer paso es crear el servidor de sockets para manejar las conexiones concurrentes
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #crea un socket TCP/IP
conection = ('192.168.101.118', 8080) 
server.bind((conection)) #vincula el socket a la dirección y puerto especificados
server.listen(5) #el servidor puede manejar hasta 5 conexiones concurrentes - no estoy seguro de como funciona esto.

#necesitamos agregar un reloj para que el timestamp lo maneje el servidor y no el cliente
clock = 0
#lock para manejar el acceso al reloj, esto es para evitar problemas de concurrencia al actualizar el reloj.
clock_lock = threading.Lock() 

#Diccionario para almacenar los acks recibidos
acks = {} 


sm = ConcurrentStateMachine()

def main()-> None:
	"""
	This function is responsible for accepting clients connections only
	Arguments:
		None
	Returns:
		None
	"""
	while True:
		print("Esperando conexiones entrantes...")
		
		conn, addr = server.accept() #acepta una conexión entrante, es un objeto de tipo socket que representa la conexión con el cliente y una tupla con la dirección del cliente
		print(f"Conexión aceptada de {addr}")
		data = b""
		while not data.endswith(b"\n"):
			chunk = conn.recv(4096) #recibe datos del cliente, esto es bloqueante, es decir, el servidor se bloqueará hasta que reciba datos del cliente
			if not chunk: #si el cliente cierra la conexión, el servidor también cierra la conexión y espera por otra conexión entrante
				print(f"Conexión cerrada por el cliente {addr}")
				
				break
			data += chunk
			try:
				request = json.loads(data.decode().strip()) #intenta decodificar los datos recibidos como JSON, esto es para manejar las solicitudes de los clientes de manera estructurada, y para evitar problemas de formato en las solicitudes.
				
			except json.JSONDecodeError:
				print(f"Datos recibidos de {addr} no son un JSON válido: {data.decode().strip()}")
				conn.close() #cierra la conexión con el cliente si los datos recibidos no son un JSON válido, esto es para evitar problemas de formato en las solicitudes y para liberar recursos.
				continue


		time_stamp = request.get("timestamp", 0) #Implementando el algoritmo de Lamport.
		comando = request.get("comando") #obtenemos el comando del JSON.
		id_empleado = request.get("id_empleado") #obtenemos el id_empleado del JSON, esto siempre es requerido.
		nuevo_nombre = request.get("nuevo_nombre") #obtenemos el nuevo_nombre del JSON, puede ser None si el comando no lo requiere.
		if comando not in ["modificar", "consultar"]: #esto puede funcionar sin problemas. 
			print("Operación no válida.")
			conn.close()
		else:
			global clock #necesitamos modificarlo globalmente
			with clock_lock:
				clock += 1 #incrementamos el reloj del servidor cada vez que recibimos una solicitud

				#actualizamos el valor del timestamp
				time_stamp = clock
			
			#creamos un id para la solicitud, esto es para manejar los acks de manera más fácil, ya que cada solicitud tendrá un id único.
			msg_id = f"{MY_ID}_{time_stamp}" 

			#Ack propio
			acks[msg_id] = set([MY_ID])
			
			#we need to add the received item to a request queue, if not
			#our server will not be able to handle concurrent request properly
			request_queue.put((time_stamp,conn,id_empleado,nuevo_nombre,comando,msg_id)) 

			# X. Once we got the item, we need to send it to other servers and we don't have to process it
			#until we receive it back with the updated timestamp.
			multicast({"type": "MSG", "id": msg_id, "timestamp": time_stamp, "sender": MY_ID, "comando": comando, 
			  "id_empleado": id_empleado, "nuevo_nombre": nuevo_nombre}) #we need to send the ack to other servers to let them know that we have received the item and we have updated our clock, this is important for the stability of the item in the system, as an item is considered stable when we have received the ack from all servers, and we don't want to consume an item that is not stable, because it may cause inconsistencies in the system.

def multicast(msg) -> None:
	"""
	This function is responsible for sending titems (which were received from clients)
	to other servers.
	Arguments: 
		item: a tuple containing the information of the request: (timestamp, id_empleado, nuevo_nombre, comando)
	Returns:
		None
	"""
	for server in servers: #iteramos sobre la lista de servidores para enviar el item a cada uno de ellos
		try:
			# 1. Crea un socket TCP/IP para enviar datos a otros servidores. Debe ser un socket diferente por cada servidor a enviar. 
			client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #crea un socket TCP/IP para enviar datos a otros servidores
			client_socket.connect((server, 9000)) #conecta al servidor en la dirección IP y puerto especificados
			# 2. Envía el item como JSON al servidor
			client_socket.sendall(json.dumps(msg) + "\n".encode()) #envía el item como JSON al servidor
		except Exception as e:
			print(f"Error al enviar item a {server}: {e}")
		finally:
			client_socket.close() #cierra el socket después de enviar el item

def produce_items_from_client(sm: ConcurrentStateMachine) -> None:
	"""
	Esta funcion deberá encargarse de consumir los datos de los clientes y agregarlos a la cola de solcitudes para que 
	el hilo consumidor pueda procesarlos. La estructura ya debería ordenarlos de manera consistente.
	Tambien eliminaremos la parte del pasado proyecto en donde si la solicitud viene de la misma maquina, se procesaba directamente, 
	ahora todo ira a la cola de prioridad.
	Argumentos:
		sm: la máquina de estados concurrente que se utilizará para procesar las solicitudes.
	Returns:
		None

	"""
	while True:
		time_stamp, conn, id_empleado, nuevo_nombre, comando, msg_id = request_queue.get() #obtiene una solicitud de la cola de solicitudes, esto es bloqueante, es decir, si no hay solicitudes en la cola, el hilo se bloqueará hasta que haya una solicitud disponible.
		sm.produce((time_stamp, id_empleado, nuevo_nombre, comando, conn, msg_id)) #agrega la solicitud a la máquina de estados concurrente para que sea procesada por el hilo consumidor.
	
def consume_items(sm: ConcurrentStateMachine) -> None:
	"""
	Esta función se encargará de consumir los datos de la máquina de estados concurrente y procesarlos, es decir, 
	modificar o consultar la información de los empleados según el comando recibido.
	Argumentos:
		sm: la máquina de estados concurrente que se utilizará para procesar las solicitudes.
	Returns:
		None

	"""
	while True:
		with sm.lock:
			if not sm.buffer: #si la cola de la máquina de estados concurrente está vacía, no hacemos nada, esto es para evitar que el hilo consumidor se bloquee al intentar consumir de una cola vacía.
				continue
			item = sm.buffer[0] #obtenemos el item con el timestamp más pequeño, esto es para mantener el orden de las solicitudes según su timestamp, y evitar problemas de concurrencia al procesar las solicitudes.
			time_stamp, id_empleado, nuevo_nombre, comando, conn, msg_id = item #desempaquetamos el item para obtener la información de la solicitud.
			if len(acks.get(msg_id, set())) == TOTAL_SERVERS: #si hemos recibido el ack de todos los servidores, entonces podemos consumir el item, esto es para mantener la estabilidad del item en el sistema, ya que un item es considerado estable cuando hemos recibido el ack de todos los servidores, y no queremos consumir un item que no es estable, porque puede causar inconsistencias en el sistema.
				heapq.heappop(sm.buffer) #eliminamos el item de la cola de la máquina de estados concurrente, esto es para evitar que el hilo consumidor intente consumir el mismo item varias veces, y para mantener el orden de las solicitudes según su timestamp.
				if comando == "modificar":
					print(f"Consuming: {time_stamp},{id_empleado}, {nuevo_nombre}, {comando}")
					res = sm.sm.modificar(id_empleado, nuevo_nombre) #modificamos la información del empleado según el id_empleado y el nuevo_nombre recibidos en la solicitud, esto es para procesar la solicitud de modificación de manera correcta.
					print(f"Consumed: {time_stamp},{id_empleado}, {nuevo_nombre}, {comando}")
					response = {
						"id_empleado": id_empleado,
						"nombre": nuevo_nombre,
						"status": 1 if res else 0
					}
					conn.sendall(json.dumps(response) + "\n".encode()) #enviamos una respuesta al cliente con el resultado de la operación, esto es para informar al cliente si la operación fue exitosa o no.
		
					conn.close() #cerramos la conexión con el cliente después de enviar la respuesta, esto es para liberar recursos y evitar que el cliente intente enviar más solicitudes por la misma conexión.
				elif comando == "consultar":
					print(f"Consuming: {time_stamp},{id_empleado}, {nuevo_nombre}, {comando}")
					res = sm.sm.consultar(id_empleado) #consultamos la información del empleado según el id_empleado recibido en la solicitud, esto es para procesar la solicitud de consulta de manera correcta.
					print(f"Consumed: {time_stamp},{id_empleado}, {nuevo_nombre}, {comando}")
					response = {
						"id_empleado": id_empleado,
						"nombre": res if res else None,
						"status": 1 if res else 0
					}
					conn.sendall(json.dumps(response) + "\n".encode()) #enviamos una respuesta al cliente con el resultado de la operación, esto es para informar al cliente si la operación fue exitosa o no.
					conn.close() #cerramos la conexión con el cliente después de enviar la respuesta, esto es para liberar recursos y evitar que el cliente intente enviar más solicitudes por la misma conexión.

def receive_items()-> None:
	"""
	The main functionality of this function is to receive items only from other servers,
	these items will be produced with a different timestamp (ts = max(local_clock, received_timestamp) + 1) 
	to maintain the consistency of the system according to Lamport's algorithm.
	It's pretty much the same as the main function but instead of receiving items from clients, 
	it receives items from other servers and produces them in the concurrent state machine for them to be 
	processed by the consumer thread.
	Arguments:
		None
	Returns:
		None
	"""
	global clock
	#we create a socket to receive items from other servers
	receive_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	#bind it to a different port than we use for clients
	receive_socket.bind(('192.168.101.118', 5050))
	#listen for incoming connections from other servers
	receive_socket.listen(5)
	while True:
		print("Esperando conexiones entrantes de otros servidores...")
		conn, addr = receive_socket.accept() #accept a connection from another server
		print(f"Conexión aceptada de {addr}")
		data = conn.recv(1024).decode() #receive data from the other server
		print(f"Datos recibidos de otro servidor: {data}")
		request = json.loads(data) #try to load the data as JSON

		time_stamp = request.get("timestamp") #get the timestamp from the received item
		comando = request.get("comando") #get the command from the received item
		id_empleado = request.get("id_empleado") #get the id_empleado from the received item
		nuevo_nombre = request.get("nuevo_nombre") #get the nuevo_nombre from the received item
		msg_id = request.get("id") #get the msg_id from the received item
		if request.get("type") == "ACK": #if the received item is an ack, we need to update our acks dictionary
			with clock_lock:
				clock = max(clock, time_stamp) + 1 #update the local clock according to Lamport's algorithm
			if msg_id not in acks:
				acks[msg_id] = set()
			acks[msg_id].add(request.get("sender")) #add the sender of the ack to the acks of the item with the received msg_id
			
		elif request.get("type") == "MSG": #if the received item is a message, we need to produce it in the concurrent state machine for it to be processed by the consumer thread, but we need to update its timestamp according to Lamport's algorithm before producing it, and we also need to send an ack to other servers to let them know that we have received the item and we have updated our clock, this is important for the stability of the item in the system, as an item is considered stable when we have received the ack from all servers, and we don't want to consume an item that is not stable, because it may cause inconsistencies in the system.
			msg_id = request.get("id") #get the msg_id from the received item
			sender = request.get("sender") #get the sender from the received item
			with clock_lock:
				clock = max(clock, time_stamp) + 1 #update the local clock according to Lamport's algorithm
			if msg_id not in acks:
				acks[msg_id] = set()
			acks[msg_id].add(MY_ID) #add our own id to the acks of the received item
			acks[msg_id].add(sender) #add the sender of the item to the acks of the received item
			item = (clock, id_empleado, nuevo_nombre, comando, conn, msg_id) #create a new item with the updated timestamp
			request_queue.put(item) #put the item in the request queue to be processed by the consumer thread
			multicast({"type": "ACK", "timestamp": clock, "id": msg_id, "sender": MY_ID}) #we need to send the ack to other servers to let them know that we have received the item and we have updated our clock, this is important for the stability of the item in the system, as an item is considered stable when we have received the ack from all servers, and we don't want to consume an item that is not stable, because it may cause inconsistencies in the system.
		conn.close()
	
if __name__=="__main__":
	hilo_principal = threading.Thread(target=main) #crea un hilo para ejecutar la función main
	hilo_principal.start() #inicia el hilo para ejecutar la función main
	
	hilo_productor = threading.Thread(target=produce_items_from_client, args=(sm,)) #crea un hilo para producir los items para la máquina de estados concurrente
	hilo_productor.start() #inicia el hilo para producir los items para la máquina de estados concurrente

	hilo_receptor = threading.Thread(target=receive_items) #crea un hilo para recibir items de otros servidores
	hilo_receptor.start() #inicia el hilo para recibir items de otros servidores

	hilo_consumidor = threading.Thread(target=consume_items, args=(sm,)) #crea un hilo para consumir los items de la máquina de estados concurrente
	hilo_consumidor.start() #inicia el hilo para consumir los items de la máquina de estados concurrente


