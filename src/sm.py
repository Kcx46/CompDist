import threading
from ops import StateMachine
import heapq
import time

class ConcurrentStateMachine:
	def __init__(self):
		self.sm = StateMachine()
		self.IsBufferFull = threading.Semaphore(100)
		self.IsThereItemsToConsume = threading.Semaphore(0)
		self.lock = threading.Lock()
		self.buffer = []

	def consume(self):
		self.IsThereItemsToConsume.acquire()
		
		with self.lock:
			#key, value, operation = self.buffer.pop(0) esta línea es para cuando tengamos mas operaciones, por ahora solo tenemos modificar y consultar, pero luego tendremos get, set, add, mult, etc.
			#esto debería consumir el item con el timestamp mas pequeño
			comando, id_empleado, nuevo_nombre, MY_ID, msg_id, time_stamp, origin, replica, conn = heapq.heappop(self.buffer)
			
		self.IsBufferFull.release()
		"""
		***** ESTA PARTE SERA CUANDO TENGAMOS MAS OPERACIONES******
		if operation == "get":
			value = self.sm.read(key)
		elif operation in ["set", "add", "mult"]:
			print(f"Consuming: {key}, {value}, {operation}")
			self.sm.update(key,value,operation)
			print(f"Consumed: {key}, {value}, {operation}")
		"""
		if comando == "modificar":
			print(f"Consuming: {time_stamp},{id_empleado}, {nuevo_nombre}, {comando}")
			res = self.sm.modificar(id_empleado, nuevo_nombre)
			print(f"Consumed: {time_stamp},{id_empleado}, {nuevo_nombre}, {comando}")
			return (time_stamp, res, nuevo_nombre, comando, msg_id)
			
		elif comando == "consultar":
			print(f"Consuming: {time_stamp},{id_empleado}, {nuevo_nombre}, {comando}")
			res=self.sm.consultar(id_empleado)
			print(f"Consumed: {time_stamp},{id_empleado}, {nuevo_nombre}, {comando}")
			return (time_stamp, res, nuevo_nombre, comando, msg_id)
		else:
			print("Operación no válida.")
			return None, None, None, None, None, None
		


	def produce(self, item: tuple) -> None:
		self.IsBufferFull.acquire()
		with self.lock:
			heapq.heappush(self.buffer, item) 
			#item tiene una forma de time_stamp, id_empleado, nuevo_nombre, comando,
		self.IsThereItemsToConsume.release()
		print(f"Produced: {item}")
	
	def consume_consultar(self, id_empleado):
		with self.lock:
			res = self.sm.consultar(id_empleado) #Me gustaría que en lugar de bloquearse la maquina de estados, solo se bloqueara el acceso a ese dato en particular. 
		return res
		


		

