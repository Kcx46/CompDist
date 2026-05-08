class StateMachine:
	def __init__(self):
		self.data = {
			"E00": "Speedy González",
            "E01": "Juan Charrasqueado",
            "E02": "María Bonita",
            "E03": "Carlos Café",
            "E04": "Lupe Vélez",
            "E05": "Pedro Infante",
            "E06": "Jorge Negrete",
            "E07": "Javier Solís",
            "E08": "Lola Beltrán",
            "E09": "Tin Tan",
            "E10": "Cantinflas",
            "E11": "Dolores del Río",
            "E12": "Sara García",
            "E13": "Emilio 'El Indio' Fernández",
            "E14": "Joaquín Pardavé",
            "E15": "Clara Bow",
            "E16": "Rudolph Valentino",
            "E17": "Greta Garbo",
            "E18": "Marlene Dietrich",
            "E19": "Bette Davis",
			}
	

	"""
	***** ESTA PARTE SERA CUANDO TENGAMOS MAS OPERACIONES******
	def read(self, key):
		print(f"Reading: {key} = {self.data.get(key)}")
		return self.data.get(key)
	
	***** ESTA PARTE SERA CUANDO TENGAMOS MAS OPERACIONES******
	def update(self, key, value, operation):
		time.sleep(10)
		if operation == "set":
			self.data[key] = value
			print(f"Updated: {key} = {value}")
		elif operation == "add":
			self.data[key] = self.data.get(key) + value
			print(f"Updated: {key} = {self.data[key]}")
		elif operation == "mult":
			self.data[key] = self.data.get(key) * value
			print(f"Updated: {key} = {self.data[key]}")
		else:
			return False
		return True
	"""
	#Usando solo modificar y consultar por ahora, pero luego tendremos get, set, add, mult, etc.
	def modificar(self, id_empleado, nuevo_nombre):
		self.data[id_empleado] = nuevo_nombre
		print(f"Updated: {id_empleado} = {nuevo_nombre}")
		return self.data[id_empleado]
	def consultar(self, id_empleado):
		print(f"Reading: {id_empleado} = {self.data.get(id_empleado)}") #si usamos self.data[id_empleado] y no existe, lanzara una excepcion y nuestro programa se caerá.
		return self.data[id_empleado]
	