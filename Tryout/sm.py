import threading
import heapq

class StateMachine:
    def __init__(self):
        self.data = {f"E{str(i).zfill(2)}": f"Empleado {i}" for i in range(20)}

    def modificar(self, key, value):
        self.data[key] = value
        return True

    def consultar(self, key):
        return self.data.get(key)


class ConcurrentStateMachine:
    def __init__(self):
        self.sm = StateMachine()
        self.lock = threading.Lock()
        self.buffer = []

    def produce(self, item):
        with self.lock:
            heapq.heappush(self.buffer, item)