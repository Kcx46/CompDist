import client
import client2
import client3
import threading


def main():
    hilo_cliente1 = threading.Thread(target=client.main) #crea un hilo para ejecutar la función main del cliente
    hilo_cliente1.start() #inicia el hilo para ejecutar la función main del cliente
    hilo_cliente2 = threading.Thread(target=client2.main) #crea un hilo para ejecutar la función main del cliente
    hilo_cliente2.start() #inicia el hilo para ejecutar la función main del cliente
    hilo_cliente3 = threading.Thread(target=client3.main) #crea un hilo para ejecutar la función main del cliente
    hilo_cliente3.start() #inicia el hilo para ejecutar la función main del cliente

if __name__=="__main__":
    main()