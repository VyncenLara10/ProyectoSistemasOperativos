import multiprocessing
import threading
import time
import random
from dataclasses import dataclass, field
from datetime import datetime

CONFIG = {
    "num_mesas":        1,   # Mesas disponibles en el salon
    "num_cocineros":    1,   # Hilos de cocineros en el proceso Cocina
    "num_meseros":      1,   # Hilos de meseros en el proceso Servicio
    "num_clientes":    10,   
    "capacidad_cocina": 1,   # Pedidos que puede tener en proceso la cocina a la vez
    "tiempo_llegada":  (1, 1),  
    "tiempo_coccion":  (4, 5),  
    "tiempo_entrega":  (1, 2),  
    "tiempo_comer":    (15, 18),
}

MENU = [ "Sopa Mein", "Carne asada","Pizza", "Tacos de cochinita",
    "Jocom", "Chiles rellenos", "Caldo de Pata", "Paches de arroz",
]

# Bloqueo global de impresion para que las lineas no se mezclen
_print_lock = multiprocessing.Lock()


def log(proceso: str, mensaje: str) -> None:
    """Imprime una linea de log con timestamp, proceso e hilo."""
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    hilo = threading.current_thread().name
    prefijos = {
        "CLIENTE":  "[CLIENTE]",
        "COCINA":   "[COCINA]",
        "MESERO":   "[MESERO]",
        "SISTEMA":  "[SISTEMA]",
    }
    prefijo = prefijos.get(proceso, f"[{proceso:<8}]")
    with _print_lock:
        print(f"  {ts}  {prefijo}  ({hilo:<18})  {mensaje}")

def separador(titulo: str = "") -> None:
    ancho = 70
    with _print_lock:
        if titulo:
            relleno = (ancho - len(titulo) - 2) // 2
            print(f"\n  {'─' * relleno} {titulo} {'─' * relleno}")
        else:
            print(f"  {'─' * ancho}")

@dataclass
class Pedido:
    id_pedido:   int
    id_cliente:  int
    plato:       str
    mesa:        int
    hora_pedido: float = field(default_factory=time.time)


def proceso_clientes(
    cola_pedidos:  multiprocessing.Queue,
    cola_listos:   multiprocessing.Queue,
    sem_mesas:     multiprocessing.Semaphore,
    sem_cocina:    multiprocessing.Semaphore,
    contador_pedido: multiprocessing.Value,
    lock_contador:   multiprocessing.Lock,
) -> None:
    """
    Proceso independiente que lanza un hilo por cada cliente.
    Cada hilo de cliente:
      1. Espera su turno de llegada al restaurante.
      2. Adquiere una mesa (semaforo de mesas).
      3. Genera un pedido y lo mete en la cola de pedidos.
      4. Espera a que su pedido aparezca en cola_listos.
      5. Libera la mesa.
    """
    separador("PROCESO CLIENTES iniciado")

    # Diccionario compartido en memoria local del proceso para rastrear
    # que clientes esperan su pedido (id_pedido -> Event)
    eventos_entrega: dict[int, threading.Event] = {}
    lock_eventos = threading.Lock()

    def hilo_cliente(id_cliente: int, numero_mesa_asignada: list) -> None:
        """Logica de un cliente individual."""
        threading.current_thread().name = f"Cliente número {id_cliente}"

        # --- Llegada al restaurante ---
        log("CLIENTE", f"Cliente {id_cliente} llega al restaurante")

        # --- Esperar mesa disponible (semaforo) ---
        log("CLIENTE", f"Cliente {id_cliente} espera mesa...")
        sem_mesas.acquire()
        mesa = id_cliente % CONFIG["num_mesas"] + 1
        numero_mesa_asignada[0] = mesa
        log("CLIENTE", f"Cliente {id_cliente} se sienta en mesa {mesa}")

        # --- Hacer pedido ---
        plato = random.choice(MENU)

        with lock_contador:
            contador_pedido.value += 1
            id_pedido = contador_pedido.value

        pedido = Pedido(
            id_pedido=id_pedido,
            id_cliente=id_cliente,
            plato=plato,
            mesa=mesa,
        )

        # Registrar evento de espera antes de encolar el pedido
        evento = threading.Event()
        with lock_eventos:
            eventos_entrega[id_pedido] = evento

        log("CLIENTE", f"Cliente {id_cliente} pide '{plato}' (pedido #{id_pedido})")
        cola_pedidos.put(pedido)

        # --- Esperar a que el mesero entregue ---
        log("CLIENTE", f"Cliente {id_cliente} espera su comida...")
        evento.wait()
        log("CLIENTE", f"Cliente {id_cliente} recibio su pedido y esta comiendo")

        # --- Tiempo de comer ---
        tiempo_comer = random.uniform(*CONFIG["tiempo_comer"])
        time.sleep(tiempo_comer)

        # --- Liberar mesa ---
        log("CLIENTE", f"Cliente {id_cliente} termino y libera la mesa {mesa}")
        sem_mesas.release()

    def hilo_receptor_entregas() -> None:
        """
        Hilo auxiliar que escucha cola_listos y despierta al cliente
        correspondiente cuando su pedido fue entregado.
        """
        threading.current_thread().name = "Receptor-Entregas"
        clientes_servidos = 0
        while clientes_servidos < CONFIG["num_clientes"]:
            try:
                id_pedido_listo = cola_listos.get(timeout=30)
                with lock_eventos:
                    evento = eventos_entrega.get(id_pedido_listo)
                if evento:
                    evento.set()
                clientes_servidos += 1
            except Exception:
                break

    t_receptor = threading.Thread(target=hilo_receptor_entregas, daemon=True)
    t_receptor.start()

    hilos = []
    for i in range(1, CONFIG["num_clientes"] + 1):
        mesa_asignada = [0]
        t = threading.Thread(
            target=hilo_cliente,
            args=(i, mesa_asignada),
        )
        hilos.append(t)
        t.start()
        retardo = random.uniform(*CONFIG["tiempo_llegada"])
        time.sleep(retardo)

    for t in hilos:
        t.join()

    t_receptor.join(timeout=10)

    for _ in range(CONFIG["num_cocineros"]):
        cola_pedidos.put(None)

    separador("PROCESO CLIENTES finalizado")

def proceso_cocina(
    cola_pedidos:   multiprocessing.Queue,
    cola_cocinados: multiprocessing.Queue,
    sem_cocina:     multiprocessing.Semaphore,
) -> None:
    """
    Proceso independiente que lanza N hilos de cocineros.
    Cada cocinero:
      1. Toma un pedido de cola_pedidos.
      2. Adquiere un espacio en la cocina (semaforo de capacidad).
      3. Cocina durante un tiempo aleatorio.
      4. Pone el pedido en cola_cocinados.
      5. Libera el espacio en cocina.
    """
    separador("PROCESO COCINA iniciado")

    finalizaciones_recibidas = multiprocessing.Value("i", 0)
    lock_fin = threading.Lock()

    def hilo_cocinero(id_cocinero: int) -> None:
        threading.current_thread().name = f"Cocinero número {id_cocinero}"
        while True:
            pedido = cola_pedidos.get()

            if pedido is None:
                # Reencolar la señal de fin para que otros cocineros la vean
                cola_pedidos.put(None)
                log("COCINA", f"Cocinero {id_cocinero} termina su turno")
                break

            # Adquirir espacio en cocina
            sem_cocina.acquire()
            log(
                "COCINA",
                f"Cocinero {id_cocinero} prepara pedido #{pedido.id_pedido} "
                f"'{pedido.plato}' para cliente {pedido.id_cliente}",
            )

            tiempo = random.uniform(*CONFIG["tiempo_coccion"])
            time.sleep(tiempo)

            log(
                "COCINA",
                f"Cocinero {id_cocinero} termino pedido #{pedido.id_pedido} "
                f"en {tiempo:.1f}s",
            )

            cola_cocinados.put(pedido)
            sem_cocina.release()

    hilos = [
        threading.Thread(target=hilo_cocinero, args=(i,))
        for i in range(1, CONFIG["num_cocineros"] + 1)
    ]
    for t in hilos:
        t.start()
    for t in hilos:
        t.join()

    # Señal de fin para meseros
    for _ in range(CONFIG["num_meseros"]):
        cola_cocinados.put(None)

    separador("PROCESO COCINA finalizado")


def proceso_servicio(
    cola_cocinados: multiprocessing.Queue,
    cola_listos:    multiprocessing.Queue,
):
    """
    Proceso independiente que lanza N hilos de meseros.
    Cada mesero:
      1. Toma un pedido listo de cola_cocinados.
      2. Lo entrega (simula tiempo de desplazamiento).
      3. Notifica a cola_listos que el pedido fue entregado.
    """
    separador("PROCESO SERVICIO iniciado")

    finalizaciones = [0]
    lock_fin = threading.Lock()

    def hilo_mesero(id_mesero: int) -> None:
        threading.current_thread().name = f"Mesero número {id_mesero}"
        while True:
            pedido = cola_cocinados.get()

            if pedido is None:
                log("MESERO", f"Mesero {id_mesero} termina su turno")
                break

            tiempo = random.uniform(*CONFIG["tiempo_entrega"])
            log(
                "MESERO",
                f"Mesero {id_mesero} lleva pedido #{pedido.id_pedido} "
                f"'{pedido.plato}' a mesa {pedido.mesa}",
            )
            time.sleep(tiempo)

            log(
                "MESERO",
                f"Mesero {id_mesero} entrego pedido #{pedido.id_pedido} "
                f"al cliente {pedido.id_cliente}",
            )
            cola_listos.put(pedido.id_pedido)

    hilos = [
        threading.Thread(target=hilo_mesero, args=(i,))
        for i in range(1, CONFIG["num_meseros"] + 1)
    ]
    for t in hilos:
        t.start()
    for t in hilos:
        t.join()

    separador("PROCESO SERVICIO finalizado")

def main() -> None:
    separador("SIMULADOR DE RESTAURANTE LOS BUENOS HERMANOS")
    with _print_lock:
        print(f"""
  Configuracion:
    Mesas disponibles   : {CONFIG['num_mesas']}
    Cocineros           : {CONFIG['num_cocineros']}
    Meseros             : {CONFIG['num_meseros']}
    Clientes totales    : {CONFIG['num_clientes']}
    Capacidad cocina    : {CONFIG['capacidad_cocina']}
""")

    # Colas para comunicacion entre procesos
    cola_pedidos   = multiprocessing.Queue()   # Clientes va a Cocina
    cola_cocinados = multiprocessing.Queue()   # Cocina va a Meseros
    cola_listos    = multiprocessing.Queue()   # Meseros va a Clientes

    # Semaforos
    sem_mesas  = multiprocessing.Semaphore(CONFIG["num_mesas"])
    sem_cocina = multiprocessing.Semaphore(CONFIG["capacidad_cocina"])

    # Contador de pedidos compartido entre procesos (exclusion mutua)
    contador_pedido = multiprocessing.Value("i", 0)
    lock_contador   = multiprocessing.Lock()

    p_clientes = multiprocessing.Process(
        target=proceso_clientes,
        name="Proceso-Clientes",
        args=(
            cola_pedidos,
            cola_listos,
            sem_mesas,
            sem_cocina,
            contador_pedido,
            lock_contador,
        ),
    )

    p_cocina = multiprocessing.Process(
        target=proceso_cocina,
        name="Proceso-Cocina",
        args=(cola_pedidos, cola_cocinados, sem_cocina),
    )

    p_servicio = multiprocessing.Process(
        target=proceso_servicio,
        name="Proceso-Servicio",
        args=(cola_cocinados, cola_listos),
    )

    inicio = time.time()

    # Los tres procesos corren en paralelo
    p_clientes.start()
    p_cocina.start()
    p_servicio.start()

    p_clientes.join()
    p_cocina.join()
    p_servicio.join()

    duracion = time.time() - inicio
    separador("CERRAMOS EL RESTAURANTE")
    with _print_lock:
        print(f"\n  Todos los clientes fueron atendidos.")
        print(f"  Tiempo total de simulacion: {duracion:.2f} segundos.\n")
    separador()


if __name__ == "__main__":
    try:
        multiprocessing.set_start_method("fork")
    except RuntimeError:
        pass 
    main()