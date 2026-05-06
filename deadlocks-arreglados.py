import threading
import time
import random
from datetime import datetime
from typing import Optional


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


_print_lock = threading.Lock()


def log(rol: str, mensaje: str) -> None:
    with _print_lock:
        hilo = threading.current_thread().name
        print(f"  {ts()}  [{rol:<10}]  ({hilo:<14})  {mensaje}")


def separador(titulo: str = "") -> None:
    ancho = 62
    with _print_lock:
        if titulo:
            pad = (ancho - len(titulo) - 2) // 2
            print(f"\n  {'─' * pad} {titulo} {'─' * pad}")
        else:
            print(f"  {'─' * ancho}")


class RecursoOrdenado:
    """Lock con un ID numerico para forzar orden de adquisicion."""
    _contador = 0
    _lock_contador = threading.Lock()

    def __init__(self, nombre: str) -> None:
        with RecursoOrdenado._lock_contador:
            RecursoOrdenado._contador += 1
            self.id = RecursoOrdenado._contador
        self.nombre = nombre
        self._lock = threading.Lock()

    def acquire(self) -> None:
        self._lock.acquire()

    def release(self) -> None:
        self._lock.release()

    def __repr__(self) -> str:
        return f"Recurso({self.id}:{self.nombre})"


def adquirir_en_orden(*recursos: RecursoOrdenado) -> list[RecursoOrdenado]:
    """
    Adquiere varios recursos siempre en orden ascendente por ID.
    Sin importar el orden en que se pasen, el orden de adquisicion
    es siempre el mismo → imposible formar un ciclo de espera.
    """
    ordenados = sorted(recursos, key=lambda r: r.id)
    for r in ordenados:
        r.acquire()
    return ordenados


def liberar_en_orden(*recursos: RecursoOrdenado) -> None:
    for r in resources:
        r.release()


def demo_orden_global() -> None:
    separador("TECNICA 1: Orden global de adquisicion")

    horno  = RecursoOrdenado("Horno")   # ID = 1
    mesero = RecursoOrdenado("Mesero")  # ID = 2

    resultados = []

    def cocinero_seguro(pedido_id: int) -> None:
        threading.current_thread().name = f"Cocinero-{pedido_id}"
        # Pide Horno y Mesero → adquirir_en_orden los ordena: ID1, ID2
        log("COCINERO", f"Pedido #{pedido_id}: solicitando Horno y Mesero")
        recursos = adquirir_en_orden(horno, mesero)
        log("COCINERO", f"Pedido #{pedido_id}: tiene ambos recursos, cocinando")
        time.sleep(random.uniform(0.2, 0.5))
        log("COCINERO", f"Pedido #{pedido_id}: listo")
        resultados.append(f"Pedido #{pedido_id} completado por cocinero")
        for r in recursos:
            r.release()

    def mesero_seguro(pedido_id: int) -> None:
        threading.current_thread().name = f"Mesero-{pedido_id}"
        # Pide Mesero y Horno → adquirir_en_orden los reordena: ID1, ID2
        # Aunque el mesero "quiera" Mesero primero, el orden forzado es el mismo
        log("MESERO", f"Pedido #{pedido_id}: solicitando Mesero y Horno")
        recursos = adquirir_en_orden(mesero, horno)  # orden de parametros da igual
        log("MESERO", f"Pedido #{pedido_id}: tiene ambos recursos, sirviendo")
        time.sleep(random.uniform(0.2, 0.5))
        log("MESERO", f"Pedido #{pedido_id}: entregado")
        resultados.append(f"Pedido #{pedido_id} completado por mesero")
        for r in recursos:
            r.release()

    hilos = []
    for i in range(1, 4):
        hilos.append(threading.Thread(target=cocinero_seguro, args=(i,)))
        hilos.append(threading.Thread(target=mesero_seguro,   args=(i,)))

    random.shuffle(hilos)  # Orden de inicio aleatorio para estres
    for t in hilos:
        t.start()
    for t in hilos:
        t.join(timeout=10)

    colgados = [t for t in hilos if t.is_alive()]
    with _print_lock:
        if colgados:
            print(f"\n  FALLO: {len(colgados)} hilo(s) siguen vivos")
        else:
            print(f"\n  Resultado: {len(resultados)} operaciones completadas sin deadlock")





def adquirir_con_timeout(
    *locks: threading.Lock,
    timeout: float = 1.0,
    max_reintentos: int = 5,
) -> Optional[list[threading.Lock]]:
    for intento in range(1, max_reintentos + 1):
        adquiridos = []
        exito = True

        for lock in locks:
            if lock.acquire(timeout=timeout):
                adquiridos.append(lock)
            else:
                # Fallo: liberar todo lo adquirido hasta ahora
                for l in adquiridos:
                    l.release()
                adquiridos.clear()
                exito = False
                break

        if exito:
            return adquiridos

        espera = random.uniform(0.05, 0.2) * intento
        log("SISTEMA", f"Timeout en intento {intento}, reintentando en {espera:.2f}s")
        time.sleep(espera)

    return None


def demo_timeout_backoff() -> None:
    separador("TECNICA 2: Timeout con Backoff")

    lock_a = threading.Lock()  # Horno
    lock_b = threading.Lock()  # Mesero

    completados = []
    lock_completados = threading.Lock()

    def tarea_con_backoff(nombre: str, primero: threading.Lock, segundo: threading.Lock) -> None:
        threading.current_thread().name = nombre
        log("TAREA", f"{nombre}: intentando adquirir recursos (orden conflictivo)")

        resultado = adquirir_con_timeout(primero, segundo, timeout=0.3, max_reintentos=6)

        if resultado is None:
            log("TAREA", f"{nombre}: no pudo adquirir recursos tras multiples intentos")
            return

        log("TAREA", f"{nombre}: tiene ambos recursos, trabajando")
        time.sleep(random.uniform(0.1, 0.3))
        log("TAREA", f"{nombre}: trabajo completado")

        for lock in resultado:
            lock.release()

        with lock_completados:
            completados.append(nombre)

    # Orden inverso deliberado igual que en el deadlock original
    t1 = threading.Thread(target=tarea_con_backoff, args=("Cocinero-A", lock_a, lock_b))
    t2 = threading.Thread(target=tarea_con_backoff, args=("Mesero-B",   lock_b, lock_a))

    t1.start()
    t2.start()
    t1.join(timeout=8)
    t2.join(timeout=8)

    colgados = [t for t in [t1, t2] if t.is_alive()]
    with _print_lock:
        if colgados:
            print(f"\n  FALLO: hilos siguen vivos (livelock o timeout insuficiente)")
        else:
            print(f"\n  Resultado: {completados} completados sin deadlock permanente")



def demo_watchdog() -> None:
    separador("TECNICA 3: Watchdog detector")

    progreso: dict[str, float] = {}   # nombre_hilo → timestamp del ultimo avance
    lock_progreso = threading.Lock()
    detener_vigilante = threading.Event()

    UMBRAL_DEADLOCK = 2.0  # segundos sin avanzar = posible deadlock

    def actualizar_progreso() -> None:
        nombre = threading.current_thread().name
        with lock_progreso:
            progreso[nombre] = time.time()

    def vigilante() -> None:
        threading.current_thread().name = "Watchdog"
        log("WATCHDOG", "Iniciado, monitoreando hilos cada 0.5s")
        while not detener_vigilante.is_set():
            time.sleep(0.5)
            ahora = time.time()
            with lock_progreso:
                for nombre, ultimo in progreso.items():
                    retraso = ahora - ultimo
                    if retraso > UMBRAL_DEADLOCK:
                        log(
                            "WATCHDOG",
                            f"ALERTA: '{nombre}' sin avanzar hace {retraso:.1f}s "
                            f"→ posible deadlock",
                        )
        log("WATCHDOG", "Detenido")

    lock_recurso = threading.Lock()

    def tarea_normal(nombre: str, sleep_antes: float) -> None:
        threading.current_thread().name = nombre
        actualizar_progreso()
        log("TAREA", f"{nombre}: iniciando")
        time.sleep(sleep_antes)
        actualizar_progreso()
        lock_recurso.acquire()
        log("TAREA", f"{nombre}: tiene el recurso, trabajando")
        time.sleep(0.3)
        lock_recurso.release()
        actualizar_progreso()
        log("TAREA", f"{nombre}: finalizado")

    def tarea_lenta(nombre: str) -> None:
        """Simula un hilo que se queda colgado esperando un recurso."""
        threading.current_thread().name = nombre
        actualizar_progreso()
        log("TAREA", f"{nombre}: iniciando (se quedara bloqueada)")
        # Adquirir el recurso y no soltarlo = simula hilo congelado
        lock_recurso.acquire()
        log("TAREA", f"{nombre}: tiene el recurso (no lo suelta)")
        time.sleep(10)  # Simula bloqueo largo
        lock_recurso.release()

    t_vigilante = threading.Thread(target=vigilante, daemon=True)
    t_vigilante.start()

    t_lenta = threading.Thread(target=tarea_lenta, args=("Tarea-Colgada",))
    t_normal = threading.Thread(target=tarea_normal, args=("Tarea-Esperando", 0.1))

    t_lenta.start()
    time.sleep(0.05)
    t_normal.start()

    t_normal.join(timeout=5)
    detener_vigilante.set()
    t_vigilante.join(timeout=2)

    with _print_lock:
        print(f"\n  El watchdog detecto el hilo bloqueado y lo reporto")
        print(f"  En un sistema real aqui se tomaria accion (matar hilo, liberar recurso)")
        print(f"  No se como matarlo inge :( )")

def main() -> None:
    separador("SOLUCIONES A DEADLOCK - RESTAURANTE LOS BUENOS HERMANOS")

    demo_orden_global()
    time.sleep(0.5)

    demo_timeout_backoff()
    time.sleep(0.5)

    demo_watchdog()

    separador("FIN DE LA DEMOSTRACION")


main()