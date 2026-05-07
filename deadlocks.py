import threading
import time
from datetime import datetime


def ts() -> str:
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

lock_horno  = threading.Lock()   # Recurso A
lock_mesero = threading.Lock()   # Recurso B

def cocinero(pedido_id: int) -> None:
    print(f"  {ts()}  [COCINERO]  Pedido #{pedido_id}: intentando tomar el Horno...")
    lock_horno.acquire()
    print(f"  {ts()}  [COCINERO]  Pedido #{pedido_id}: tiene el Horno. Cocinando...")
    time.sleep(0.5)  
    print(f"  {ts()}  [COCINERO]  Pedido #{pedido_id}: necesita al Mesero para emplatar...")
    lock_mesero.acquire()                         # BLOQUEO
    print(f"  {ts()}  [COCINERO]  Pedido #{pedido_id}: listo para servir")  # Nunca llega

    lock_mesero.release()
    lock_horno.release()

def mesero(pedido_id: int) -> None:
    print(f"  {ts()}  [MESERO  ]  Pedido #{pedido_id}: intentando asignarse la bandeja...")
    lock_mesero.acquire()
    print(f"  {ts()}  [MESERO  ]  Pedido #{pedido_id}: tiene la bandeja. Esperando horno...")
    time.sleep(0.5)  # Simula espera; en este lapso el cocinero ya tiene su lock

    print(f"  {ts()}  [MESERO  ]  Pedido #{pedido_id}: necesita el Horno para recoger el plato...")
    lock_horno.acquire()                          # BLOQUEO
    print(f"  {ts()}  [MESERO  ]  Pedido #{pedido_id}: recogio el plato")  # Nunca llega

    lock_horno.release()
    lock_mesero.release()


def main() -> None:
    print(f"""
  {'─' * 60}
  DEMOSTRACION DE DEADLOCK
  {'─' * 60}
  Cocinero adquiere: Horno  a Mesero
  Mesero   adquiere: Mesero a Horno
  Orden INVERSO = ciclo de espera garantizado
  {'─' * 60}
""")

    t1 = threading.Thread(target=cocinero, args=(1,), name="Cocinero")
    t2 = threading.Thread(target=mesero,   args=(1,), name="Mesero")

    t1.start()
    t2.start()

    t1.join(timeout=4)
    t2.join(timeout=4)

    if t1.is_alive() or t2.is_alive():
        print(f"""
  {ts()}  *** DEADLOCK DETECTADO ***

  Estado actual:
    Cocinero vivo : {t1.is_alive()}  (bloqueado esperando lock_mesero)
    Mesero   vivo : {t2.is_alive()}  (bloqueado esperando lock_horno)

  Ninguno puede avanzar
  El programa esta congelado
  En un sistema real esto quedaria bloqueado para siempre
""")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n  Interrumpido por el usuario.")
    else:
        print(f"  {ts()}  Los hilos terminaron (no hubo deadlock esta vez)")

main()