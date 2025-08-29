"""
main.py

Lanza los bots de cada facultad en procesos separados (multiprocessing).
Cada bot debe exponer una función `main()` (ya presente en los módulos de bot).

Uso: python main.py
"""
import multiprocessing
import time
import traceback
import sys
import argparse

# Importar los módulos de los bots (los archivos deben existir en el mismo directorio)
import src.botAgrarias as botAgrarias
import src.botArquitectura as botArquitectura
import src.botCiencias as botCiencias
import src.botFCHE as botFCHE
import src.botMinas as botMinas
import src.botMinas2 as botMinas2
import src.writer as writer_module

"""
BOT_MODULES = [
    ("FCHE", botFCHE),
    ("Arquitectura", botArquitectura),
    ("Agrarias", botAgrarias),
    ("Ciencias", botCiencias),
    ("Minas", botMinas),
]
"""

BOT_MODULES = [
    ("FCHE", botFCHE),
    ("Arquitectura", botArquitectura),
    ("Agrarias", botAgrarias),
    ("Ciencias", botCiencias),
    ("Minas", botMinas),
    ("Minas2", botMinas2)
]


def start_processes(modules, delay_between_starts=15):
    processes = []
    for name, mod in modules:
        # Ejecutar la función main del módulo en un proceso separado
        p = multiprocessing.Process(target=mod.main, name=f"bot-{name}")
        p.start()
        print(f"[main] Lanzado proceso {p.name} pid={p.pid} para bot {name}")
        processes.append((name, p))
        # Espera configurable entre lanzamientos para permitir interacción manual
        time.sleep(delay_between_starts)
    return processes


def monitor_processes(processes):
    try:
        while True:
            alive = False
            for name, p in processes:
                status = 'alive' if p.is_alive() else 'stopped'
                print(f"[main] {name}: pid={p.pid} status={status} exitcode={p.exitcode}" )
                if p.is_alive():
                    alive = True
            if not alive:
                print("[main] Todos los procesos han terminado.")
                break
            # Intervalo de sondeo
            time.sleep(5)
    except KeyboardInterrupt:
        print("[main] Interrupción por teclado: terminando procesos...")
        for name, p in processes:
            if p.is_alive():
                print(f"[main] Terminando {name} (pid={p.pid})")
                p.terminate()
        raise


def main():
    # En Windows, asegurar el método 'spawn' para multiprocessing
    try:
        multiprocessing.set_start_method('spawn')
    except RuntimeError:
        # ya establecido
        pass

    parser = argparse.ArgumentParser(description='Lanza bots en procesos separados')
    parser.add_argument('--delay', '-d', type=float, default=15,
                        help='Segundos a esperar entre el lanzamiento de cada bot (por defecto 15s)')
    parser.add_argument('--headless', action='store_true', help='Ejecutar navegadores en modo headless (sin UI)')
    args = parser.parse_args()

    # Crear cola y proceso writer central
    manager = multiprocessing.Manager()
    writer_queue = manager.Queue()
    writer_proc = multiprocessing.Process(target=writer_module.start_writer, args=(writer_queue,), name='writer')
    writer_proc.start()
    print(f"[main] Lanzado proceso writer pid={writer_proc.pid}")

    # Pasar la opción headless y la writer_queue a cada proceso como argumento
    processes = []
    for name, mod in BOT_MODULES:
        # Cada bot.main(headless, writer_queue)
        p = multiprocessing.Process(target=mod.main, args=(args.headless, writer_queue), name=f"bot-{name}")
        p.start()
        print(f"[main] Lanzado proceso {p.name} pid={p.pid} para bot {name} (headless={args.headless})")
        processes.append((name, p))
    procs = processes
    print(f"[main] Lanzados {len(procs)} bots. Monitorizando... (delay entre lanzamientos: {args.delay}s)")
    monitor_processes(procs)

    # Reporte final
    for name, p in procs:
        p.join(timeout=0.1)
        print(f"[main] Bot {name} exitcode={p.exitcode}")
    # tell writer to shutdown
    try:
        writer_queue.put({'type': 'shutdown'})
    except Exception:
        pass
    writer_proc.join(timeout=5)
    print(f"[main] Writer exitcode={writer_proc.exitcode}")


if __name__ == '__main__':
    try:
        main()
    except Exception:
        print("[main] Excepción no manejada en main:")
        traceback.print_exc()
        sys.exit(1)
