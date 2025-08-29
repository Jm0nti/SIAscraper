"""
Writer process: consumes messages from a multiprocessing.Queue and writes CSVs safely.
Messages:
 - {'type':'asignatura', 'info': {...}, 'output_dir': 'Data/Facultad_X', 'omit_existing': bool}
 - {'type':'flush'} -> force write
 - {'type':'shutdown'} -> write remaining and exit
"""
import multiprocessing
import time
import pandas as pd
import os
import tempfile

PLACEHOLDER_PATTERNS = ["Selecciona qué quieres consultar"]

class CentralWriter:
    def __init__(self, queue: multiprocessing.Queue, flush_interval=5):
        self.queue = queue
        self.flush_interval = flush_interval
        self.running = True
        # in-memory stores
        self.asignaturas = []
        self.asignaturas_carrera = []
        self.horarios = []
        self.prerrequisitos = []
        self.last_flush = time.time()

    def _is_placeholder(self, text: str) -> bool:
        if not text:
            return False
        for p in PLACEHOLDER_PATTERNS:
            if p.lower() in text.lower():
                return True
        return False

    def _ingest_asignatura(self, info: dict):
        # filter placeholder names or codes
        codigo = info.get('codigo', '')
        nombre = info.get('nombre', '')
        if self._is_placeholder(codigo) or self._is_placeholder(nombre):
            print(f"[writer] Omitiendo asignatura placeholder: {codigo} - {nombre}")
            return
        # Asignaturas
        self.asignaturas.append({
            'Codigo de asignatura': codigo,
            'Nombre de asignatura': nombre,
            'Numero de creditos': info.get('creditos', '')
        })
        # AsignaturasCarrera
        self.asignaturas_carrera.append({
            'Codigo de asignatura': codigo,
            'Nombre de asignatura': nombre,
            'Carrera': info.get('carrera', ''),
            'Tipologia de asignatura': info.get('tipologia', '')
        })
        # Horarios
        for grupo in info.get('grupos', []):
            for horario in grupo.get('horarios', []):
                dia = horario.get('dia', '')
                if not dia:
                    continue
                self.horarios.append({
                    'Codigo de asignatura': codigo,
                    'Nombre de asignatura': nombre,
                    'Grupo': grupo.get('numero_grupo', ''),
                    'Profesor': grupo.get('profesor', ''),
                    'Dia': dia,
                    'Hora inicio': horario.get('hora_inicio', ''),
                    'Hora fin': horario.get('hora_fin', ''),
                    'Salon': horario.get('salon', '')
                })
        # Prerrequisitos
        for pr in info.get('prerrequisitos', []):
            self.prerrequisitos.append(pr)

    def _atomic_write(self, df: pd.DataFrame, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(delete=False, dir=os.path.dirname(path), suffix='.csv')
        try:
            df.to_csv(tmp.name, index=False)
            tmp.close()
            os.replace(tmp.name, path)
        finally:
            try:
                if os.path.exists(tmp.name):
                    os.remove(tmp.name)
            except Exception:
                pass

    def flush(self, output_dir='Data'):
        # dedupe and write
        try:
            output_dir = output_dir if output_dir and output_dir != '.' else 'Data'
            os.makedirs(output_dir, exist_ok=True)
            if self.asignaturas:
                df = pd.DataFrame(self.asignaturas)
                if 'Codigo de asignatura' in df.columns:
                    df.drop_duplicates(subset=['Codigo de asignatura'], inplace=True)
                path = os.path.join(output_dir, 'Asignaturas.csv')
                if os.path.exists(path):
                    existing = pd.read_csv(path, dtype=str)
                    combined = pd.concat([existing, df], ignore_index=True)
                    if 'Codigo de asignatura' in combined.columns:
                        combined.drop_duplicates(subset=['Codigo de asignatura'], inplace=True)
                    self._atomic_write(combined, path)
                else:
                    self._atomic_write(df, path)
                print(f"[writer] Asignaturas.csv actualizado ({len(df)} nuevas)")
                self.asignaturas.clear()

            if self.asignaturas_carrera:
                df = pd.DataFrame(self.asignaturas_carrera)
                path = os.path.join(output_dir, 'AsignaturasCarrera.csv')
                if os.path.exists(path):
                    existing = pd.read_csv(path)
                    combined = pd.concat([existing, df], ignore_index=True)
                    self._atomic_write(combined, path)
                else:
                    self._atomic_write(df, path)
                print(f"[writer] AsignaturasCarrera.csv actualizado ({len(df)} nuevas)")
                self.asignaturas_carrera.clear()

            if self.horarios:
                df = pd.DataFrame(self.horarios)
                path = os.path.join(output_dir, 'Horarios.csv')
                if os.path.exists(path):
                    existing = pd.read_csv(path, dtype=str)
                    combined = pd.concat([existing, df], ignore_index=True)
                    cols = ['Codigo de asignatura', 'Grupo', 'Dia', 'Hora inicio', 'Hora fin']
                    available = [c for c in cols if c in combined.columns]
                    if available:
                        combined.drop_duplicates(subset=available, inplace=True)
                    self._atomic_write(combined, path)
                else:
                    self._atomic_write(df, path)
                print(f"[writer] Horarios.csv actualizado ({len(df)} nuevas)")
                self.horarios.clear()

            if self.prerrequisitos:
                df = pd.DataFrame(self.prerrequisitos)
                path = os.path.join(output_dir, 'Prerrequisitos.csv')
                if os.path.exists(path):
                    existing = pd.read_csv(path)
                    combined = pd.concat([existing, df], ignore_index=True)
                    combined.drop_duplicates(subset=['Codigo asignatura', 'Carrera', 'Prerrequisito'], inplace=True)
                    self._atomic_write(combined, path)
                else:
                    self._atomic_write(df, path)
                print(f"[writer] Prerrequisitos.csv actualizado ({len(df)} nuevas)")
                self.prerrequisitos.clear()
        except Exception as e:
            print(f"[writer] Error al flush: {e}")

    def run(self):
        print('[writer] Writer iniciado')
        while self.running:
            try:
                try:
                    msg = self.queue.get(timeout=self.flush_interval)
                except Exception:
                    msg = None
                if msg:
                    t = msg.get('type')
                    if t == 'asignatura':
                        info = msg.get('info')
                        out = msg.get('output_dir') or 'Data'
                        self._ingest_asignatura(info)
                        # flush per-message to minimize data loss
                        self.flush(output_dir=out)
                    elif t == 'flush':
                        self.flush(msg.get('output_dir', 'Data'))
                    elif t == 'shutdown':
                        print('[writer] Shutdown received; flushing and exiting')
                        self.flush(msg.get('output_dir', 'Data'))
                        self.running = False
                else:
                    # periodic flush
                    if time.time() - self.last_flush > self.flush_interval:
                        self.flush()
                        self.last_flush = time.time()
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"[writer] Error en loop: {e}")
        print('[writer] Writer terminado')


def start_writer(queue: multiprocessing.Queue):
    writer = CentralWriter(queue)
    writer.run()


if __name__ == '__main__':
    q = multiprocessing.Queue()
    start_writer(q)
