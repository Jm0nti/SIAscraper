from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import pandas as pd
import re
import time
from typing import Dict, List, Tuple
import os
import json
import stat
import errno
import platform
import tempfile


# Cross-platform file lock (uses msvcrt on Windows, fcntl on POSIX)
class FileLock:
    def __init__(self, lock_path, timeout=60, poll_interval=0.1):
        self.lock_path = lock_path
        self.timeout = timeout
        self.poll_interval = poll_interval
        self.handle = None
        self.is_windows = platform.system().lower().startswith('win')

    def __enter__(self):
        start = time.time()
        # Ensure parent dir exists
        os.makedirs(os.path.dirname(self.lock_path) or '.', exist_ok=True)
        while True:
            try:
                # Open (or create) lock file
                if self.is_windows:
                    # Windows: open file and lock using msvcrt
                    import msvcrt
                    self.handle = open(self.lock_path, 'a+')
                    try:
                        msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                        return self
                    except OSError:
                        self.handle.close()
                        self.handle = None
                else:
                    # POSIX: use fcntl.flock
                    import fcntl
                    self.handle = open(self.lock_path, 'a+')
                    try:
                        fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                        return self
                    except OSError:
                        self.handle.close()
                        self.handle = None
            except Exception:
                # any failure to open/lock -> retry until timeout
                if self.handle:
                    try:
                        self.handle.close()
                    except Exception:
                        pass
                    self.handle = None

            if (time.time() - start) >= self.timeout:
                raise TimeoutError(f"Timeout acquiring lock {self.lock_path}")
            time.sleep(self.poll_interval)

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.handle:
                try:
                    if self.is_windows:
                        import msvcrt
                        try:
                            self.handle.seek(0)
                            msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
                        except Exception:
                            pass
                    else:
                        import fcntl
                        try:
                            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                        except Exception:
                            pass
                finally:
                    try:
                        self.handle.close()
                    except Exception:
                        pass
                    # best-effort cleanup of lock file
                    try:
                        if os.path.exists(self.lock_path):
                            os.remove(self.lock_path)
                    except Exception:
                        pass
        except Exception:
            pass



# Utilidad para integración directa desde botMinas.py
def scrape_asignatura_from_driver(driver_externo, output_dir=".", writer_queue=None):
    """
    Procesa la asignatura abierta en el driver externo y guarda los CSVs.
    Args:
        driver_externo: instancia de selenium.webdriver ya posicionada en la asignatura.
        output_dir: directorio donde guardar los archivos CSV.
    """
    print("[asignaturasinfo] scrape_asignatura_from_driver llamado correctamente.")
    if driver_externo is None:
        print("[asignaturasinfo] Error: driver_externo es None.")
        return
    
    # Crear una nueva instancia del scraper
    scraper = AsignaturasScraper()

    # Primero, intentar extraer el código de la asignatura sin scrapear todo
    # Usar el driver externo para obtener el código de la asignatura de la página
    codigo_asignatura = None
    try:
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import re
        wait = WebDriverWait(driver_externo, 10)
        titulo_element = wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".ocu-titulo h2"))
        )
        titulo_text = titulo_element.text
        codigo_match = re.search(r'\(([^)]+)\)', titulo_text)
        if codigo_match:
            codigo_asignatura = codigo_match.group(1)
    except Exception as e:
        print(f"[asignaturasinfo] No se pudo extraer el código de la asignatura antes del scraping completo: {e}")

    # Verificar si el código ya existe en Asignaturas.csv (usar el output_dir proporcionado)
    output_dir_check = output_dir if output_dir and output_dir != "." else "Data"
    csv_asignaturas = os.path.join(output_dir_check, "Asignaturas.csv")
    codigo_existe = False
    if codigo_asignatura is not None and os.path.exists(csv_asignaturas):
        try:
            df_asig = pd.read_csv(csv_asignaturas, dtype=str)
            if not df_asig.empty:
                codigos = df_asig['Codigo de asignatura'].astype(str).tolist()
                if str(codigo_asignatura) in codigos:
                    codigo_existe = True
        except Exception as e:
            print(f"[asignaturasinfo] Error leyendo Asignaturas.csv: {e}")

    # Procesar la asignatura usando el driver externo con flag de asignatura existente
    info = scraper.extract_asignatura_info_from_driver(driver_externo, omitir_horarios=codigo_existe)

    if info:
        # If a writer_queue is provided, send the extracted info to the central writer
        if writer_queue is not None:
            try:
                msg = {'type': 'asignatura', 'info': info, 'output_dir': output_dir, 'omit_existing': codigo_existe}
                writer_queue.put(msg)
                print(f"[scraper] Enviado info de {info.get('codigo')} al writer queue")
            except Exception as e:
                print(f"[scraper] Error enviando al writer queue: {e}")
        else:
            # Agregar los datos a las listas del scraper con flag de asignatura existente
            scraper.add_asignatura_data(info, omitir_asignatura=codigo_existe, omitir_horarios=codigo_existe)

            # Generar o actualizar los CSVs
            scraper.append_to_csvs(output_dir)

            if codigo_existe:
                print(f"✅ Asignatura {info['nombre']} ({info['codigo']}) ya existía. Se actualizó AsignaturasCarrera.csv y Prerrequisitos.csv. Se omitió scraping de horarios.")
            else:
                print(f"✅ Asignatura procesada: {info['nombre']} ({info['codigo']})")
    else:
        print("❌ No se pudo extraer información de la asignatura")


class AsignaturasScraper:
    def __init__(self, headless=True):
        """
        Inicializa el scraper de asignaturas
        
        Args:
            headless (bool): Si True, ejecuta el navegador sin interfaz gráfica
        """
        self.driver = None
        self.wait = None
        self.headless = headless
        self.asignaturas_data = []
        self.asignaturas_carrera_data = []
        self.horarios_data = []
        self.prerrequisitos_data = []  # Para almacenar los prerrequisitos


    def extract_prerrequisitos_from_page(self, driver, info_asignatura):
        """
        Extrae los prerrequisitos de la asignatura desde la página actual (driver ya posicionado).
        Args:
            driver: Driver de selenium ya posicionado en la página de la asignatura
            info_asignatura: dict con info de la asignatura (codigo, nombre, carrera)
        Returns:
            List[Dict]: Lista de dicts con los prerrequisitos
        """
        prerrequisitos = []
        try:
            print(f"Buscando prerrequisitos para {info_asignatura['codigo']} - {info_asignatura['nombre']}")
            h3s = driver.find_elements(By.TAG_NAME, "h3")
            prerreq_h3 = None
            for h3 in h3s:
                if h3.text.strip().lower() == "prerrequisitos":
                    prerreq_h3 = h3
                    print("Sección de prerrequisitos encontrada")
                    break
            if not prerreq_h3:
                print("No se encontró sección de prerrequisitos")
                return prerrequisitos
            parent = prerreq_h3.find_element(By.XPATH, "..")
            spans = parent.find_elements(By.XPATH, "following-sibling::span[contains(@class, 'borde') and contains(@class, 'salto')]")
            print(f"Se encontraron {len(spans)} elementos span para procesar")
            for span in spans:
                divs = span.find_elements(By.XPATH, ".//div[contains(@class, 'af_panelGroupLayout')]")
                for div in divs:
                    spans_prer = div.find_elements(By.XPATH, ".//span")
                    for i in range(len(spans_prer)-1):
                        text1 = spans_prer[i].text.strip()
                        text2 = spans_prer[i+1].text.strip()
                        # Permitir códigos de 7 dígitos o 7 dígitos + guion + letra
                        if re.match(r"^\d{7}(-[A-Z])?$", text1, re.IGNORECASE) and text2:
                            prerreq_data = {
                                'Codigo asignatura': info_asignatura['codigo'],
                                'Nombre asignatura': info_asignatura['nombre'],
                                'Carrera': info_asignatura['carrera'],
                                'Prerrequisito': f"{text1} {text2}"
                            }
                            prerrequisitos.append(prerreq_data)
                            print(f"Prerrequisito encontrado: {text1} {text2}")
            print(f"Total prerrequisitos extraídos: {len(prerrequisitos)}")
            return prerrequisitos
        except Exception as e:
            print(f"Error extrayendo prerrequisitos: {e}")
            return prerrequisitos
    
    def setup_driver(self):
        """Configura y inicializa el driver de Chrome"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
    
    def extract_asignatura_info_from_driver(self, driver_externo, omitir_horarios=False) -> Dict:
        """
        Extrae toda la información de una asignatura usando un driver externo ya posicionado
        
        Args:
            driver_externo: Driver de selenium ya posicionado en la página de la asignatura
            omitir_horarios (bool): Si True, omite la extracción de información de horarios/grupos
            
        Returns:
            Dict: Diccionario con toda la información extraída
        """
        try:
            # Usar el driver externo
            driver = driver_externo
            wait = WebDriverWait(driver, 10)
            
            time.sleep(2)  # Esperar a que cargue la página
            
            info = {
                'codigo': '',
                'nombre': '',
                'creditos': '',
                'carrera': '',
                'tipologia': '',
                'grupos': [],
                'prerrequisitos': []
            }
            
            # Extraer código y nombre de asignatura del título principal
            try:
                titulo_element = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".ocu-titulo h2"))
                )
                titulo_text = titulo_element.text
                print(f"Título encontrado: {titulo_text}")
                
                # Buscar código entre paréntesis (puede contener letras y guiones)
                codigo_match = re.search(r'\(([\w\-]+)\)', titulo_text)
                if codigo_match:
                    info['codigo'] = codigo_match.group(1)
                # El nombre es todo lo que está antes del paréntesis
                info['nombre'] = titulo_text.split('(')[0].strip()
                
                print(f"Código extraído: {info['codigo']}")
                print(f"Nombre extraído: {info['nombre']}")
            except Exception as e:
                print(f"Error extrayendo título: {e}")
            
            # Extraer créditos
            try:
                creditos_element = driver.find_element(
                    By.CSS_SELECTOR, ".row.detass-creditos span[id*='ot']"
                )
                info['creditos'] = creditos_element.text.strip()
                print(f"Créditos extraídos: {info['creditos']}")
            except Exception as e:
                print(f"Error extrayendo créditos: {e}")
            
            # Extraer carrera (plan de estudios)
            try:
                carrera_element = driver.find_element(
                    By.CSS_SELECTOR, ".row.detass-plan span[id*='ot']"
                )
                info['carrera'] = carrera_element.text.strip()
                print(f"Carrera extraída: {info['carrera']}")
            except Exception as e:
                print(f"Error extrayendo carrera: {e}")
            
            # Extraer tipología
            try:
                tipologia_element = driver.find_element(
                    By.CSS_SELECTOR, ".row.detass-tipologia span[id*='ot']"
                )
                info['tipologia'] = tipologia_element.text.strip()
                print(f"Tipología extraída: {info['tipologia']}")
            except Exception as e:
                print(f"Error extrayendo tipología: {e}")
            
            # Extraer información de grupos solo si no se debe omitir
            if not omitir_horarios:
                try:
                    grupos_elements = driver.find_elements(
                        By.CSS_SELECTOR, ".borde.salto .af_showDetailHeader"
                    )
                    
                    print(f"Se encontraron {len(grupos_elements)} grupos")
                    
                    for i, grupo_element in enumerate(grupos_elements):
                        grupo_info = self.extract_grupo_info(grupo_element, driver)
                        if grupo_info:
                            info['grupos'].append(grupo_info)
                            print(f"Grupo {i+1} procesado: {grupo_info['numero_grupo']}")
                            
                except Exception as e:
                    print(f"Error extrayendo grupos: {e}")
            else:
                print("Se omite extracción de horarios (asignatura ya existe)")
            
            # Extraer prerrequisitos de la página (siempre se extraen)
            info['prerrequisitos'] = self.extract_prerrequisitos_from_page(driver, info)
            return info
            
        except Exception as e:
            print(f"Error general extrayendo información: {e}")
            return None
    
    def extract_grupo_info(self, grupo_element, driver) -> Dict:
        """
        Extrae información de un grupo específico
        
        Args:
            grupo_element: Elemento HTML del grupo
            driver: Driver de selenium
            
        Returns:
            Dict: Información del grupo
        """
        try:
            grupo_info = {
                'numero_grupo': '',
                'profesor': '',
                'horarios': []
            }
            
            # Extraer número de grupo (puede contener letras, números y guiones)
            try:
                grupo_titulo = grupo_element.find_element(
                    By.CSS_SELECTOR, ".af_showDetailHeader_title-text0"
                )
                grupo_match = re.search(r'\(([\w\-]+)\)', grupo_titulo.text)
                if grupo_match:
                    grupo_info['numero_grupo'] = grupo_match.group(1)
            except Exception as e:
                print(f"Error extrayendo número de grupo: {e}")
            
            # Hacer click para expandir si está colapsado
            try:
                disclosure_link = grupo_element.find_element(
                    By.CSS_SELECTOR, ".af_showDetailHeader_disclosure-link"
                )
                if "undisclosed" in disclosure_link.get_attribute("class"):
                    disclosure_link.click()
                    time.sleep(1)
            except Exception:
                pass  # Ya está expandido o no se puede expandir
            
            # Extraer información del contenido expandido
            try:
                content_div = grupo_element.find_element(
                    By.CSS_SELECTOR, ".af_showDetailHeader_content0"
                )
                
                # Extraer profesor
                try:
                    profesor_element = content_div.find_element(
                        By.CSS_SELECTOR, ".strong"
                    )
                    grupo_info['profesor'] = profesor_element.text.strip()
                except Exception as e:
                    print(f"Error extrayendo profesor: {e}")
                
                # Extraer horarios
                try:
                    horarios_elements = content_div.find_elements(
                        By.CSS_SELECTOR, ".lista-elemento.sin-descripcion"
                    )
                    
                    for horario_element in horarios_elements:
                        horario_info = self.extract_horario_info(horario_element)
                        if horario_info:
                            grupo_info['horarios'].append(horario_info)
                            
                except Exception as e:
                    print(f"Error extrayendo horarios: {e}")
                    
            except Exception as e:
                print(f"Error accediendo al contenido del grupo")
            
            return grupo_info
            
        except Exception as e:
            print(f"Error extrayendo información del grupo: {e}")
            return None
    
    def extract_horario_info(self, horario_element) -> Dict:
        """
        Extrae información de un horario específico (sin fechas de inicio/fin)
        
        Args:
            horario_element: Elemento HTML del horario
            
        Returns:
            Dict: Información del horario, o None si no hay día válido
        """
        try:
            horario_info = {
                'dia': '',
                'hora_inicio': '',
                'hora_fin': '',
                'salon': ''
            }
            # Extraer día y horas, omitiendo spans que sean fechas (dd/mm/yyyy)
            try:
                
                tiempo_element = horario_element.find_element(
                    By.CSS_SELECTOR, "span[id*='ot10']"
                )
                tiempo_text = tiempo_element.text.strip()
                # Omitir si el texto es una fecha (dd/mm/yyyy)
                if not re.match(r'\d{2}/\d{2}/\d{4}$', tiempo_text):
                    # Parsear día y horas (ej: "MIÉRCOLES de 08:00 a 10:00")
                    tiempo_match = re.search(r'(\w+)\s+de\s+(\d{2}:\d{2})\s+a\s+(\d{2}:\d{2})', tiempo_text)
                    if tiempo_match:
                        horario_info['dia'] = tiempo_match.group(1)
                        horario_info['hora_inicio'] = tiempo_match.group(2)
                        horario_info['hora_fin'] = tiempo_match.group(3)
            except Exception as e:
                print(f"Error extrayendo día y horas")
            # Extraer información del aula (salón)
            try:
                aula_elements = horario_element.find_elements(
                    By.CSS_SELECTOR, "span[id*='ot27'], span[id*='ot28'], span[id*='ot29']"
                )
                salon_parts = []
                for element in aula_elements:
                    text = element.text.strip()
                    if text:
                        salon_parts.append(text)
                horario_info['salon'] = ' '.join(salon_parts)
            except Exception as e:
                print(f"Error extrayendo información del aula: {e}")
            # Si no se extrajo un día válido, omitir este horario
            if not horario_info['dia']:
                return None
            return horario_info
        except Exception as e:
            print(f"Error extrayendo información del horario: {e}")
            return None
    
    def add_asignatura_data(self, info, omitir_asignatura=False, omitir_horarios=False):
        """
        Agrega la información de una asignatura a las listas de datos
        
        Args:
            info (Dict): Información de la asignatura
            omitir_asignatura (bool): Si True, no agrega a Asignaturas.csv
            omitir_horarios (bool): Si True, no agrega a Horarios.csv
        """
        # Agregar a CSV Asignaturas solo si no se debe omitir
        if not omitir_asignatura:
            self.asignaturas_data.append({
                'Codigo de asignatura': info['codigo'],
                'Nombre de asignatura': info['nombre'],
                'Numero de creditos': info['creditos']
            })
        
        # Siempre agregar a CSV AsignaturasCarrera
        self.asignaturas_carrera_data.append({
            'Codigo de asignatura': info['codigo'],
            'Nombre de asignatura': info['nombre'],
            'Carrera': info['carrera'],
            'Tipologia de asignatura': info['tipologia']
        })
        
        # Agregar a CSV Horarios solo si no se debe omitir
        if not omitir_horarios:
            for grupo in info['grupos']:
                for horario in grupo['horarios']:
                    self.horarios_data.append({
                        'Codigo de asignatura': info['codigo'],
                        'Nombre de asignatura': info['nombre'],
                        'Grupo': grupo['numero_grupo'],
                        'Profesor': grupo['profesor'],
                        'Dia': horario['dia'],
                        'Hora inicio': horario['hora_inicio'],
                        'Hora fin': horario['hora_fin'],
                        'Salon': horario['salon']
                    })
        
        # Siempre agregar a CSV Prerrequisitos
        if 'prerrequisitos' in info and info['prerrequisitos']:
            print(f"Agregando {len(info['prerrequisitos'])} prerrequisitos a la lista de datos")
            self.prerrequisitos_data.extend(info['prerrequisitos'])
        else:
            print("No hay prerrequisitos para agregar")
        # Guardar referencia a la carrera dentro del info para AsignaturasCarrera
        # (ya se hace en callers, pero asegurar que existe)
        if 'carrera' not in info:
            info['carrera'] = ''
        

    def append_to_csvs(self, output_dir: str = "."):
        """
        Agrega los datos a los archivos CSV existentes o crea nuevos si no existen
        
        Args:
            output_dir (str): Directorio donde guardar los archivos
        """
        # Resolver output_dir: usar el proporcionado (ej. Data/Facultad_X) o 'Data' por defecto
        output_dir = output_dir if output_dir and output_dir != "." else "Data"
        # Use FileLock to protect concurrent writes across processes
        os.makedirs(output_dir, exist_ok=True)
        lock_path = os.path.join(output_dir, "append_csv.lock")
        try:
            lock_ctx = FileLock(lock_path, timeout=30)
        except Exception:
            lock_ctx = None

        try:
            # Acquire lock if available
            if lock_ctx:
                lock_ctx.__enter__()
                # Asegurar que el directorio existe
                os.makedirs(output_dir, exist_ok=True)
            
            # CSV 1: Asignaturas (solo si hay datos para agregar)
            if self.asignaturas_data:
                csv_asignaturas = f"{output_dir}/Asignaturas.csv"
                if os.path.exists(csv_asignaturas):
                    # Leer datos existentes y agregar nuevos
                    df_existing = pd.read_csv(csv_asignaturas, dtype=str)
                    df_new = pd.DataFrame(self.asignaturas_data)
                    # Unir y eliminar duplicados por código de asignatura
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    if 'Codigo de asignatura' in df_combined.columns:
                        before = len(df_combined)
                        df_combined.drop_duplicates(subset=['Codigo de asignatura'], inplace=True)
                        after = len(df_combined)
                        if before != after:
                            print(f"Asignaturas.csv: eliminados {before-after} duplicados por codigo")
                    # atomic write
                    tmp = tempfile.NamedTemporaryFile(delete=False, dir=output_dir, suffix='.csv')
                    try:
                        df_combined.to_csv(tmp.name, index=False)
                        tmp.close()
                        os.replace(tmp.name, csv_asignaturas)
                    finally:
                        try:
                            if os.path.exists(tmp.name):
                                os.remove(tmp.name)
                        except Exception:
                            pass
                else:
                    # Crear nuevo archivo
                    df_new = pd.DataFrame(self.asignaturas_data)
                    # Asegurar no duplicados en el nuevo dataframe
                    if 'Codigo de asignatura' in df_new.columns:
                        df_new.drop_duplicates(subset=['Codigo de asignatura'], inplace=True)
                    tmp = tempfile.NamedTemporaryFile(delete=False, dir=output_dir, suffix='.csv')
                    try:
                        df_new.to_csv(tmp.name, index=False)
                        tmp.close()
                        os.replace(tmp.name, csv_asignaturas)
                    finally:
                        try:
                            if os.path.exists(tmp.name):
                                os.remove(tmp.name)
                        except Exception:
                            pass
                print(f"Archivo Asignaturas.csv actualizado")
            
            # CSV 2: AsignaturasCarrera (siempre se actualiza)
            if self.asignaturas_carrera_data:
                csv_asignaturas_carrera = f"{output_dir}/AsignaturasCarrera.csv"
                if os.path.exists(csv_asignaturas_carrera):
                    df_existing = pd.read_csv(csv_asignaturas_carrera)
                    df_new = pd.DataFrame(self.asignaturas_carrera_data)
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    tmp = tempfile.NamedTemporaryFile(delete=False, dir=output_dir, suffix='.csv')
                    try:
                        df_combined.to_csv(tmp.name, index=False)
                        tmp.close()
                        os.replace(tmp.name, csv_asignaturas_carrera)
                    finally:
                        try:
                            if os.path.exists(tmp.name):
                                os.remove(tmp.name)
                        except Exception:
                            pass
                else:
                    df_new = pd.DataFrame(self.asignaturas_carrera_data)
                    tmp = tempfile.NamedTemporaryFile(delete=False, dir=output_dir, suffix='.csv')
                    try:
                        df_new.to_csv(tmp.name, index=False)
                        tmp.close()
                        os.replace(tmp.name, csv_asignaturas_carrera)
                    finally:
                        try:
                            if os.path.exists(tmp.name):
                                os.remove(tmp.name)
                        except Exception:
                            pass
                print(f"Archivo AsignaturasCarrera.csv actualizado")
            
            # CSV 3: Horarios (solo si hay datos para agregar)
            if self.horarios_data:
                csv_horarios = f"{output_dir}/Horarios.csv"
                if os.path.exists(csv_horarios):
                    df_existing = pd.read_csv(csv_horarios, dtype=str)
                    df_new = pd.DataFrame(self.horarios_data)
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    # Eliminar duplicados basados en codigo + dia + hora inicio + hora fin + salon
                    cols_to_check = ['Codigo de asignatura', 'Grupo', 'Dia', 'Hora inicio', 'Hora fin']
                    available_cols = [c for c in cols_to_check if c in df_combined.columns]
                    if available_cols:
                        before_h = len(df_combined)
                        df_combined.drop_duplicates(subset=available_cols, inplace=True)
                        after_h = len(df_combined)
                        if before_h != after_h:
                            print(f"Horarios.csv: eliminados {before_h-after_h} duplicados basados en {available_cols}")
                    tmp = tempfile.NamedTemporaryFile(delete=False, dir=output_dir, suffix='.csv')
                    try:
                        df_combined.to_csv(tmp.name, index=False)
                        tmp.close()
                        os.replace(tmp.name, csv_horarios)
                    finally:
                        try:
                            if os.path.exists(tmp.name):
                                os.remove(tmp.name)
                        except Exception:
                            pass
                else:
                    df_new = pd.DataFrame(self.horarios_data)
                    cols_to_check = ['Codigo de asignatura', 'Grupo', 'Dia', 'Hora inicio', 'Hora fin']
                    available_cols = [c for c in cols_to_check if c in df_new.columns]
                    if available_cols:
                        df_new.drop_duplicates(subset=available_cols, inplace=True)
                    tmp = tempfile.NamedTemporaryFile(delete=False, dir=output_dir, suffix='.csv')
                    try:
                        df_new.to_csv(tmp.name, index=False)
                        tmp.close()
                        os.replace(tmp.name, csv_horarios)
                    finally:
                        try:
                            if os.path.exists(tmp.name):
                                os.remove(tmp.name)
                        except Exception:
                            pass
                print(f"Archivo Horarios.csv actualizado")
            
            # CSV 4: Prerrequisitos (siempre se actualiza si hay datos)
            if self.prerrequisitos_data:
                print(f"Procesando {len(self.prerrequisitos_data)} prerrequisitos para guardar")
                csv_prerreq = f"{output_dir}/Prerrequisitos.csv"
                df_new = pd.DataFrame(self.prerrequisitos_data)
                if os.path.exists(csv_prerreq):
                    df_existing = pd.read_csv(csv_prerreq)
                    print(f"Archivo existente tiene {len(df_existing)} registros")
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                    # Eliminar duplicados basándose solo en las columnas clave
                    before_dedup = len(df_combined)
                    df_combined.drop_duplicates(subset=['Codigo asignatura', 'Carrera', 'Prerrequisito'], inplace=True)
                    after_dedup = len(df_combined)
                    tmp = tempfile.NamedTemporaryFile(delete=False, dir=output_dir, suffix='.csv')
                    try:
                        df_combined.to_csv(tmp.name, index=False)
                        tmp.close()
                        os.replace(tmp.name, csv_prerreq)
                    finally:
                        try:
                            if os.path.exists(tmp.name):
                                os.remove(tmp.name)
                        except Exception:
                            pass
                    print(f"Archivo Prerrequisitos.csv actualizado: {before_dedup} -> {after_dedup} registros (eliminados {before_dedup - after_dedup} duplicados)")
                else:
                    tmp = tempfile.NamedTemporaryFile(delete=False, dir=output_dir, suffix='.csv')
                    try:
                        df_new.to_csv(tmp.name, index=False)
                        tmp.close()
                        os.replace(tmp.name, csv_prerreq)
                    finally:
                        try:
                            if os.path.exists(tmp.name):
                                os.remove(tmp.name)
                        except Exception:
                            pass
                    print(f"Archivo Prerrequisitos.csv creado con {len(df_new)} registros")
            else:
                print("No hay prerrequisitos para procesar")
    
            # Limpiar las listas para la próxima asignatura
            self.asignaturas_data.clear()
            self.asignaturas_carrera_data.clear()
            self.horarios_data.clear()
            self.prerrequisitos_data.clear()
        except Exception as e:
            print(f"Error generando/actualizando CSVs: {e}")
        finally:
            # release file lock if we used it
            try:
                if lock_ctx:
                    lock_ctx.__exit__(None, None, None)
            except Exception:
                pass
    
    def generate_csvs(self, output_dir: str = "."):
        output_dir = "Data"
        """
        Genera los 3 archivos CSV con los datos extraídos (método original)
        
        Args:
            output_dir (str): Directorio donde guardar los archivos
        """

        try:
            # CSV 1: Asignaturas
            df_asignaturas = pd.DataFrame(self.asignaturas_data)
            df_asignaturas.to_csv(f"{output_dir}/Asignaturas.csv", index=False)
            print(f"Archivo Asignaturas.csv creado con {len(df_asignaturas)} registros")
            
            # CSV 2: AsignaturasCarrera
            self.prerrequisitos_data.clear()
            df_asignaturas_carrera = pd.DataFrame(self.asignaturas_carrera_data)
            df_asignaturas_carrera.to_csv(f"{output_dir}/AsignaturasCarrera.csv", index=False)
            print(f"Archivo AsignaturasCarrera.csv creado con {len(df_asignaturas_carrera)} registros")
            
            # CSV 3: Horarios
            df_horarios = pd.DataFrame(self.horarios_data)
            df_horarios.to_csv(f"{output_dir}/Horarios.csv", index=False)
            print(f"Archivo Horarios.csv creado con {len(df_horarios)} registros")

            # CSV 4: Prerrequisitos
            if self.prerrequisitos_data:
                df_prerreq = pd.DataFrame(self.prerrequisitos_data)
                df_prerreq.to_csv(f"{output_dir}/Prerrequisitos.csv", index=False)
                print(f"Archivo Prerrequisitos.csv creado con {len(df_prerreq)} registros")
            
        except Exception as e:
            print(f"Error generando CSVs: {e}")

    # Métodos originales mantenidos para compatibilidad
    def extract_asignatura_info(self, url: str) -> Dict:
        """
        Extrae toda la información de una asignatura desde su URL (método original)
        """
        try:
            self.driver.get(url)
            time.sleep(2)
            return self.extract_asignatura_info_from_driver(self.driver)
        except Exception as e:
            print(f"Error general extrayendo información: {e}")
            return None
    
    def process_asignatura(self, url: str = None, driver=None):
        """
        Procesa una asignatura completa y agrega los datos a las listas
        
        Args:
            url (str): URL de la asignatura (para uso independiente)
            driver: Driver externo ya posicionado (para integración)
        """
        if driver:
            # Usar driver externo
            info = self.extract_asignatura_info_from_driver(driver)
        elif url:
            # Usar URL (método original)
            print(f"Procesando: {url}")
            info = self.extract_asignatura_info(url)
        else:
            print("Error: Se debe proporcionar url o driver")
            return
        
        if not info:
            print("No se pudo extraer información de la asignatura")
            return
        
        self.add_asignatura_data(info)
        print(f"Procesada asignatura: {info['nombre']} ({info['codigo']})")
    
    def scrape_multiple_asignaturas(self, urls: List[str], output_dir: str = "."):
        """
        Scrапea múltiples asignaturas y genera los CSVs
        
        Args:
            urls (List[str]): Lista de URLs de asignaturas
            output_dir (str): Directorio donde guardar los archivos
        """
        try:
            self.setup_driver()
            
            for i, url in enumerate(urls, 1):
                print(f"\n--- Procesando asignatura {i}/{len(urls)} ---")
                self.process_asignatura(url)
                time.sleep(1)  # Pausa entre requests para no sobrecargar el servidor
            
            self.generate_csvs(output_dir)
            
        except Exception as e:
            print(f"Error durante el scraping: {e}")
        finally:
            if self.driver:
                self.driver.quit()
    
    def scrape_single_asignatura(self, url: str, output_dir: str = "."):
        """
        Scrапea una sola asignatura y genera los CSVs
        
        Args:
            url (str): URL de la asignatura
            output_dir (str): Directorio donde guardar los archivos
        """
        self.scrape_multiple_asignaturas([url], output_dir)


if __name__ == "__main__":
    print("Iniciando scraper de asignaturas...")
    # Ejecución independiente para pruebas
    scraper = AsignaturasScraper(headless=False)
    url = "https://sia.unal.edu.co/Catalogo/facespublico/public/servicioPublico.jsf?taskflowId=task-flow-AC_CatalogoAsignaturas#"
    scraper.process_asignatura(url)
    scraper.generate_csvs()
    print("Scraping completado. Revisa los archivos CSV generados.")