from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
import time
from src.utils import Carreras_F_Arquitectura
import os

class AsignaturaExtractor:
    def __init__(self, driver_path='src/chromedriver.exe', headless=False):
        self.driver = None
        self.headless = headless
        self.setup_driver(driver_path)
    
    def setup_driver(self, driver_path):
        options = webdriver.ChromeOptions()
        if getattr(self, 'headless', False):
            options.add_argument('--headless=new')
        # Opciones para evitar interacción/manual focus y mantener tamaño razonable
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-infobars")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--window-size=1280,800")
        service = Service(executable_path=driver_path)
        self.driver = webdriver.Chrome(service=service, options=options)
        # Intentar forzar foco de la ventana desde JS
        try:
            self.driver.execute_script("window.focus();")
        except Exception:
            pass

    def safe_click(self, element):
        """Click con fallbacks: element.click(), JS click y ActionChains."""
        try:
            element.click()
            return
        except Exception:
            pass
        try:
            self.driver.execute_script("arguments[0].click();", element)
            return
        except Exception:
            pass
        try:
            from selenium.webdriver import ActionChains
            ActionChains(self.driver).move_to_element(element).click().perform()
        except Exception:
            pass
    
    def configure_filters(self, nivel_estudio="Pregrado", sede="1102 SEDE MEDELLÍN", 
                         facultad="3064 FACULTAD DE ARQUITECTURA", carrera=str,
                         tipo_asignatura="TODAS MENOS LIBRE ELECCIÓN"):

        try:
            print("Configurando filtros de búsqueda...")
            
            print(f"Seleccionando nivel de estudio: {nivel_estudio}")
            nivel_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "pt1:r1:0:soc1::content"))
            )
            select_nivel = Select(nivel_element)
            select_nivel.select_by_visible_text(nivel_estudio)
            time.sleep(1)
            
            print(f"Seleccionando sede: {sede}")
            sede_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "pt1:r1:0:soc9::content"))
            )
            select_sede = Select(sede_element)
            select_sede.select_by_visible_text(sede)
            time.sleep(1)
            
            print(f"Seleccionando facultad: {facultad}")
            facultad_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "pt1:r1:0:soc2::content"))
            )
            select_facultad = Select(facultad_element)
            select_facultad.select_by_visible_text(facultad)
            time.sleep(1)
            
            print(f"Seleccionando carrera: {carrera}")
            carrera_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "pt1:r1:0:soc3::content"))
            )
            select_carrera = Select(carrera_element)
            select_carrera.select_by_visible_text(carrera)
            time.sleep(1)
            
            print(f"Seleccionando tipo de asignatura: {tipo_asignatura}")
            tipo_element = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "pt1:r1:0:soc4::content"))
            )
            select_tipo = Select(tipo_element)
            select_tipo.select_by_visible_text(tipo_asignatura)
            time.sleep(1)
            
            print("Haciendo clic en el botón Mostrar...")
            boton_mostrar = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "af_button_link"))
            )
            self.safe_click(boton_mostrar)
            time.sleep(3)  # Esperar a que se actualicen los resultados
            
            print("Filtros configurados correctamente")
            return True
            
        except TimeoutException:
            print("Error: Tiempo de espera agotado configurando filtros")
            return False
        except Exception as e:
            print(f"Error configurando filtros: {e}")
            return False
    
    # Espera a que la tabla de asignaturas cargue
    def wait_for_table(self, timeout=10):

        try:
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "tr.af_table_data-row"))
            )
            print("Tabla de asignaturas cargada correctamente")
            return True
        except TimeoutException:
            print("Error: No se pudo cargar la tabla de asignaturas")
            return False
    
    # Extrae info de asignaturas de la tabla
    def extract_asignaturas(self):
        asignaturas = []
        asignaturas_omitidas = []
        
        try:
            # Buscar todas las filas de la tabla que contienen datos de asignaturas
            filas = self.driver.find_elements(By.CSS_SELECTOR, "tr.af_table_data-row")
            
            print(f"Se encontraron {len(filas)} filas en total")
            
            for i, fila in enumerate(filas, 1):
                try:
                    # Extraer el código de la asignatura 
                    codigo_element = fila.find_element(By.CSS_SELECTOR, "td:nth-child(1) a.af_commandLink")
                    codigo = codigo_element.text.strip()
                    
                    # Verificar si la asignatura está programada 
                    segunda_columna = fila.find_element(By.CSS_SELECTOR, "td:nth-child(2)")
                    texto_completo_columna = segunda_columna.text.strip()
                    
                    
                    if "ASIGNATURA SIN PROGRAMAR" in texto_completo_columna:
                        # Extraer el nombre para el reporte de omitidas
                        nombre_element = fila.find_element(By.CSS_SELECTOR, "td:nth-child(2) span[title]")
                        nombre = nombre_element.get_attribute('title').strip()
                        if not nombre:
                            nombre = nombre_element.text.strip().replace("ASIGNATURA SIN PROGRAMAR", "").strip()
                        
                        asignaturas_omitidas.append({
                            'codigo': codigo,
                            'nombre': nombre,
                            'razon': 'Sin programar'
                        })
                        print(f"❌ Asignatura omitida (sin programar): {codigo} - {nombre}")
                        continue
                    
                    # Extraer el nombre de la asignatura 
                    nombre_element = fila.find_element(By.CSS_SELECTOR, "td:nth-child(2) span[title]")
                    nombre = nombre_element.get_attribute('title').strip()
                    if not nombre:  # Si el title está vacío, usar el texto
                        nombre = nombre_element.text.strip()
                    
                    # Extraer el número de créditos
                    creditos_element = fila.find_element(By.CSS_SELECTOR, "td:nth-child(3) span[title]")
                    creditos = creditos_element.get_attribute('title').strip()
                    if not creditos:  # Si el title está vacío, usar el texto
                        creditos = creditos_element.text.strip()
                    
                    # Extraer tipo de asignatura 
                    tipo_element = fila.find_element(By.CSS_SELECTOR, "td:nth-child(4) span[title]")
                    tipo = tipo_element.text.strip()
                    if not tipo:  # Si el title está vacío, usar el texto
                        tipo = tipo_element.text.strip()

                    
                    asignatura = {
                        'codigo': codigo,
                        'nombre': nombre,
                        'creditos': int(creditos) if creditos.isdigit() else creditos,
                        'tipo': tipo
                    }
                    
                    asignaturas.append(asignatura)
                    print(f"✅ Asignatura {len(asignaturas)}: {codigo} - {nombre} ({creditos} créditos)")
                    
                except NoSuchElementException as e:
                    print(f"Error extrayendo datos de la fila {i}: {e}")
                    continue
                except Exception as e:
                    print(f"Error inesperado en la fila {i}: {e}")
                    continue
            
            
            if asignaturas_omitidas:
                print(f"\n📊 RESUMEN DE FILTRADO:")
                print(f"   - Asignaturas programadas: {len(asignaturas)}")
                print(f"   - Asignaturas omitidas: {len(asignaturas_omitidas)}")
                print(f"   - Total procesadas: {len(filas)}")
            
            return asignaturas
            
        except Exception as e:
            print(f"Error general extrayendo asignaturas: {e}")
            return []
     
    
    def close(self):
        """Cierra el driver"""
        if self.driver:
            self.driver.quit()

def main(headless=False, writer_queue=None):
    """Función principal de ejemplo"""
    

    # URL de la página
    url = "https://sia.unal.edu.co/Catalogo/facespublico/public/servicioPublico.jsf?taskflowId=task-flow-AC_CatalogoAsignaturas"

    # Crear instancia del extractor
    extractor = AsignaturaExtractor('src/chromedriver.exe', headless=headless)

    try:
        from src.scraper import scrape_asignatura_from_driver
        for idx_carrera, carrera in enumerate(Carreras_F_Arquitectura, 1):
            print(f"\n==============================")
            print(f"Procesando carrera {idx_carrera}/{len(Carreras_F_Arquitectura)}: {carrera}")
            print(f"Navegando a: {url}")
            extractor.driver.get(url)
            # Configurar filtros de búsqueda para la carrera actual
            if extractor.configure_filters(carrera=carrera):
                # Esperar a que cargue la tabla
                if extractor.wait_for_table():
                    print("\n=== Intentando extracción principal ===")
                    asignaturas = extractor.extract_asignaturas()

                    if asignaturas:
                        print(f"\n=== RESUMEN ({carrera}) ===")
                        print(f"Total de asignaturas extraídas: {len(asignaturas)}")
                        total_creditos = sum(a['creditos'] for a in asignaturas if isinstance(a['creditos'], int))
                        print(f"Total de créditos: {total_creditos}")

                        # Recorrer cada asignatura programada y hacer clic en el código
                        for idx, asignatura in enumerate(asignaturas, 1):
                            print(f"\n➡️ Procesando asignatura {idx}/{len(asignaturas)}: {asignatura['codigo']} - {asignatura['nombre']}")
                            try:
                                # Buscar el enlace por código en la tabla actual
                                enlace = extractor.driver.find_element(By.LINK_TEXT, asignatura['codigo'])
                                extractor.safe_click(enlace)
                                time.sleep(1)

                                # Usar la función del modulo scraper y guardar por facultad
                                out_dir = os.path.join("Data", "Facultad_Arquitectura")
                                os.makedirs(out_dir, exist_ok=True)
                                scrape_asignatura_from_driver(extractor.driver, output_dir=out_dir, writer_queue=writer_queue)
                                time.sleep(1)

                                print(f"✅ Asignatura {asignatura['codigo']} procesada correctamente")
                                
                                # Vuelve a tabla de asignaturas
                                boton_atras = extractor.driver.find_element(By.CLASS_NAME, "af_button_text")
                                extractor.safe_click(boton_atras)
                                time.sleep(3)
                            except Exception as e:
                                print(f"Error procesando asignatura: {e}")
                    else:
                        print(f"No se pudieron extraer asignaturas para la carrera {carrera}")
                else:
                    print(f"No se pudo cargar la tabla de resultados para la carrera {carrera}")
            else:
                print(f"No se pudieron configurar los filtros para la carrera {carrera}")

    except Exception as e:
        print(f"Error en la ejecución principal: {e}")

    finally:
        extractor.close()

if __name__ == "__main__":
    main()