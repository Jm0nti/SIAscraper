# SIAscraper

Este proyecto permite extraer, unificar y procesar información académica de diferentes facultades de la Universidad Nacional de Colombia sede Medellín a partir de información disponible en su buscador de cursos. El objetivo es centralizar y facilitar el acceso a datos como asignaturas, horarios, prerrequisitos y relaciones entre carreras y asignaturas.

## Estructura del Proyecto

- `main.py`: Script principal para ejecutar el flujo general del proyecto.
- `Data/`: Carpeta que contiene los datos unificados y los datos originales por facultad.
  - `unified_Asignaturas.csv`, `unified_AsignaturasCarrera.csv`, `unified_Horarios.csv`, `unified_Prerrequisitos.csv`: Archivos unificados de todas las facultades.
  - `unifier.py`: Script para unificar los datos de las carpetas de diferentes facultades.
  - `Facultad_*`: Carpetas con los archivos CSV originales de cada facultad.
- `src/`: Código fuente del scraper y utilidades.
  - `botAgrarias.py`, `botArquitectura.py`, `botCiencias.py`, `botFCHE.py`, `botMinas.py`, `botMinas2.py`: Scrapers específicos para cada facultad.
  - `scraper.py`: Lógica común de scraping.
  - `utils.py`: Listas auxiliares de facultades y carreras.
  - `writer.py`: Funciones para escribir los datos en archivos.
  - `chromedriver.exe`: Driver para automatizar la navegación web con Selenium.

## Requisitos

- Python 3.10+
- Google Chrome y el driver correspondiente (`chromedriver.exe`)

## Instalación de dependencias

Instala las dependencias necesarias ejecutando:

```bash
pip install -r requirements.txt
```

## Uso

1. Ejecuta el main que llama a ejecución los 5 bots de forma simultánea, esta ejecución puede tardar al rededor de 1h
   ```bash
   python main.py 
   ...
   ```
2. Una vez extraida la información por facultades, unifica los datos ejecutando:
   ```bash
   python Data/unifier.py
   ```


## Notas
- El proyecto está pensado para uso académico y de investigación.

## Autor
- Juan Montiel

## Licencia
Este proyecto se distribuye bajo la Licencia MIT.
