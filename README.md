# Cómo desarrollar un Pipeline de ETL

La sigla ETL significa Extract, Transform and Load, al desarrollar un pipeline de ETL creamos uno o varios scripts que sean capaces de concatenar estos pasos fundamentales para:

1. Conectarse a una fuente de datos:
    * Archivos planos `.csv`, `.xlsx`, `.tsv`, `.txt`, `.zip`, etc.
    * API (Application Programming Interface) con retorno en formato `JSON` o `XML` entre otros.
    * Query en una base de datos en lenguaje `SQL`.

2. Aplicar distintas transformaciones a los datos para limpiarlos (`Formato Tidy`) y modelarlos como una base de datos relacional si es necesario.

3. Ingestar los datos en una base de datos, ya sea local o remota.

# Temas a revisar

* **Git Basics**:
    * ¿Qué es Git? [(Link 1](https://rogerdudler.github.io/git-guide/), [Link 2)](https://dev.to/doylecodes/git-for-dummies-1a2i)
    * Clonar un repositorio: `git clone https://github.com/innerstage/etl-lesson.git`
    * Actualizar repositorio clonado: `git fetch origin && git pull`
    * Crear una `branch` personal
    * Hacer un `commit`
    * Hacer un `PR` o `Pull Request`

* **Python**:
    * Dudas sobre Python
    * Entornos virtuales
    * Creación de un entorno virtual: `virtualenv -p python3 py3`
    * Activar entorno virtual: `source py3/bin/activate`
    * Instalar librerías: `pip install -r requirements.txt`

* **Data Wrangling**:
    * ¿Qué es Tidy Data? [(Paper)](https://vita.had.co.nz/papers/tidy-data.pdf)
    * Principios:
        1. Cada variable forma una columna.
        2. Cada observación forma una fila.
        3. Cada tipo de unidad observacional forma una tabla.
    * Operaciones Vectorizadas con `pandas` [(Link)](https://engineering.upside.com/a-beginners-guide-to-optimizing-pandas-code-for-speed-c09ef2c6a4d6)

* **Bases de Datos**:
    * Instalación de DBeaver [(Link)](https://dbeaver.io/)
    * Creación de una instancia local de PostgreSQL [(Link)](https://www.postgresql.org/)
    * Conexión con DBeaver
    * Crear archivo `.env` con credenciales.

* **Bamboo-lib**:
    * ¿Qué es Bamboo?
    * Basics:
        * Steps
        * Parameters
        * Connections
    * Proyecto:
        * Wakanda Trade
        * OEC Trade

# Hola, soy GRJ
