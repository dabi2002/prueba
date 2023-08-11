# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer un directorio de trabajo
WORKDIR /app

RUN pip install --upgrade pip

# Instalar las dependencias
RUN pip install pandas
#RUN pip install logging


# Copiar los scripts al contenedor
COPY durationbyTrxHome.py .
COPY consolidate.py .

# Entrypoint para ejecutar los scripts en orden
ENTRYPOINT ["sh", "-c", "python durationbyTrxHome.py && consolidate.py"]