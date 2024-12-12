FROM python:3.9

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 requests



WORKDIR /app
COPY ingest_data.py ingest_data.py

# Entrypoint to run the script when the container starts.
# Python wil be run and then the first args (args[0]) will be passed as an argument to the script (which is ingest_data.py).
ENTRYPOINT [ "python", "ingest_data.py" ]