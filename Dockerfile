FROM python:latest

COPY README.md requirements.txt setup.cfg setup.py /usr/src/lta/
COPY lta /usr/src/lta/lta
RUN pip install --no-cache-dir -r /usr/src/lta/requirements.txt

RUN useradd -m -U app
USER app

WORKDIR /usr/src/lta
CMD ["python3", "-c", "print('Hello, LTA!')"]
