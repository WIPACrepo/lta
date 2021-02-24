FROM python:latest

RUN useradd -m -U app

RUN pip3 install --upgrade pip

COPY README.md requirements.txt setup.cfg setup.py /usr/src/lta/
COPY lta /usr/src/lta/lta
RUN pip install --no-cache-dir -r /usr/src/lta/requirements.txt

USER app

WORKDIR /usr/src/lta
CMD ["python3", "-c", "print('Hello, LTA!')"]
