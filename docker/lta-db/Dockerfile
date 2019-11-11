FROM alpine:latest

RUN apk add --no-cache gcc git libffi-dev musl-dev openssl-dev python3-dev
RUN pip3 install --upgrade pip

COPY README.md requirements.txt setup.cfg setup.py /usr/src/lta/
COPY lta /usr/src/lta/lta
RUN pip install --no-cache-dir -r /usr/src/lta/requirements.txt

RUN addgroup -S app && adduser -S -g app app
USER app

WORKDIR /usr/src/lta
CMD ["python3", "-m", "lta.rest_server"]
