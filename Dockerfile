FROM python:3.10-bullseye

# install globus GridFTP tools like globus-url-copy
RUN wget -q -O - \
    https://dist.eugridpma.info/distribution/igtf/current/GPG-KEY-EUGridPMA-RPM-4 \
    | tee /etc/apt/trusted.gpg.d/GPG-KEY-EUGridPMA-RPM-4.asc

RUN echo "deb http://dist.eugridpma.info/distribution/igtf/current igtf accredited" >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    libglobus-gssapi-gsi-dev libglobus-common-dev \
    ca-policy-igtf-classic ca-policy-igtf-mics ca-policy-igtf-slcs ca-policy-igtf-iota \
    globus-gass-copy-progs globus-proxy-utils voms-clients \
    && apt-get clean

# install Long Term Archive (LTA) code
COPY README.md setup.cfg setup.py /usr/src/lta/
COPY lta /usr/src/lta/lta
COPY resources /usr/src/lta/resources
RUN pip install --no-cache-dir /usr/src/lta[monitoring]

RUN useradd -m -U app
USER app

WORKDIR /usr/src/lta
CMD ["python3", "-c", "print('Hello, LTA!')"]
