FROM python:3.10-bullseye

# install globus GridFTP tools like globus-url-copy
RUN wget -q -O - \
      https://dist.eugridpma.info/distribution/igtf/current/GPG-KEY-EUGridPMA-RPM-3 \
      | apt-key add -

RUN echo "deb http://dist.eugridpma.info/distribution/igtf/current igtf accredited" >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y \
    libglobus-gssapi-gsi-dev libglobus-common-dev \
    ca-policy-igtf-classic ca-policy-igtf-mics ca-policy-igtf-slcs ca-policy-igtf-iota \
    globus-gass-copy-progs globus-proxy-utils voms-clients \
    && apt-get clean

# install Long Term Archive (LTA) code
COPY README.md requirements.txt setup.cfg setup.py /usr/src/lta/
COPY lta /usr/src/lta/lta
RUN pip install --no-cache-dir -r /usr/src/lta/requirements.txt

RUN useradd -m -U app
USER app

WORKDIR /usr/src/lta
CMD ["python3", "-c", "print('Hello, LTA!')"]
