# Dockerfile
FROM almalinux:9

ARG PYTHON=3.13

# add an enterprise linux repo that provides Python packages
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm && \
    dnf clean all && yum clean all

# install Python and other snakes
RUN dnf install -y --allowerasing curl git python${PYTHON} python${PYTHON}-pip && \
    dnf clean all && yum clean all

# copy our project into the container
RUN useradd -m -U app
RUN mkdir /app

WORKDIR /app

COPY lta /app/lta
COPY resources /app/resources
COPY README.md pyproject.toml setup.py /app/

RUN chown -R app:app /app

# install our application with dependencies
USER app

ENV VIRTUAL_ENV=/app/venv
RUN python${PYTHON} -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN --mount=source=.git,target=.git,type=bind pip install -e .[monitoring]

# by default, just show a friendly message
CMD ["python3", "-c", "print('Hello, LTA!')"]
