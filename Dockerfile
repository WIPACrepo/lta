# Dockerfile
FROM almalinux:9

RUN dnf install -y dnf-plugins-core wget && dnf clean all

# install GridFTP tools like globus-url-copy
RUN wget -O /etc/pki/rpm-gpg/RPM-GPG-KEY-EUGridPMA-RPM-4 \
    https://dist.eugridpma.info/distribution/igtf/current/GPG-KEY-EUGridPMA-RPM-4 \
    && rpm --import /etc/pki/rpm-gpg/RPM-GPG-KEY-EUGridPMA-RPM-4 \
    && cat <<EOF > /etc/yum.repos.d/igtf.repo
[igtf]
name=IGTF Accredited CAs
baseurl=http://dist.eugridpma.info/distribution/igtf/current/
enabled=1
gpgcheck=1
gpgkey=http://dist.eugridpma.info/distribution/igtf/current/GPG-KEY-EUGridPMA-RPM-4
EOF

RUN dnf install -y \
    ca_policy_igtf-classic \
    ca_policy_igtf-mics \
    ca_policy_igtf-slcs \
    ca_policy_igtf-iota \
    && dnf clean all

RUN wget https://downloads.globus.org/globus-connect-server/stable/installers/repo/rpm/globus-repo-6.0.33-1.noarch.rpm \
    && rpm -Uvh globus-repo-6.0.33-1.noarch.rpm \
    && sed -i 's/\\$releasever/9/g' /etc/yum.repos.d/globus*repo

RUN dnf install -y \
    globus-connect-server54 \
    globus-gass-copy-progs \
    globus-proxy-utils \
    && dnf clean all

# install voms-clients-cpp manually
RUN wget https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/Packages/v/voms-2.1.2-1.el9.x86_64.rpm \
    && wget https://dl.fedoraproject.org/pub/epel/9/Everything/x86_64/Packages/v/voms-clients-cpp-2.1.2-1.el9.x86_64.rpm \
    && dnf install -y ./voms-2.1.2-1.el9.x86_64.rpm ./voms-clients-cpp-2.1.2-1.el9.x86_64.rpm \
    && rm voms-2.1.2-1.el9.x86_64.rpm voms-clients-cpp-2.1.2-1.el9.x86_64.rpm \
    && dnf clean all

# install OSG tools
RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm && \
    yum install -y https://repo.opensciencegrid.org/osg/24-main/osg-24-main-el9-release-latest.rpm && \
    yum install -y osg-ca-certs && \
    dnf install -y --allowerasing python3.12 python3.12-pip git curl && \
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

RUN python3.12 -m venv $VIRTUAL_ENV

ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN --mount=source=.git,target=.git,type=bind pip install -e .[monitoring]

# by default, just show a friendly message
CMD ["python3", "-c", "print('Hello, LTA!')"]
