# AlmaLinux 9 base
FROM almalinux:9

# Make sure dnf can use HTTPS and certs
RUN dnf -y update && dnf -y install ca-certificates && update-ca-trust

RUN yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm && \
    yum install -y https://repo.opensciencegrid.org/osg/24-main/osg-24-main-el9-release-latest.rpm && \
    yum install -y osg-ca-certs && \
    dnf install -y --allowerasing python3.12 python3.12-pip git curl && \
    dnf clean all && yum clean all
