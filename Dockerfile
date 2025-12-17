# Optimized single-stage build for containerized deployments
# Builds and installs SIMP RPMs to ensure identical installation to traditional deployments
FROM oraclelinux:8

# set working directory
WORKDIR /tmp/simp-build

# add globalnoc and epel repos, enable additional ol8 repos, and install all dependencies in one layer
RUN dnf install -y \
    https://build.grnoc.iu.edu/repo/rhel/8/x86_64/globalnoc-release-8-1.el8.noarch.rpm \
    oracle-epel-release-el8 \
    && yum-config-manager --enable \
    ol8_appstream ol8_baseos_latest ol8_codeready_builder \
    ol8_developer_EPEL  ol8_developer_EPEL_modular \
    && dnf install -y \
    gcc \
    make \
    rpm-build \
    openssl-devel \
    expat-devel \
    perl-devel \
    perl-App-cpanminus \
    perl-List-MoreUtils \
    perl-AnyEvent \
    perl-IO-AIO \
    perl-Net-SNMP \
    perl-Net-SNMP-XS \
    perl-Test-Deep \
    perl-Test-Pod \
    perl-GRNOC-Log \
    perl-GRNOC-Config \
    perl-GRNOC-RabbitMQ \
    perl-GRNOC-WebService-Client \
    perl-GRNOC-Monitoring-Service-Status \
    net-snmp \
    net-snmp-devel \
    net-snmp-libs \
    net-snmp-utils \
    redis \
    && dnf clean all \
    && cpanm --notest Carton

# copy source files
COPY . /tmp/simp-build/

# build RPMs using the Makefile
RUN make rpm

# install the RPMs

RUN dnf install -y \
    /root/rpmbuild/RPMS/x86_64/simp-*.rpm \
    /root/rpmbuild/RPMS/noarch/simp-*.rpm

# cleanup build artifacts
RUN rm -rf /tmp/simp-build /root/rpmbuild

# set working directory to standard location
WORKDIR /

# set up environment
ENV PERL5LIB=/opt/grnoc/venv/simp/lib/perl5

# default entrypoint - can be overridden to run different services
# Examples:
#   docker run simp simp-poller.pl
#   docker run simp simp-comp.pl
ENTRYPOINT ["/bin/bash"]
CMD ["-c", "echo 'Welcome to SIMP! Available commands: simp-poller.pl, simp-comp.pl, simp-data.pl, simp-tsds.pl'"]