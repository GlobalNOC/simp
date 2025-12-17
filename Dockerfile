# Optimized single-stage build for containerized deployments
# For RPM building, use Dockerfile.rpmbuild instead
FROM oraclelinux:8

# set working directory
WORKDIR /opt/simp

# add globalnoc and epel repos, enable additional ol8 repos, and install all dependencies in one layer
RUN dnf install -y \
    https://build.grnoc.iu.edu/repo/rhel/8/x86_64/globalnoc-release-8-1.el8.noarch.rpm \
    oracle-epel-release-el8 \
    && yum-config-manager --enable \
    ol8_appstream ol8_baseos_latest ol8_codeready_builder \
    ol8_developer_EPEL  ol8_developer_EPEL_modular \
    && dnf install -y \
    net-snmp \
    net-snmp-devel \
    net-snmp-libs \
    net-snmp-utils \
    perl-IO-AIO \
    perl-Net-SNMP-XS \
    perl-GRNOC-Log \
    perl-GRNOC-Config \
    perl-GRNOC-RabbitMQ \
    perl-GRNOC-WebService-Client \
    perl-GRNOC-Monitoring-Service-Status \
    perl-App-cpanminus \
    redis \
    gcc \
    make \
    perl-devel \
    && dnf clean all \
    && cpanm --notest Carton

# copy only dependency files first for better layer caching
COPY cpanfile cpanfile.snapshot ./

# install Perl dependencies
RUN carton install --deployment --path=/opt/grnoc/venv/simp

# copy application code
COPY bin/ ./bin/
COPY lib/ ./lib/

# copy and set up configuration directories
RUN mkdir -p /etc/simp/poller /etc/simp/comp /etc/simp/data /etc/simp/tsds \
    && mkdir -p /var/lib/simp/poller

COPY conf/logging.conf /etc/simp/
COPY conf/poller/ /etc/simp/poller/
COPY conf/comp/ /etc/simp/comp/
COPY conf/data/ /etc/simp/data/
COPY conf/tsds/ /etc/simp/tsds/

# set up environment
ENV PERL5LIB=/opt/grnoc/venv/simp/lib/perl5:/opt/simp/lib

# set permissions for simp user (created by GRNOC packages)
RUN chown -R simp:simp /opt/simp /etc/simp /var/lib/simp

USER simp

# default entrypoint - can be overridden to run different services
# Examples:
#   docker run simp /opt/simp/bin/simp-poller.pl
ENTRYPOINT ["/bin/bash"]
CMD ["-c", "echo 'Welcome to SIMP! Available commands: simp-test.pl, simp-poller.pl, simp-comp.pl, simp-data.pl, simp-tsds.pl'"]