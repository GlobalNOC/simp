# Optimized single-stage build for containerized deployments
# Builds and installs SIMP RPMs to ensure identical installation to traditional deployments
FROM oraclelinux:8

# set working directory
WORKDIR /tmp/simp-build

COPY conf/poller/groups.d/*.xml /etc/simp/poller/groups.d/
COPY conf/comp/composites.d/*.xml /etc/simp/comp/composites.d/

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

RUN dnf install -y /root/*

RUN yes '' | cpan Net::AMQP::RabbitMQ

# run makecache
RUN dnf makecache

# set entrypoint
ENTRYPOINT ["echo", "SIMP container has started up!"]
