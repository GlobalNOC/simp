FROM oraclelinux:8 AS rpmbuild

# set working directory
WORKDIR /app

# add globalnoc and epel repos, enable additional ol8 repos, and install all build dependencies in one layer
RUN dnf install -y \
    https://build.grnoc.iu.edu/repo/rhel/8/x86_64/globalnoc-release-8-1.el8.noarch.rpm \
    oracle-epel-release-el8 \
    && yum-config-manager --enable \
    ol8_appstream ol8_baseos_latest ol8_codeready_builder \
    ol8_developer_EPEL  ol8_developer_EPEL_modular \
    && dnf install -y \
    openssl-devel \
    perl-App-cpanminus \
    expat-devel \
    rpm-build \
    perl-List-MoreUtils \
    perl-AnyEvent \
    net-snmp \
    net-snmp-devel \
    net-snmp-libs \
    net-snmp-utils \
    perl-Test-Deep \
    perl-Test-Pod \
    perl-Net-SNMP \
    perl-IO-AIO \
    && dnf makecache \
    && dnf clean all \
    && cpanm Carton

# copy only dependency files first for better layer caching
COPY cpanfile cpanfile.snapshot ./

# install Perl dependencies
RUN carton install --deployment --path=venv

# copy the rest of the application
COPY . /app

# build rpm
RUN make rpm


FROM oraclelinux:8

# add globalnoc and epel repos, enable additional ol8 repos, and install all runtime dependencies
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
    && dnf makecache \
    && dnf clean all

# copy RPMs from build stage
COPY --from=rpmbuild /root/rpmbuild/RPMS/noarch/simp-*.rpm /root/

# install simp RPMs
RUN dnf install -y /root/*.rpm && rm -rf /root/*.rpm

# copy configuration
RUN rm -rf /etc/simp/comp/composites.d
COPY composites.d /etc/simp/comp/composites.d
RUN rm -rf /etc/simp/poller/groups.dev
COPY groups.d /etc/simp/poller/groups.d

# set entrypoint
ENTRYPOINT ["/bin/echo", "'Welcome to SIMP!'"]