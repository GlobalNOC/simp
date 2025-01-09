FROM oraclelinux:8 AS rpmbuild

# set working directory
WORKDIR /app

# add globalnoc and epel repos
RUN dnf install -y \
    https://build.grnoc.iu.edu/repo/rhel/8/x86_64/globalnoc-release-8-1.el8.noarch.rpm \
    oracle-epel-release-el8

# enable additional ol8 repos
RUN yum-config-manager --enable \
    ol8_appstream ol8_baseos_latest ol8_codeready_builder \
    ol8_developer_EPEL  ol8_developer_EPEL_modular

RUN dnf install -y \
    openssl-devel perl-App-cpanminus expat-devel

RUN dnf install -y rpm-build perl-List-MoreUtils perl-AnyEvent net-snmp net-snmp-devel net-snmp-libs net-snmp-utils perl-Test-Deep perl-Test-Pod perl-Net-SNMP

# run makecache
RUN dnf makecache

RUN cpanm Carton

# copy everything in
COPY . /app

# build & install rpm
RUN make rpm


FROM oraclelinux:8

COPY --from=rpmbuild /root/rpmbuild/RPMS/noarch/simp-*.rpm /root/

RUN dnf install -y net-snmp net-snmp-devel net-snmp-libs net-snmp-utils

RUN dnf install -y \
    https://build.grnoc.iu.edu/repo/rhel/8/x86_64/globalnoc-release-8-1.el8.noarch.rpm \
    oracle-epel-release-el8

# enable additional ol8 repos
RUN yum-config-manager --enable \
    ol8_appstream ol8_baseos_latest ol8_codeready_builder \
    ol8_developer_EPEL  ol8_developer_EPEL_modular

RUN dnf install /root/*.rpm

# run makecache
RUN dnf makecache

RUN rm -rf /etc/simp/comp/composites.d
COPY composites.d /etc/simp/comp/composites.d

# set entrypoint
ENTRYPOINT ["/bin/echo", "'Welcome to SIMP!'"]