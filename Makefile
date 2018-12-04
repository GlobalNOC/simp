NAME=simp
VERSION=1.0.10
.PHONY: dist
rpm:	dist
	rpmbuild -ta dist/simp-poller-$(VERSION).tar.gz
	rpmbuild -ta dist/simp-data-$(VERSION).tar.gz
	rpmbuild -ta dist/simp-comp-$(VERSION).tar.gz
	rpmbuild -ta dist/simp-tsds-$(VERSION).tar.gz
	rpmbuild -ta dist/simp-monitor-$(VERSION).tar.gz

test:
	/usr/bin/perl -I ./lib t/TEST $(TEST_VERBOSE)

test_jenkins:
	/usr/bin/perl -I ./lib t/TEST $(TEST_VERBOSE) --formatter=TAP::Formatter::Console

clean:
	rm -rf dist
	rm -rf tap
	rm -rf cover_db

dist:
	rm -rf dist
	mkdir -p dist/simp-data-$(VERSION)/lib/GRNOC/Simp
	mkdir -p dist/simp-data-$(VERSION)/bin
	mkdir -p dist/simp-data-$(VERSION)/conf
	mkdir -p dist/simp-comp-$(VERSION)/lib/GRNOC/Simp
	mkdir -p dist/simp-comp-$(VERSION)/bin
	mkdir -p dist/simp-comp-$(VERSION)/conf
	mkdir -p dist/simp-poller-$(VERSION)/lib/GRNOC/Simp
	mkdir -p dist/simp-poller-$(VERSION)/bin
	mkdir -p dist/simp-poller-$(VERSION)/conf/hosts.d
	mkdir -p dist/simp-tsds-$(VERSION)/lib/GRNOC/Simp
	mkdir -p dist/simp-tsds-$(VERSION)/bin
	mkdir -p dist/simp-tsds-$(VERSION)/conf
	mkdir -p dist/simp-monitor-$(VERSION)/cgi-dir
	mkdir -p dist/simp-monitor-$(VERSION)/simp_monitor
	mkdir -p dist/simp-monitor-$(VERSION)/conf
	mkdir -p dist/simp-monitor-$(VERSION)/bin

	cp -r lib/GRNOC/Simp/Poller* dist/simp-poller-$(VERSION)/lib/GRNOC/Simp
	cp -r lib/GRNOC/Simp/Data* dist/simp-data-$(VERSION)/lib/GRNOC/Simp
	cp -r lib/GRNOC/Simp/Comp* dist/simp-comp-$(VERSION)/lib/GRNOC/Simp
	cp -r lib/GRNOC/Simp/TSDS* dist/simp-tsds-$(VERSION)/lib/GRNOC/Simp
	cp -r bin/simp-comp.pl dist/simp-comp-$(VERSION)/bin/
	cp -r bin/simp-data.pl dist/simp-data-$(VERSION)/bin/
	cp -r bin/simp-poller.pl dist/simp-poller-$(VERSION)/bin/
	cp -r bin/simp-tsds.pl dist/simp-tsds-$(VERSION)/bin/
	cp -r bin/response_timer.pl dist/simp-monitor-$(VERSION)/bin/
	cp -r bin/redis_tsds.pl dist/simp-monitor-$(VERSION)/bin/

	cp -r conf/compDataConfig.xml dist/simp-comp-$(VERSION)/conf/
	cp -r conf/logging.conf dist/simp-comp-$(VERSION)/conf/
	cp -r conf/simp-comp.systemd dist/simp-comp-$(VERSION)/conf/
	cp -r conf/simpDataConfig.xml dist/simp-data-$(VERSION)/conf/
	cp -r conf/logging.conf dist/simp-data-$(VERSION)/conf/
	cp -r conf/simp-data.systemd dist/simp-data-$(VERSION)/conf/
	cp -r conf/config.xml dist/simp-poller-$(VERSION)/conf/
	cp -r conf/logging.conf dist/simp-poller-$(VERSION)/conf/
	cp -r conf/simp-poller.systemd dist/simp-poller-$(VERSION)/conf/
	cp -r conf/hosts.d/* dist/simp-poller-$(VERSION)/conf/hosts.d/
	cp -r conf/simp-tsds.xml dist/simp-tsds-$(VERSION)/conf/
	cp -r conf/logging.conf dist/simp-tsds-$(VERSION)/conf/
	cp -r conf/simp-tsds.systemd dist/simp-tsds-$(VERSION)/conf/

	cp -r conf/response-graph-config.xml dist/simp-monitor-$(VERSION)/conf/
	cp -r conf/response-monitor-log.conf dist/simp-monitor-$(VERSION)/conf/
	cp -r conf/simp_monitor_http.conf dist/simp-monitor-$(VERSION)/conf/
	cp -r conf/redis_config.xml dist/simp-monitor-$(VERSION)/conf/
	cp -r conf/redis_tsds_cron dist/simp-monitor-$(VERSION)/conf/
	cp -r conf/redis_log.conf dist/simp-monitor-$(VERSION)/conf/


	cp -r simp-data.spec dist/simp-data-$(VERSION)/
	cp -r simp-comp.spec dist/simp-comp-$(VERSION)/
	cp -r simp-poller.spec dist/simp-poller-$(VERSION)/
	cp -r simp-tsds.spec dist/simp-tsds-$(VERSION)/
	cp -r simp-monitor.spec dist/simp-monitor-$(VERSION)/
	cp -r conf/sysconfig dist/simp-tsds-$(VERSION)/conf/

	cp -r simp_comp_webservice/cgi-dir/* dist/simp-monitor-$(VERSION)/cgi-dir/
	cp -r simp_comp_webservice/simp_monitor/* dist/simp-monitor-$(VERSION)/simp_monitor/

	cd dist; tar -czvf simp-poller-$(VERSION).tar.gz simp-poller-$(VERSION)/ --exclude .svn --exclude .git
	cd dist; tar -czvf simp-data-$(VERSION).tar.gz simp-data-$(VERSION)/ --exclude .svn --exclude .git
	cd dist; tar -czvf simp-comp-$(VERSION).tar.gz simp-comp-$(VERSION)/ --exclude .svn --exclude .git
	cd dist; tar -czvf simp-tsds-$(VERSION).tar.gz simp-tsds-$(VERSION)/ --exclude .svn --exclude .git
	cd dist; tar -czvf simp-monitor-$(VERSION).tar.gz simp-monitor-$(VERSION)/ --exclude .svn --exclude .git

