NAME=simp
VERSION=2.0.0

.PHONY: all

test: venv
	/usr/bin/perl -I ./lib -I ./venv/lib/perl5 t/TEST $(TEST_VERBOSE)

rpm: dist
	cd dist;
	rpmbuild -ta dist/simp-poller-$(VERSION).tar.gz
	rpmbuild -ta dist/simp-data-$(VERSION).tar.gz
	rpmbuild -ta dist/simp-comp-$(VERSION).tar.gz
	rpmbuild -ta dist/simp-tsds-$(VERSION).tar.gz
	rpmbuild -ta dist/simp-env-$(VERSION).tar.gz

clean:
	rm -rf dist/$(NAME)-$(VERSION)/
	rm -rf dist
	rm -rf venv

dist: clean venv
	mkdir -p dist/simp-data-$(VERSION)/lib/GRNOC/Simp
	mkdir -p dist/simp-data-$(VERSION)/bin
	mkdir -p dist/simp-data-$(VERSION)/conf/data
	mkdir -p dist/simp-comp-$(VERSION)/lib/GRNOC/Simp
	mkdir -p dist/simp-comp-$(VERSION)/bin
	mkdir -p dist/simp-comp-$(VERSION)/scripts
	mkdir -p dist/simp-comp-$(VERSION)/conf/comp
	mkdir -p dist/simp-poller-$(VERSION)/lib/GRNOC/Simp
	mkdir -p dist/simp-poller-$(VERSION)/bin
	mkdir -p dist/simp-poller-$(VERSION)/conf/poller/hosts.d
	mkdir -p dist/simp-tsds-$(VERSION)/lib/GRNOC/Simp
	mkdir -p dist/simp-tsds-$(VERSION)/bin
	mkdir -p dist/simp-tsds-$(VERSION)/conf/tsds/collections.d
	mkdir -p dist/simp-env-$(VERSION)/

	cp -r lib/GRNOC/Simp/Poller* dist/simp-poller-$(VERSION)/lib/GRNOC/Simp
	cp -r lib/GRNOC/Simp/Data* dist/simp-data-$(VERSION)/lib/GRNOC/Simp
	cp -r lib/GRNOC/Simp/Comp* dist/simp-comp-$(VERSION)/lib/GRNOC/Simp
	cp -r lib/GRNOC/Simp/TSDS* dist/simp-tsds-$(VERSION)/lib/GRNOC/Simp

	cp -r bin/simp-poller.pl dist/simp-poller-$(VERSION)/bin/
	cp -r bin/simp-data.pl dist/simp-data-$(VERSION)/bin/
	cp -r bin/simp-comp.pl dist/simp-comp-$(VERSION)/bin/
	cp -r bin/simp-tsds.pl dist/simp-tsds-$(VERSION)/bin/
	cp -r bin/simp-test.pl dist/simp-comp-$(VERSION)/scripts

	cp -r conf/poller/config.xml dist/simp-poller-$(VERSION)/conf/poller/
	cp -r conf/poller/validation.d dist/simp-poller-$(VERSION)/conf/poller/
	cp -r conf/poller/simp-poller.systemd dist/simp-poller-$(VERSION)/conf/poller/
	cp -r conf/poller/simp-poller.service dist/simp-poller-$(VERSION)/conf/poller/
	cp -r conf/poller/hosts.d/hosts.xml.example dist/simp-poller-$(VERSION)/conf/poller/hosts.d/
	cp -r conf/poller/groups.d dist/simp-poller-$(VERSION)/conf/poller/

	cp -r conf/data/config.xml dist/simp-data-$(VERSION)/conf/data/
	cp -r conf/data/validation.d dist/simp-data-$(VERSION)/conf/data/
	cp -r conf/data/simp-data.systemd dist/simp-data-$(VERSION)/conf/data/
	cp -r conf/data/simp-data.service dist/simp-data-$(VERSION)/conf/data/

	cp -r conf/comp/config.xml dist/simp-comp-$(VERSION)/conf/comp/
	cp -r conf/comp/validation.d dist/simp-comp-$(VERSION)/conf/comp/
	cp -r conf/comp/composites.d dist/simp-comp-$(VERSION)/conf/comp/
	cp -r conf/comp/simp-comp.systemd dist/simp-comp-$(VERSION)/conf/comp/
	cp -r conf/comp/simp-comp.service dist/simp-comp-$(VERSION)/conf/comp/

	cp -r conf/tsds/config.xml dist/simp-tsds-$(VERSION)/conf/tsds/
	cp -r conf/tsds/validation.d dist/simp-tsds-$(VERSION)/conf/tsds/
	cp -r conf/tsds/collections.d/collection.xml.example dist/simp-tsds-$(VERSION)/conf/tsds/collections.d/
	cp -r conf/tsds/simp-tsds.systemd dist/simp-tsds-$(VERSION)/conf/tsds/
	cp -r conf/tsds/simp-tsds.service dist/simp-tsds-$(VERSION)/conf/tsds/

	cp -r conf/logging.conf dist/simp-poller-$(VERSION)/conf/
	cp -r conf/logging.conf dist/simp-data-$(VERSION)/conf/
	cp -r conf/logging.conf dist/simp-comp-$(VERSION)/conf/
	cp -r conf/logging.conf dist/simp-tsds-$(VERSION)/conf/

	cp -r spec/simp-data.spec dist/simp-data-$(VERSION)/
	cp -r spec/simp-comp.spec dist/simp-comp-$(VERSION)/
	cp -r spec/simp-poller.spec dist/simp-poller-$(VERSION)/
	cp -r spec/simp-tsds.spec dist/simp-tsds-$(VERSION)/

	cp -r spec/simp-env.spec dist/simp-env-$(VERSION)/
	cp -r venv/ dist/simp-env-$(VERSION)/

	cd dist; tar -czvf simp-poller-$(VERSION).tar.gz simp-poller-$(VERSION)/
	cd dist; tar -czvf simp-data-$(VERSION).tar.gz simp-data-$(VERSION)/ 
	cd dist; tar -czvf simp-comp-$(VERSION).tar.gz simp-comp-$(VERSION)/ 
	cd dist; tar -czvf simp-tsds-$(VERSION).tar.gz simp-tsds-$(VERSION)/ 
	cd dist; tar -czvf simp-env-$(VERSION).tar.gz simp-env-$(VERSION)/ 
venv:
	carton install --deployment --path=venv
	cpanm --notest -L venv Net::SNMP::XS
