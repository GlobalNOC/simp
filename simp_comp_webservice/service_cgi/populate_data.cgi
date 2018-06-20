#!/usr/bin/perl
use strict;
use Time::HiRes qw(usleep gettimeofday tv_interval);
use GRNOC::WebService;
use JSON;
use Data::Dumper;
#use GRNOC::RabbitMQ::Client;
#use AnyEvent;

#------ Variables
my $method_obj;
my $params;
my $host;
my $oid;


#------ Callback
sub get_initial_data {
    my %results;
    my %host_hash;
    my %group_hash;
    $method_obj = shift;
    $params = shift;

    $results{'hosts'};
    $results{'groups'};
    opendir(DIR, "./hosts.d") or die "Error opening the directory";
    my @files = grep(/\.xml$/,readdir(DIR));
    closedir(DIR);

    # Iterating over each file in hosts.d folder
    foreach my $file (@files) {

        my $config = GRNOC::Config->new(config_file => "./hosts.d/$file", force_array => 0, debug => 0);
        my $hosts= $config->get("/config/host");

        # Array and hash handling of perl object
        if (ref($hosts) eq 'HASH') {

            # Add if it does not exists to maintain uniqueness
            if (!exists($host_hash{$hosts->{'ip'}})) {
                $host_hash{$hosts->{'ip'}} = 1;
            }
            my $groups = $config->get("/config/host/group");
            # Array and hash handling of perl object
            if (ref($groups) eq 'HASH'){
                if (!exists($group_hash{$groups->{'name'}})) {
                    $group_hash{$groups->{'name'}} = 1;
                }
            } else {
                foreach my $group (@$groups){
                    # warn Dumper($group);
                    if (!exists($group_hash{$group->{'name'}})) {
                        $group_hash{$group->{'name'}} = 1;
                    }
                }
            }
        } else  {
            foreach my $host (@$hosts){
                my $id = $host->{'ip'};
                if (!exists($host_hash{$host->{'ip'}})) {
                    $host_hash{$host->{'ip'}} = 1;
                }

            }
            my $groups = $config->get("/config/host/group");

            if (ref($groups) eq 'HASH') {
                warn Dumper("GROUPS HASH 2");
            }
            foreach my $group (@$groups){
                # warn Dumper($group);
                if (!exists($group_hash{$group->{'id'}})) {
                    $group_hash{$group->{'id'}} = 1;
                }
            }
        }
    }

    $results{'hosts'} = [keys %host_hash];
    $results{'groups'} = [keys %group_hash];

    return \%results;
}


#------ wrap callback in service method object
my $get_initial_data = GRNOC::WebService::Method->new(

    name => "get_initial_data",
    description => "descr",
    callback => \&get_initial_data
);


#------ Callback
sub get_hosts {
    my %results;
    my %host_hash;
    my %group_hash;
    $method_obj = shift;
    $params = shift;
    my $group_param = $params->{'group'}{'value'};

    opendir(DIR, "./hosts.d") or die "Error opening the directory";
    my @files = grep(/\.xml$/,readdir(DIR));
    closedir(DIR);

    # Iterating over each file in hosts.d folder
    foreach my $file (@files) {

        my $config = GRNOC::Config->new(config_file => "./hosts.d/$file", force_array => 0, debug => 0);
        my $hosts= $config->get("/config/host");
        warn "$file";
        # Array and hash handling of perl object
        if (ref($hosts) eq 'HASH') {

            foreach my $key (keys $hosts->{'group'}) {
                if ($key eq 'name') {
                    if ($group_param eq $hosts->{'group'}{$key}) {
                        if (!exists($host_hash{$hosts->{'ip'}})) {
                            $host_hash{$hosts->{'ip'}} = 1;
                        }
                    }
                } else {
                    if ($group_param eq $key) {
                        if (!exists($host_hash{$hosts->{'ip'}})) {
                            $host_hash{$hosts->{'ip'}} = 1;
                        }
                    }
                }
            } 

        } else  {
            foreach my $host (@$hosts){
                foreach my $group (@$host{'group'}) {
                    foreach my $key (keys $group) {
                        if ($key eq 'id') {
                            if ($group->{$key} eq $group_param) {
                                if (!exists($host_hash{$host->{'ip'}})) {
                                    $host_hash{$host->{'ip'}} = 1;
                                }     
                            }
                        } else {
                            if ($key eq $group_param){
                                if (!exists($host_hash{$host->{'ip'}})) {
                                    $host_hash{$host->{'ip'}} = 1;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    $results{'hosts'} = [keys %host_hash];
    return \%results;
}


#------ wrap callback in service method object
my $get_hosts = GRNOC::WebService::Method->new(

    name => "get_hosts",
    description => "descr",
    callback => \&get_hosts
);

# #------ define the parameters we will allow into this callback
$get_hosts->add_input_parameter (
    name => 'group',
    pattern => '^(.*)$',
    description => "URL Parameters"
);




#------ get_groups Callback
sub get_groups{
    my %results;
    my %host_hash;
    my %group_hash;
    my %results;
    my @group_array = ();
    $method_obj = shift;
    $params = shift;
    my $group_param = $params->{'host'}{'value'};
    # my $file = "hosts.xml";
    # my $config = GRNOC::Config->new(config_file => $file, force_array => 0, debug => 0);

    # my $hosts = $config->get("/config/host");
    # foreach my $host (@$hosts){
    #     if ($host->{'ip'} eq $host_param) {
    #         foreach my $group (@$host{'group'}) {
    #             foreach my $key (keys $group) {
    #                 if ($key eq 'id') {
    #                     push @group_array, $group->{$key};
    #                 } else {
    #                     push @group_array, $key;
    #                 }
    #             }
    #         }
    #     }
    # }


    opendir(DIR, "./hosts.d") or die "Error opening the directory";
    my @files = grep(/\.xml$/,readdir(DIR));
    closedir(DIR);

    # Iterating over each file in hosts.d folder
    foreach my $file (@files) {

        my $config = GRNOC::Config->new(config_file => "./hosts.d/$file", force_array => 0, debug => 0);
        my $hosts= $config->get("/config/host");
        # Array and hash handling of perl object
        if (ref($hosts) eq 'HASH') {
            # warn Dumper($hosts);
            if ($hosts->{'ip'} eq $group_param){
                foreach my $key (keys $hosts->{'group'}) {
                    if ($key eq 'name') {
                        if (!exists($group_hash{$hosts->{'group'}{$key}})) {
                            $group_hash{$hosts->{'group'}{$key}} = 1;
                        }
                    } else {
                        if (!exists($group_hash{$key})) {
                            $group_hash{$key} = 1;
                        }
                    }
                } 
            }
        } else  {
            foreach my $host (@$hosts){
        
                
                warn Dumper($host); 
                if ($host->{'ip'} eq $group_param){
                    foreach my $group (@$host{'group'}) {
                        foreach my $key (keys $group) {
                            
                            if ($key eq 'id') {
                                if (!exists($group_hash{$group->{$key}})) {
                                    $group_hash{$group->{$key}} = 1;
                                }
                                # if ($group->{$key} eq $group_param) {
                                #     if (!exists($host_hash{$host->{'ip'}})) {
                                #         $host_hash{$host->{'ip'}} = 1;
                                #     }     
                                # }
                            } else {
                                if (!exists($group_hash{$key})) {
                                    $group_hash{$key} = 1;
                                }
                                # if ($key eq $group_param){
                                #     if (!exists($host_hash{$host->{'ip'}})) {
                                #         $host_hash{$host->{'ip'}} = 1;
                                #     }
                                # }
                            }
                        }
                    }
                } 
            }
        }
    }
    $results{'groups'} = [keys %group_hash];

    return \%results;
}
#------ get_groups wrap callback in service method object
my $get_groups = GRNOC::WebService::Method->new(

    name => "get_groups",
    description => "descr",
    callback => \&get_groups
);

#------ get_groups define the parameters we will allow into this callback
$get_groups->add_input_parameter (
    name => 'host',
    pattern => '^(.*)$',
    description => "URL Parameters"
);



#------ get_oids Callback
sub get_oids{
    my %results;
    my @oid_array = ();
    $method_obj = shift;
    $params = shift;
    my $group_param = $params->{'group'}{'value'};
    my $file = "config.xml";
    my $config = GRNOC::Config->new(config_file => $file, force_array => 0, debug => 0);
    my $groups = $config->get("/config/group");
    foreach my $group (@$groups){
        if ($group->{'name'} eq $group_param) {
            foreach my $mib (@$group{'mib'}) {
                foreach my $oid (@$mib) {

                    push @oid_array, $oid->{'oid'};
                }
            }
        }
    }

    $results{'oids'} = [@oid_array];

    return \%results;
}
#------ get_oids wrap callback in service method object
my $get_oids = GRNOC::WebService::Method->new(

    name => "get_oids",
    description => "descr",
    callback => \&get_oids
);

#------ get_oids define the parameters we will allow into this callback
$get_oids->add_input_parameter (
    name => 'group',
    pattern => '^(.*)$',
    description => "URL Parameters"
);


#------ create dispatcher
my $svc = GRNOC::WebService::Dispatcher->new();

#------ bind our method
my $res = $svc->register_method($get_hosts);
my $res1 = $svc->register_method($get_groups);
my $res2 = $svc->register_method($get_initial_data);
my $res3 = $svc->register_method($get_oids);

#------ go to town
my $res4 = $svc->handle_request();


