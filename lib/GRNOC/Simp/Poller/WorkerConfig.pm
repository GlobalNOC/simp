package GRNOC::Simp::Poller::WorkerConfig;

use GRNOC::Config;

sub build_config {
    my $conf_file = shift;
    my $hosts_dir = shift;


    my %groups;
    # A list of hosts: { ip, snmp_version, community, node_name, {host_variable} }
    my @hosts;
    # For each host group, a list of the hosts belonging to the group
    my %host_groups;


    my $config = GRNOC::Config->new(
        config_file => $self,
        force_array => 1
    );

    foreach my $group (@{$config->get('/config/group')}) {
        my %grp;

        next if !$group->{'active'};

        foreach my $i ('name', 'interval', 'snmp_timeout', 'max_reps', 'retention') {
            $grp{$i} = $group->{$i};
        }

        my @workers;
        my $num_workers = 0 + $group->{'workers'};
        for (my $i = 0; $i < $num_workers; $i++) {
            push @workers, "$grp{'name'}$i";
        }
        $grp{'workers'} = \@workers;

        my $mib = $group->{'mib'};
        $mib = [] if !defined($mib);
        my @mib_mapped;
        @mib_mapped = map { $_->{'oid'} } @$mib;
        $grp{'mib'} = \@mib_mapped;

        $groups{$grp{'name'}} = \%grp;
    }



    opendir my $dir, $hosts_dir;
    my @hosts_files = readdir $dir;
    closedir $dir;

    foreach my $host_file (@host_files) {

        next if $host_file !~ /\.xml$/; # so we don't ingest editor tempfiles, etc.

        my $conf = GRNOC::Config->new(
            config_file => "$hosts_dir/$host_file",
            force_array => 1
        );

        foreach my $raw (@{$conf->get('/config/host')}) {
            my %host;
            for my $i ('node_name', 'ip', 'snmp_version', 'community', 'auth_key') {
                $host{$i} = $raw->{$i};
            }

            if (defined($raw->{'host_variable'})) {
                foreach my $var (keys @{$raw->{'host_variable'}}) {
                    $host{'host_variable'}{$var} = $raw->{'host_variable'}{'value'};
                }
            }

            $host{'groups'} = [];

            if (defined($raw->{'group'})) {
                foreach my $grp (keys @{$raw->{'group'}}) {
                    next if !defined($groups{$grp});
                    my $context_ids = $raw->{'group'}{$grp}{'context_id'};
                    $context_ids = [] if !defined($context_ids);
                    push @{$host_groups{$grp}}, [\%host, $context_ids] if defined($groups{$grp});
                    push @{$host{'groups'}}, $grp;
                }
            }

            push @hosts, \%host;
        }
    }

    return {
        groups      => \%groups,
        hosts       => \@hosts,
        host_groups => \%host_groups,
    };
}

1;
