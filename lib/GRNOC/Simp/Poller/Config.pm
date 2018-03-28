package GRNOC::Simp::Poller::Config;

use GRNOC::Config;

sub build_config {
    my %args = @_;

    my $conf_file = $args{'config_file'};
    my $hosts_dir = $args{'hosts_dir'};


    # Config that isn't specific to a group or a host: { redis_host, redis_port, pid_file }
    my %globals;

    # The set of active groups: name -> { name, interval, snmp_timeout, max_reps, retention, [workers], [mib] }
    my %groups;

    # A list of hosts, each: { ip, snmp_version, community, username, node_name, {groups}, {host_variable} }
    #
    # groups is a hash of group names to an array of per-group context IDs; an empty array means the host
    #   belongs to the group, but doesn't use context IDs
    # host_variable is a hash of host variable name-value pairs
    #
    # will *not* contain keys "poll_id", "pending_replies", or "poll_status"
    my @hosts;

    # For each host group, a list of the hosts belonging to the group
    my %host_groups;


    my $config = GRNOC::Config->new(
        config_file => $conf_file,
        force_array => 1
    );

    $globals{'redis_host'} = $config->get('/config/redis/@host')->[0];
    $globals{'redis_port'} = $config->get('/config/redis/@port')->[0];

    my $pid_file = $config->get('/config/@pid-file')->[0];
    $pid_file = '/var/run/simp_poller.pid' if !defined($pid_file);
    $globals{'pid_file'} = $pid_file;

    foreach my $group (@{$config->get('/config/group')}) {
        my %grp;

        next if !$group->{'active'};

        foreach my $i ('name', 'interval', 'snmp_timeout', 'max_reps', 'retention') {
            $grp{$i} = $group->{$i};
        }

        my @workers;
        my $num_workers = int(0 + $group->{'workers'});
        $num_workers = 1 if $num_workers < 1;
        for (my $i = 0; $i < $num_workers; $i++) {
            push @workers, "$grp{'name'},$i";
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
    my @host_files = readdir $dir;
    closedir $dir;

    foreach my $host_file (@host_files) {

        next if $host_file !~ /\.xml$/; # so we don't ingest editor tempfiles, etc.

        my $conf = GRNOC::Config->new(
            config_file => "$hosts_dir/$host_file",
            force_array => 1
        );

        foreach my $raw (@{$conf->get('/config/host') || []}) {
            my %host;
            for my $i ('node_name', 'ip', 'snmp_version', 'community', 'username') {
                $host{$i} = $raw->{$i};
            }

            $host{'host_variable'} = {};

            foreach my $var (keys %{$raw->{'host_variable'} || {}}) {
                $host{'host_variable'}{$var} = $raw->{'host_variable'}{'value'};
            }

            $host{'groups'} = {};

            foreach my $grp (keys %{$raw->{'group'} || {}}) {
                next if !defined($groups{$grp});
                my $context_ids = $raw->{'group'}{$grp}{'context_id'};
                $context_ids = [] if !defined($context_ids);
                $host{'groups'}{$grp} = $context_ids;
                push @{$host_groups{$grp}}, \%host if defined($groups{$grp});
            }

            push @hosts, \%host;
        }
    }

    return {
        global      => \%globals,
        groups      => \%groups,
        hosts       => \@hosts,
        host_groups => \%host_groups,
    };
}

1;
