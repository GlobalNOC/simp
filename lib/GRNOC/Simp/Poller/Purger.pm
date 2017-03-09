package GRNOC::Simp::Poller::Purger;

use strict;
use Try::Tiny;
use Data::Dumper;
use Moo;
use AnyEvent;
use AnyEvent::SNMP;
use Net::SNMP;
use Redis;


### required attributes ###

has config      => ( is => 'ro',
		     required => 1 );

has logger => ( is => 'rwp',
                required => 1 );

has purge_interval => ( is => 'ro',
			required => 1 );


### internal attributes ###

has is_running => ( is => 'rwp',
                    default => 0 );

has need_restart => (is => 'rwp',
		     default => 0 );

has redis => ( is => 'rwp' );

has worker_name => (is => 'ro',
		    default => "purger");

### public methods ###

sub start {
    
    my ( $self ) = @_;
    
    my $logger = GRNOC::Log->get_logger($self->worker_name);
    $self->_set_logger($logger);
    
    $self->logger->debug( "Purger Starting." );
    
    # flag that we're running
    $self->_set_is_running( 1 );
    
    # change our process name
    $0 = "simp(purger)";

    # setup signal handlers
    $SIG{'TERM'} = sub {
	
        $self->logger->info($self->worker_name. " Received SIG TERM." );
        $self->stop();
    };
    
    $SIG{'HUP'} = sub {
	
        $self->logger->info($self->worker_name. " Received SIG HUP." );
    };

    # connect to redis
    my $redis_host = $self->config->get( '/config/redis/@host' )->[0];
    my $redis_port = $self->config->get( '/config/redis/@port' )->[0];
    
    $self->logger->debug($self->worker_name." Connecting to Redis $redis_host:$redis_port." );
    
    my $redis;

    try {
	#--- try to connect twice per second for 30 seconds, 60 attempts every 500ms.
	$redis = Redis->new(
	    server    => "$redis_host:$redis_port",
	    reconnect => 60,
	    every     => 500,
	    read_timeout => .5,
	    write_timeout => 3,
	    );
    }
    catch {
        $self->logger->error($self->worker_name." Error connecting to Redis: $_" );
    };
    
    $self->_set_redis( $redis );
    
    $self->_set_need_restart(0);
    
    $self->logger->debug( $self->worker_name . ' Starting Purger loop.' );
    

    $self->{'purge_timer'} = AnyEvent->timer( after => 1, 
					      interval => $self->purge_interval,
					      cb => sub {
						  $self->_purge_data();
					      });
    

    #let the magic happen
    AnyEvent->condvar->recv;

}

sub _purge_data{
  my $self    = shift;

  my $redis   = $self->redis;
  my $id      = $self->worker_name;  

  $self->logger->info("$id Starting Purge of stale data");
  my $removed=0;
  my @to_be_removed;

  #get all the keys
  try{
      $redis->select(1);
      my @keys = $redis->keys( '*' );
      
      foreach my $key (@keys){
	  while( $redis->llen($key) > 15){
	      my $ts = $redis->rpop($key);
	      push(@to_be_removed, $key . ",$ts");
	  }      
      }
      
      $redis->select(0);
  
      my @possible_oids = $redis->keys('*');
      
      if(scalar(@to_be_removed) >= 1){
	  foreach my $key (@possible_oids){
	      my $res = $redis->hdel($key, @to_be_removed);
	      $removed += $res;
	  }
      }
      $self->logger->info("Total Purged Entries: " . $removed . "\n");

  }catch{

      $self->logger->error("Error attempting to purge entries: " . $_);

  }
}

1;
