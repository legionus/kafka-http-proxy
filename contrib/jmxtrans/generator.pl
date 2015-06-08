#!/usr/bin/perl

use strict;
use warnings;

use Sys::Hostname;
use JSON;

my $kafka_jmx_port = 9999;

my $hostname = hostname;
my $hostname_alias = $hostname;
$hostname_alias =~ s/\./_/g;

my $graphite_servers = [
	{
		'host'   => 'localhost',
		'port'   => 2024,
		'prefix' => 'one_min',
	}
];

my $checks = {
	'servers' => [
		{
			'alias' => $hostname_alias,
			'host' =>  $hostname,
			'port' => $kafka_jmx_port,
			'queries' => [
				{
					'attr' => [
						'HeapMemoryUsage',
						'NonHeapMemoryUsage'
					],
					'obj' => 'java.lang:type=Memory',
					'resultAlias' => 'kafka.heap',
					'wildcard' => 0
				},
				{
					'attr' => [
						'CollectionCount',
						'CollectionTime'
					],
					'obj' => 'java.lang:type=GarbageCollector,name=*',
					'resultAlias' => 'kafka.gc',
					'wildcard' => 0
				},
				{
					'attr' => [
						'ThreadCount'
					],
					'obj' => 'java.lang:type=Threading',
					'resultAlias' => 'kafka.thread',
					'wildcard' => 0
				},
				{
					'attr' => [
						'Count'
					],
					'obj' => '"kafka.log":type="LogFlushStats",name="LogFlushRateAndTimeMs"',
					'resultAlias' => 'kafka.log.LogFlushStats.LogFlush',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FifteenMinuteRate'
					],
					'obj' => '"kafka.log":type="LogFlushStats",name="LogFlushRateAndTimeMs"',
					'resultAlias' => 'kafka.log.LogFlushStats.LogFlush',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FiveMinuteRate'
					],
					'obj' => '"kafka.log":type="LogFlushStats",name="LogFlushRateAndTimeMs"',
					'resultAlias' => 'kafka.log.LogFlushStats.LogFlush',
					'wildcard' => 0
				},
				{
					'attr' => [
						'MeanRate'
					],
					'obj' => '"kafka.log":type="LogFlushStats",name="LogFlushRateAndTimeMs"',
					'resultAlias' => 'kafka.log.LogFlushStats.LogFlush',
					'wildcard' => 0
				},
				{
					'attr' => [
						'OneMinuteRate'
					],
					'obj' => '"kafka.log":type="LogFlushStats",name="LogFlushRateAndTimeMs"',
					'resultAlias' => 'kafka.log.LogFlushStats.LogFlush',
					'wildcard' => 0
				},
				{
					'attr' => [
						'Count'
					],
					'obj' => '"kafka.server":type="BrokerTopicMetrics",name=*',
					'resultAlias' => 'kafka.server.BrokerTopicMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'FifteenMinuteRate'
					],
					'obj' => '"kafka.server":type="BrokerTopicMetrics",name=*',
					'resultAlias' => 'kafka.server.BrokerTopicMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'FiveMinuteRate'
					],
					'obj' => '"kafka.server":type="BrokerTopicMetrics",name=*',
					'resultAlias' => 'kafka.server.BrokerTopicMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'MeanRate'
					],
					'obj' => '"kafka.server":type="BrokerTopicMetrics",name=*',
					'resultAlias' => 'kafka.server.BrokerTopicMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'OneMinuteRate'
					],
					'obj' => '"kafka.server":type="BrokerTopicMetrics",name=*',
					'resultAlias' => 'kafka.server.BrokerTopicMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'Value'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="UnderReplicatedPartitions"',
					'resultAlias' => 'kafka.server.ReplicaManager.UnderReplicatedPartitions',
					'wildcard' => 0
				},
				{
					'attr' => [
						'Value'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="PartitionCount"',
					'resultAlias' => 'kafka.server.ReplicaManager.PartitionCount',
					'wildcard' => 0
				},
				{
					'attr' => [
						'Value'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="LeaderCount"',
					'resultAlias' => 'kafka.server.ReplicaManager.LeaderCount',
					'wildcard' => 0
				},
				{
					'attr' => [
						'Count'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="ISRShrinksPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.ISRShrinks',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FifteenMinuteRate'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="ISRShrinksPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.ISRShrinks',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FiveMinuteRate'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="ISRShrinksPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.ISRShrinks',
					'wildcard' => 0
				},
				{
					'attr' => [
						'MeanRate'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="ISRShrinksPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.ISRShrinks',
					'wildcard' => 0
				},
				{
					'attr' => [
						'OneMinuteRate'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="ISRShrinksPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.ISRShrinks',
					'wildcard' => 0
				},
				{
					'attr' => [
						'Count'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="IsrExpandsPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.IsrExpands',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FifteenMinuteRate'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="IsrExpandsPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.IsrExpands',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FiveMinuteRate'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="IsrExpandsPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.IsrExpands',
					'wildcard' => 0
				},
				{
					'attr' => [
						'MeanRate'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="IsrExpandsPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.IsrExpands',
					'wildcard' => 0
				},
				{
					'attr' => [
						'OneMinuteRate'
					],
					'obj' => '"kafka.server":type="ReplicaManager",name="IsrExpandsPerSec"',
					'resultAlias' => 'kafka.server.ReplicaManager.IsrExpands',
					'wildcard' => 0
				},
				{
					'attr' => [
						'Value'
					],
					'obj' => '"kafka.server":type="ReplicaFetcherManager",name=*',
					'resultAlias' => 'kafka.server.ReplicaFetcherManager',
					'wildcard' => 1
				},
				{
					'attr' => [
						'Value'
					],
					'obj' => '"kafka.server":type="ProducerRequestPurgatory",name=*',
					'resultAlias' => 'kafka.server.ProducerRequestPurgatory',
					'wildcard' => 1
				},
				{
					'attr' => [
						'Value'
					],
					'obj' => '"kafka.server":type="FetchRequestPurgatory",name=*',
					'resultAlias' => 'kafka.server.ProducerRequestPurgatory',
					'wildcard' => 1
				},
				{
					'attr' => [
						'Count'
					],
					'obj' => '"kafka.network":type="RequestMetrics",name="*-RequestsPerSec"',
					'resultAlias' => 'kafka.network.RequestMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'FifteenMinuteRate'
					],
					'obj' => '"kafka.network":type="RequestMetrics",name="*-RequestsPerSec"',
					'resultAlias' => 'kafka.network.RequestMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'FiveMinuteRate'
					],
					'obj' => '"kafka.network":type="RequestMetrics",name="*-RequestsPerSec"',
					'resultAlias' => 'kafka.network.RequestMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'MeanRate'
					],
					'obj' => '"kafka.network":type="RequestMetrics",name="*-RequestsPerSec"',
					'resultAlias' => 'kafka.network.RequestMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'OneMinuteRate'
					],
					'obj' => '"kafka.network":type="RequestMetrics",name="*-RequestsPerSec"',
					'resultAlias' => 'kafka.network.RequestMetrics',
					'wildcard' => 1
				},
				{
					'attr' => [
						'Value'
					],
					'obj' => '"kafka.controller":type="KafkaController",name=*',
					'resultAlias' => 'kafka.controller.KafkaController',
					'wildcard' => 1
				},
				{
					'attr' => [
						'Count'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="LeaderElectionRateAndTimeMs"',
					'resultAlias' => 'kafka.controller.ControllerStats.LeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FifteenMinuteRate'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="LeaderElectionRateAndTimeMs"',
					'resultAlias' => 'kafka.controller.ControllerStats.LeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FiveMinuteRate'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="LeaderElectionRateAndTimeMs"',
					'resultAlias' => 'kafka.controller.ControllerStats.LeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'MeanRate'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="LeaderElectionRateAndTimeMs"',
					'resultAlias' => 'kafka.controller.ControllerStats.LeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'OneMinuteRate'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="LeaderElectionRateAndTimeMs"',
					'resultAlias' => 'kafka.controller.ControllerStats.LeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'Count'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="UncleanLeaderElectionsPerSec"',
					'resultAlias' => 'kafka.controller.ControllerStats.UncleanLeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FifteenMinuteRate'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="UncleanLeaderElectionsPerSec"',
					'resultAlias' => 'kafka.controller.ControllerStats.UncleanLeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'FiveMinuteRate'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="UncleanLeaderElectionsPerSec"',
					'resultAlias' => 'kafka.controller.ControllerStats.UncleanLeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'MeanRate'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="UncleanLeaderElectionsPerSec"',
					'resultAlias' => 'kafka.controller.ControllerStats.UncleanLeaderElection',
					'wildcard' => 0
				},
				{
					'attr' => [
						'OneMinuteRate'
					],
					'obj' => '"kafka.controller":type="ControllerStats",name="UncleanLeaderElectionsPerSec"',
					'resultAlias' => 'kafka.controller.ControllerStats.UncleanLeaderElection',
					'wildcard' => 0
				}
			]
		}
	]
};

foreach my $server (@{$checks->{'servers'}}) {
	foreach my $query (@{$server->{'queries'}}) {
		$query->{'outputWriters'} = [];

		foreach my $graphite_server (@{$graphite_servers}) {
			my $elem = {
				'@class' => 'com.googlecode.jmxtrans.model.output.GraphiteWriter',
				'settings' => {
					'host' => $graphite_server->{'host'},
					'port' => $graphite_server->{'port'},
				}
			};

			$elem->{'settings'}->{'rootPrefix'} = $graphite_server->{'prefix'}
				if $graphite_server->{'prefix'};

			$elem->{'settings'}->{'typeNames'} = ['name']
				if $query->{'wildcard'};

			push(@{$query->{'outputWriters'}}, $elem);
		}
		delete $query->{'wildcard'};
	}
}

print to_json($checks, {utf8 => 1, pretty => 1});
