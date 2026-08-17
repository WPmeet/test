
kafka.server:type=ReplicaManager,name=LeaderCount
kafka.server:type=ReplicaManager,name=PartitionCount
kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions
kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount
kafka.server:type=ReplicaManager,name=ReassigningPartitions
kafka.controller:type=KafkaController,name=ActiveControllerCount
kafka.controller:type=KafkaController,name=OfflinePartitionsCount
kafka.controller:type=KafkaController,name=PreferredReplicaImbalanceCount
JOLOKIA='http://cto-eep-obs-prod-uk-azb4001-broker6.uk.hsbc:7777/jolokia'

curl -sS --connect-timeout 5 --max-time 15 \
  -H 'Content-Type: application/json' \
  -X POST "$JOLOKIA/" \
  --data '[
    {
      "type": "read",
      "mbean": "kafka.server:type=KafkaServer,name=BrokerState",
      "attribute": "Value"
    },
    {
      "type": "read",
      "mbean": "java.lang:type=Memory",
      "attribute": "HeapMemoryUsage"
    },
    {
      "type": "read",
      "mbean": "java.lang:type=OperatingSystem",
      "attribute": ["ProcessCpuLoad","SystemCpuLoad","ProcessCpuTime","AvailableProcessors"]
    },
    {
      "type": "read",
      "mbean": "java.lang:type=GarbageCollector,name=G1 Young Generation",
      "attribute": ["CollectionCount","CollectionTime"]
    },
    {
      "type": "read",
      "mbean": "java.lang:type=GarbageCollector,name=G1 Old Generation",
      "attribute": ["CollectionCount","CollectionTime"]
    },
    {
      "type": "read",
      "mbean": "kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent",
      "attribute": "Value"
    },
    {
      "type": "read",
      "mbean": "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions",
      "attribute": "Value"
    },
    {
      "type": "read",
      "mbean": "kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount",
      "attribute": "Value"
    }
  ]' | jq .
