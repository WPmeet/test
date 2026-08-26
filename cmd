index=hsbc_cto_kafka_zookeeper
namespace="cto-eep-obs-prod-uk"
sourcetype="kube:container:cto-cfk-sydc-kafka"
host="YOUR_BROKER_HOST"
| eval marker_time=if(searchmatch("Started socket server acceptors and processors"), _time, null())
| eventstats min(marker_time) as first_marker_time
| where _time >= first_marker_time
| sort 0 _time

curl -k \
  -u 'YOUR_USER_ID' \
  'https://glog-htse-rest.systems.uk.hsbc:8089/services/search/jobs/export' \
  --data-urlencode 'search=search index=hsbc_cto_kafka_zookeeper namespace IN ("cto-eep-obs-prod-uk") "edo.otel.dbpostgres.metrics.raw.public-9" sourcetype="kube:container:cto-cfk-sydc-kafka"' \
  --data-urlencode 'earliest_time=2026-07-26T00:00:00' \
  --data-urlencode 'latest_time=2026-08-22T00:00:00' \
  --data-urlencode 'output_mode=raw' \
  -o kafka_logs_26Jul_22Aug.log

index=hsbc_cto_kafka_zookeeper
namespace IN ("cto-eep-obs-prod-uk")
"edo.otel.dbpostgres.metrics.raw.public-9"
sourcetype="kube:container:cto-cfk-sydc-kafka"
| sort 0 _time

curl -k \
  -u 'YOUR_USERNAME' \
  'https://glog-htse-searchsystems.uk.hsbc:8089/services/search/jobs/export' \
  --data-urlencode 'search=search index=hsbc_cto_kafka_zookeeper namespace IN ("cto-eep-obs-prod-uk") "edo.otel.dbpostgres.metrics.raw.public-9" sourcetype="kube:container:cto-cfk-sydc-kafka"' \
  --data-urlencode 'earliest_time=2026-07-26T00:00:00' \
  --data-urlencode 'latest_time=2026-08-22T00:00:00' \
  --data-urlencode 'output_mode=raw' \
  -o kafka_logs_26Jul_22Aug.log

max by (topic, partition) (
  kafka_log_log_end_offset
)
-
on (topic, partition)
group_right(instance)
kafka_log_log_end_offset
> 100

max by (topic, partition) (
  kafka_log_log_end_offset{leader="true"}
)
-
on (topic, partition)
group_right(instance)
kafka_log_log_end_offset{leader="false"}
> 100


PID=$(pgrep -f 'kafka.Kafka')
ls -l /proc/$PID/fd 2>/dev/null | grep "$(pwd)" | grep '\.log'
ls -1 *.log | sort > /tmp/all.log
kafka-configs \
  --bootstrap-server '<bootstrap-host>:<port>' \
  --command-config '<client.properties>' \
  --entity-type topics \
  --entity-name '<topic-name>' \
  --describe \
  --all |
  grep -E '^(cleanup\.policy|retention\.ms|retention\.bytes|segment\.ms|segment\.bytes|file\.delete\.delay\.ms)='


ls -l /proc/1/fd 2>/dev/null | grep "$(pwd)/" | grep '\.log$' | awk -F/ '{print $NF}' | sort -u > /tmp/open.log


for f in *.log; do
  if ! ls -l /proc/1/fd 2>/dev/null | grep -Fq "/$(basename "$f")"; then
    echo "$f"
  fi
done


# jk-5-6-request-queue.ps1
# Kafka / Jolokia watcher for broker 5 and broker 6.
#
# What it does:
# - Polls both brokers every 10 seconds.
# - Appends new output blocks; it does NOT clear the terminal.
# - Tracks deltas independently for each broker.
# - Shows replication health, ZK deltas, JVM/CPU, request queue,
#   request-handler idle, fetch timings, storage/flush metrics, and traffic.
# - Attempts the standard Kafka request-handler idle MBean and renders N/A
#   if this Kafka / Confluent version does not expose it.
#
# Stop with Ctrl+C.

$ErrorActionPreference = 'Stop'

# -----------------------------
# Configuration
# -----------------------------
$intervalSeconds = 10

$brokers = @(
    @{
        Name = 'broker-5'
         Url  = 'http://cto-eep-obs-prod-uk-azb4001-broker5.uk.hsbc:7777/jolokia/'
    },
    @{
        Name = 'broker-6'
        Url  = 'http://cto-eep-obs-prod-uk-azb4001-broker6.uk.hsbc:7777/jolokia/'
    }
)

# -----------------------------
# Jolokia bulk request
# -----------------------------
# NOTE:
# RequestHandlerAvgIdlePercent is normally exposed as a Meter, so "Count"
# is used rather than "Value". If unavailable in your version, script shows N/A.
$body = @'
[
  {"type":"read","mbean":"kafka.server:name=BrokerState,type=KafkaServer","attribute":"Value"},

  {"type":"read","mbean":"java.lang:type=Memory","attribute":"HeapMemoryUsage"},
  {"type":"read","mbean":"java.lang:type=OperatingSystem","attribute":["ProcessCpuLoad","SystemCpuLoad"]},
  {"type":"read","mbean":"java.lang:name=G1 Young Generation,type=GarbageCollector","attribute":["CollectionCount","CollectionTime"]},
  {"type":"read","mbean":"java.lang:name=G1 Old Generation,type=GarbageCollector","attribute":["CollectionCount","CollectionTime"]},

  {"type":"read","mbean":"kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer","attribute":"Value"},
  {"type":"read","mbean":"kafka.network:name=RequestQueueSize,type=RequestChannel","attribute":"Value"},
  {"type":"read","mbean":"kafka.server:name=RequestHandlerAvgIdlePercent,type=KafkaRequestHandlerPool","attribute":["Count","OneMinuteRate","FiveMinuteRate","MeanRate"]},

  {"type":"read","mbean":"kafka.network:name=TotalTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max","99thPercentile"]},
  {"type":"read","mbean":"kafka.network:name=RequestQueueTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max","99thPercentile"]},
  {"type":"read","mbean":"kafka.network:name=LocalTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max","99thPercentile"]},
  {"type":"read","mbean":"kafka.network:name=ResponseQueueTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max","99thPercentile"]},
  {"type":"read","mbean":"kafka.network:name=ResponseSendTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max","99thPercentile"]},

  {"type":"read","mbean":"kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager","attribute":"Value"},
  {"type":"read","mbean":"kafka.server:name=UnderMinIsrPartitionCount,type=ReplicaManager","attribute":"Value"},
  {"type":"read","mbean":"kafka.server:name=OfflineReplicaCount,type=ReplicaManager","attribute":"Value"},
  {"type":"read","mbean":"kafka.server:name=PartitionCount,type=ReplicaManager","attribute":"Value"},
  {"type":"read","mbean":"kafka.server:name=LeaderCount,type=ReplicaManager","attribute":"Value"},
  {"type":"read","mbean":"kafka.server:name=ReassigningPartitions,type=ReplicaManager","attribute":"Value"},
  {"type":"read","mbean":"kafka.server:name=IsrShrinksPerSec,type=ReplicaManager","attribute":"Count"},
  {"type":"read","mbean":"kafka.server:name=IsrExpandsPerSec,type=ReplicaManager","attribute":"Count"},

  {"type":"read","mbean":"kafka.log:name=OfflineLogDirectoryCount,type=LogManager","attribute":"Value"},
  {"type":"read","mbean":"kafka.log:name=LogFlushRateAndTimeMs,type=LogFlushStats","attribute":["Count","Mean","Max","99thPercentile"]},

  {"type":"read","mbean":"kafka.server:name=ZooKeeperDisconnectsPerSec,type=SessionExpireListener","attribute":"Count"},
  {"type":"read","mbean":"kafka.server:name=ZooKeeperExpiresPerSec,type=SessionExpireListener","attribute":"Count"},

  {"type":"read","mbean":"kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics","attribute":"Count"},
  {"type":"read","mbean":"kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics","attribute":"Count"},
  {"type":"read","mbean":"kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics","attribute":"Count"},

  {"type":"read","mbean":"kafka.controller:name=ActiveControllerCount,type=KafkaController","attribute":"Value"}
]
'@

# -----------------------------
# Helper functions
# -----------------------------
function Get-Entry {
    param(
        [array]$Results,
        [string]$MBean
    )

    return $Results |
        Where-Object {
            $_.request.mbean -eq $MBean -and $_.status -eq 200
        } |
        Select-Object -First 1
}

function Get-Scalar {
    param(
        [array]$Results,
        [string]$MBean
    )

    $entry = Get-Entry -Results $Results -MBean $MBean

    if ($null -eq $entry) {
        return $null
    }

    if ($null -ne $entry.value.Value) {
        return $entry.value.Value
    }

    return $entry.value
}

function Get-MetricObject {
    param(
        [array]$Results,
        [string]$MBean
    )

    $entry = Get-Entry -Results $Results -MBean $MBean

    if ($null -eq $entry) {
        return $null
    }

    return $entry.value
}

function Get-Delta {
    param(
        $Current,
        $Previous
    )

    if ($null -eq $Current -or $null -eq $Previous) {
        return $null
    }

    return [double]$Current - [double]$Previous
}

function Format-Number {
    param(
        $Value,
        [int]$Decimals = 1
    )

    if ($null -eq $Value) {
        return 'N/A'
    }

    try {
        return ([math]::Round([double]$Value, $Decimals)).ToString()
    }
    catch {
        return "$Value"
    }
}

function Format-Integer {
    param($Value)

    if ($null -eq $Value) {
        return 'N/A'
    }

    try {
        return ([math]::Round([double]$Value, 0)).ToString('0')
    }
    catch {
        return "$Value"
    }
}

function Format-Bytes {
    param($Bytes)

    if ($null -eq $Bytes) {
        return 'N/A'
    }

    $value = [double]$Bytes

    if ($value -ge 1GB) {
        return ('{0:N2} GiB' -f ($value / 1GB))
    }

    if ($value -ge 1MB) {
        return ('{0:N2} MiB' -f ($value / 1MB))
    }

    if ($value -ge 1KB) {
        return ('{0:N2} KiB' -f ($value / 1KB))
    }

    return ('{0:N0} B' -f $value)
}

function Get-PropertyValue {
    param(
        $Object,
        [string]$Name
    )

    if ($null -eq $Object) {
        return $null
    }

    $property = $Object.PSObject.Properties[$Name]

    if ($null -eq $property) {
        return $null
    }

    return $property.Value
}

function Is-GreaterThan {
    param(
        $Value,
        [double]$Threshold
    )

    if ($null -eq $Value) {
        return $false
    }

    try {
        return ([double]$Value -gt $Threshold)
    }
    catch {
        return $false
    }
}

function Is-LessThan {
    param(
        $Value,
        [double]$Threshold
    )

    if ($null -eq $Value) {
        return $false
    }

    try {
        return ([double]$Value -lt $Threshold)
    }
    catch {
        return $false
    }
}

function Write-MetricLine {
    param(
        [string]$Label,
        [string]$Text,
        [bool]$Warning = $false,
        [bool]$Critical = $false
    )

    Write-Host $Label -ForegroundColor Yellow -NoNewline

    if ($Critical) {
        Write-Host $Text -ForegroundColor Red
    }
    elseif ($Warning) {
        Write-Host $Text -ForegroundColor DarkYellow
    }
    else {
        Write-Host $Text -ForegroundColor White
    }
}

# -----------------------------
# Output / state
# -----------------------------
function Show-BrokerMetrics {
    param(
        [hashtable]$Broker,
        [array]$Results,
        [hashtable]$Previous,
        [int]$IntervalSeconds
    )

    # Broker state and replication
    $brokerState = Get-Scalar $Results 'kafka.server:name=BrokerState,type=KafkaServer'
    $urp = Get-Scalar $Results 'kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager'
    $underMinIsr = Get-Scalar $Results 'kafka.server:name=UnderMinIsrPartitionCount,type=ReplicaManager'
    $offlineReplicas = Get-Scalar $Results 'kafka.server:name=OfflineReplicaCount,type=ReplicaManager'
    $partitionCount = Get-Scalar $Results 'kafka.server:name=PartitionCount,type=ReplicaManager'
    $leaderCount = Get-Scalar $Results 'kafka.server:name=LeaderCount,type=ReplicaManager'
    $reassigning = Get-Scalar $Results 'kafka.server:name=ReassigningPartitions,type=ReplicaManager'
    $activeController = Get-Scalar $Results 'kafka.controller:name=ActiveControllerCount,type=KafkaController'
    $isrShrinksCount = Get-Scalar $Results 'kafka.server:name=IsrShrinksPerSec,type=ReplicaManager'
    $isrExpandsCount = Get-Scalar $Results 'kafka.server:name=IsrExpandsPerSec,type=ReplicaManager'

    # JVM / CPU
    $memory = Get-MetricObject $Results 'java.lang:type=Memory'
    $heap = Get-PropertyValue $memory 'HeapMemoryUsage'

    $os = Get-MetricObject $Results 'java.lang:type=OperatingSystem'

    $youngGc = Get-MetricObject $Results 'java.lang:name=G1 Young Generation,type=GarbageCollector'
    $oldGc = Get-MetricObject $Results 'java.lang:name=G1 Old Generation,type=GarbageCollector'

    # Network / queue / request handler
    $networkIdle = Get-Scalar $Results 'kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer'
    $requestQueue = Get-Scalar $Results 'kafka.network:name=RequestQueueSize,type=RequestChannel'
    $requestHandler = Get-MetricObject $Results 'kafka.server:name=RequestHandlerAvgIdlePercent,type=KafkaRequestHandlerPool'

    # Fetch request metrics
    $fetchTotal = Get-MetricObject $Results 'kafka.network:name=TotalTimeMs,request=FetchFollower,type=RequestMetrics'
    $fetchRequestQueue = Get-MetricObject $Results 'kafka.network:name=RequestQueueTimeMs,request=FetchFollower,type=RequestMetrics'
    $fetchLocal = Get-MetricObject $Results 'kafka.network:name=LocalTimeMs,request=FetchFollower,type=RequestMetrics'
    $fetchResponseQueue = Get-MetricObject $Results 'kafka.network:name=ResponseQueueTimeMs,request=FetchFollower,type=RequestMetrics'
    $fetchResponseSend = Get-MetricObject $Results 'kafka.network:name=ResponseSendTimeMs,request=FetchFollower,type=RequestMetrics'

    # Storage / ZK / traffic
    $logFlush = Get-MetricObject $Results 'kafka.log:name=LogFlushRateAndTimeMs,type=LogFlushStats'
    $offlineLogDirs = Get-Scalar $Results 'kafka.log:name=OfflineLogDirectoryCount,type=LogManager'

    $zkDisconnectsCount = Get-Scalar $Results 'kafka.server:name=ZooKeeperDisconnectsPerSec,type=SessionExpireListener'
    $zkExpiresCount = Get-Scalar $Results 'kafka.server:name=ZooKeeperExpiresPerSec,type=SessionExpireListener'

    $bytesInCount = Get-Scalar $Results 'kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics'
    $bytesOutCount = Get-Scalar $Results 'kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics'
    $messagesInCount = Get-Scalar $Results 'kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics'

    # -----------------------------
    # Derive current JVM data
    # -----------------------------
    $heapUsedGiB = $null
    $heapMaxGiB = $null
    $heapPct = $null

    if ($null -ne $heap) {
        $heapUsed = Get-PropertyValue $heap 'used'
        $heapMax = Get-PropertyValue $heap 'max'

        if ($null -ne $heapUsed) {
            $heapUsedGiB = [double]$heapUsed / 1GB
        }

        if ($null -ne $heapMax -and [double]$heapMax -gt 0) {
            $heapMaxGiB = [double]$heapMax / 1GB
        }

        if ($null -ne $heapUsed -and $null -ne $heapMax -and [double]$heapMax -gt 0) {
            $heapPct = ([double]$heapUsed / [double]$heapMax) * 100
        }
    }

    $processCpuPct = $null
    $systemCpuPct = $null

    $processCpuLoad = Get-PropertyValue $os 'ProcessCpuLoad'
    $systemCpuLoad = Get-PropertyValue $os 'SystemCpuLoad'

    if ($null -ne $processCpuLoad -and [double]$processCpuLoad -ge 0) {
        $processCpuPct = [double]$processCpuLoad * 100
    }

    if ($null -ne $systemCpuLoad -and [double]$systemCpuLoad -ge 0) {
        $systemCpuPct = [double]$systemCpuLoad * 100
    }

    $networkIdlePct = $null

    if ($null -ne $networkIdle -and [double]$networkIdle -ge 0) {
        $networkIdlePct = [double]$networkIdle * 100
    }

    # Request-handler MBean varies across releases.
    # If available as an idle percentage, it could be a decimal value.
    $requestHandlerOneMinuteRate = Get-PropertyValue $requestHandler 'OneMinuteRate'
    $requestHandlerFiveMinuteRate = Get-PropertyValue $requestHandler 'FiveMinuteRate'
    $requestHandlerMeanRate = Get-PropertyValue $requestHandler 'MeanRate'
    $requestHandlerCount = Get-PropertyValue $requestHandler 'Count'

    # -----------------------------
    # Calculate independent deltas
    # -----------------------------
    $youngGcTime = Get-PropertyValue $youngGc 'CollectionTime'
    $oldGcTime = Get-PropertyValue $oldGc 'CollectionTime'
    $fetchCount = Get-PropertyValue $fetchTotal 'Count'
    $flushCount = Get-PropertyValue $logFlush 'Count'

    $youngGcDelta = Get-Delta $youngGcTime $Previous.YoungGcTime
    $oldGcDelta = Get-Delta $oldGcTime $Previous.OldGcTime
    $fetchDelta = Get-Delta $fetchCount $Previous.FetchCount
    $flushDelta = Get-Delta $flushCount $Previous.FlushCount

    $isrShrinksDelta = Get-Delta $isrShrinksCount $Previous.IsrShrinksCount
    $isrExpandsDelta = Get-Delta $isrExpandsCount $Previous.IsrExpandsCount

    $zkDisconnectsDelta = Get-Delta $zkDisconnectsCount $Previous.ZkDisconnectsCount
    $zkExpiresDelta = Get-Delta $zkExpiresCount $Previous.ZkExpiresCount

    $bytesInDelta = Get-Delta $bytesInCount $Previous.BytesInCount
    $bytesOutDelta = Get-Delta $bytesOutCount $Previous.BytesOutCount
    $messagesInDelta = Get-Delta $messagesInCount $Previous.MessagesInCount

    $requestQueueDelta = Get-Delta $requestQueue $Previous.RequestQueue

    # -----------------------------
    # Timer fields
    # -----------------------------
    $fetchQueueMax = Get-PropertyValue $fetchRequestQueue 'Max'
    $fetchLocalMax = Get-PropertyValue $fetchLocal 'Max'
    $fetchResponseQueueMax = Get-PropertyValue $fetchResponseQueue 'Max'
    $fetchResponseSendMax = Get-PropertyValue $fetchResponseSend 'Max'
    $fetchTotalMax = Get-PropertyValue $fetchTotal 'Max'

    $fetchQueueP99 = Get-PropertyValue $fetchRequestQueue '99thPercentile'
    $fetchLocalP99 = Get-PropertyValue $fetchLocal '99thPercentile'
    $fetchTotalP99 = Get-PropertyValue $fetchTotal '99thPercentile'

    $flushMean = Get-PropertyValue $logFlush 'Mean'
    $flushMax = Get-PropertyValue $logFlush 'Max'
    $flushP99 = Get-PropertyValue $logFlush '99thPercentile'

    # -----------------------------
    # Classify queue condition
    # -----------------------------
    $queueStatus = 'unknown'
    $queueMessage = 'queue metric unavailable'

    if ($null -ne $requestQueue) {
        if ([double]$requestQueue -eq 0) {
            $queueStatus = 'healthy'
            $queueMessage = 'empty'
        }
        elseif ([double]$requestQueue -lt 100) {
            $queueStatus = 'watch'
            $queueMessage = 'non-zero'
        }
        elseif ([double]$requestQueue -lt 500) {
            $queueStatus = 'warning'
            $queueMessage = 'elevated'
        }
        else {
            $queueStatus = 'critical'
            $queueMessage = 'SATURATED or fixed capacity value'
        }
    }

    # -----------------------------
    # Print block
    # -----------------------------
    Write-Host ''
    Write-Host "========== $($Broker.Name) | $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ==========" -ForegroundColor Cyan

    $brokerText =
        "state=$(Format-Integer $brokerState) | partitions=$(Format-Integer $partitionCount) | leaders=$(Format-Integer $leaderCount) | activeController=$(Format-Integer $activeController) | URP=$(Format-Integer $urp) | minISR=$(Format-Integer $underMinIsr) | offlineReplicas=$(Format-Integer $offlineReplicas) | reassigning=$(Format-Integer $reassigning) | ISRshrink(${IntervalSeconds}s)=$(Format-Integer $isrShrinksDelta) | ISRexpand(${IntervalSeconds}s)=$(Format-Integer $isrExpandsDelta)"

    $brokerCritical =
        ($brokerState -ne 3) -or
        (Is-GreaterThan $urp 0) -or
        (Is-GreaterThan $underMinIsr 0) -or
        (Is-GreaterThan $offlineReplicas 0) -or
        (Is-GreaterThan $isrShrinksDelta 0)

    Write-MetricLine -Label 'BROKER    ' -Text $brokerText -Critical $brokerCritical

    $zkText =
        "disconnects(total)=$(Format-Integer $zkDisconnectsCount) | disconnects(${IntervalSeconds}s)=$(Format-Integer $zkDisconnectsDelta) | expiries(total)=$(Format-Integer $zkExpiresCount) | expiries(${IntervalSeconds}s)=$(Format-Integer $zkExpiresDelta)"

    $zkCritical =
        (Is-GreaterThan $zkDisconnectsDelta 0) -or
        (Is-GreaterThan $zkExpiresDelta 0)

    Write-MetricLine -Label 'ZK        ' -Text $zkText -Critical $zkCritical

    $jvmText =
        "heapUsed=$(Format-Number $heapUsedGiB 2) GiB | heapMax=$(Format-Number $heapMaxGiB 2) GiB | heapUsedPct=$(Format-Number $heapPct 1)% | processCPU=$(Format-Number $processCpuPct 1)% | systemCPU=$(Format-Number $systemCpuPct 1)% | youngGC(${IntervalSeconds}s)=$(Format-Integer $youngGcDelta) ms | oldGC(${IntervalSeconds}s)=$(Format-Integer $oldGcDelta) ms"

    $jvmCritical =
        (Is-GreaterThan $heapPct 90) -or
        (Is-GreaterThan $oldGcDelta 1000)

    $jvmWarning =
        (Is-GreaterThan $heapPct 80) -or
        (Is-GreaterThan $youngGcDelta 3000)

    Write-MetricLine -Label 'JVM       ' -Text $jvmText -Warning $jvmWarning -Critical $jvmCritical

    $handlerInfo = 'N/A'

    if ($null -ne $requestHandler) {
        $handlerInfo =
            "count=$(Format-Integer $requestHandlerCount) | oneMinRate=$(Format-Number $requestHandlerOneMinuteRate 3) | fiveMinRate=$(Format-Number $requestHandlerFiveMinuteRate 3) | meanRate=$(Format-Number $requestHandlerMeanRate 3)"
    }

    $networkText =
        "networkIdle=$(Format-Number $networkIdlePct 1)% | requestQueue=$(Format-Integer $requestQueue) | queueDelta(${IntervalSeconds}s)=$(Format-Integer $requestQueueDelta) | queueStatus=$queueStatus ($queueMessage) | handlerMetric=$handlerInfo"

    $networkCritical =
        ($queueStatus -eq 'critical') -or
        (Is-LessThan $networkIdlePct 5)

    $networkWarning =
        ($queueStatus -eq 'warning') -or
        (Is-LessThan $networkIdlePct 15)

    Write-MetricLine -Label 'NETWORK   ' -Text $networkText -Warning $networkWarning -Critical $networkCritical

    $fetchText =
        "fetches(${IntervalSeconds}s)=$(Format-Integer $fetchDelta) | reqQueueP99=$(Format-Number $fetchQueueP99 1) ms | reqQueueMax=$(Format-Number $fetchQueueMax 1) ms | localP99=$(Format-Number $fetchLocalP99 1) ms | localMax=$(Format-Number $fetchLocalMax 1) ms | responseQueueMax=$(Format-Number $fetchResponseQueueMax 1) ms | responseSendMax=$(Format-Number $fetchResponseSendMax 1) ms | totalP99=$(Format-Number $fetchTotalP99 1) ms | totalMax=$(Format-Number $fetchTotalMax 1) ms"

    $fetchCritical =
        (Is-GreaterThan $fetchLocalP99 1000) -or
        (Is-GreaterThan $fetchQueueP99 1000)

    $fetchWarning =
        (Is-GreaterThan $fetchLocalP99 100) -or
        (Is-GreaterThan $fetchQueueP99 100)

    Write-MetricLine -Label 'FETCH     ' -Text $fetchText -Warning $fetchWarning -Critical $fetchCritical

    $storageText =
        "flushes(${IntervalSeconds}s)=$(Format-Integer $flushDelta) | flushMean=$(Format-Number $flushMean 1) ms | flushP99=$(Format-Number $flushP99 1) ms | flushMax=$(Format-Number $flushMax 1) ms | offlineLogDirs=$(Format-Integer $offlineLogDirs)"

    $storageCritical =
        (Is-GreaterThan $offlineLogDirs 0) -or
        (Is-GreaterThan $flushP99 1000)

    $storageWarning =
        (Is-GreaterThan $flushP99 100)

    Write-MetricLine -Label 'STORAGE   ' -Text $storageText -Warning $storageWarning -Critical $storageCritical

    $trafficText =
        "bytesIn(${IntervalSeconds}s)=$(Format-Bytes $bytesInDelta) | bytesOut(${IntervalSeconds}s)=$(Format-Bytes $bytesOutDelta) | messagesIn(${IntervalSeconds}s)=$(Format-Integer $messagesInDelta)"

    Write-MetricLine -Label 'TRAFFIC   ' -Text $trafficText

    # Return the exact counters needed for this broker's next poll.
    return @{
        YoungGcTime        = $youngGcTime
        OldGcTime          = $oldGcTime
        FetchCount         = $fetchCount
        FlushCount         = $flushCount
        IsrShrinksCount    = $isrShrinksCount
        IsrExpandsCount    = $isrExpandsCount
        ZkDisconnectsCount = $zkDisconnectsCount
        ZkExpiresCount     = $zkExpiresCount
        BytesInCount       = $bytesInCount
        BytesOutCount      = $bytesOutCount
        MessagesInCount    = $messagesInCount
        RequestQueue       = $requestQueue
    }
}

# -----------------------------
# Main monitoring loop
# -----------------------------
$previousByBroker = @{}

Write-Host ''
Write-Host 'Kafka broker 5 + 6 watcher started. New blocks append every 10 seconds. Press Ctrl+C to stop.' -ForegroundColor Cyan
Write-Host 'Queue guidance: 0=healthy; 1-99=watch; 100-499=elevated; 500=saturated or metric capacity. Do not increase queued.max.requests based on this alone.' -ForegroundColor DarkGray
Write-Host 'Timer Mean/P99/Max values are cumulative since JVM / metric startup. Values marked (10s) are per-poll deltas.' -ForegroundColor DarkGray

while ($true) {
    foreach ($broker in $brokers) {
        try {
            $result = Invoke-RestMethod `
                -Uri $broker.Url `
                -Method POST `
                -ContentType 'application/json' `
                -Body $body `
                -TimeoutSec 15 `
                -ErrorAction Stop

            $previous = $previousByBroker[$broker.Name]

            if ($null -eq $previous) {
                $previous = @{}
            }

            $previousByBroker[$broker.Name] = Show-BrokerMetrics `
                -Broker $broker `
                -Results $result `
                -Previous $previous `
                -IntervalSeconds $intervalSeconds
        }
        catch {
            Write-Host ''
            Write-Host "========== $($broker.Name) | $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ==========" -ForegroundColor Cyan
            Write-Host "JOLOKIA FAILED: $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    Start-Sleep -Seconds $intervalSeconds
}
