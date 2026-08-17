# jk.ps1
# Broker 16 Jolokia watcher - vertical output
# Stop with Ctrl+C

$url = 'http://cto-eep-obs-prod-uk-azb4001-broker6.uk.hsbc:7777/jolokia/'
$intervalSeconds = 10

$body = @'
[
{"type":"read","mbean":"kafka.server:name=BrokerState,type=KafkaServer","attribute":"Value"},
{"type":"read","mbean":"java.lang:type=Memory","attribute":"HeapMemoryUsage"},
{"type":"read","mbean":"java.lang:type=OperatingSystem","attribute":["ProcessCpuLoad","SystemCpuLoad"]},
{"type":"read","mbean":"java.lang:name=G1 Young Generation,type=GarbageCollector","attribute":["CollectionCount","CollectionTime"]},
{"type":"read","mbean":"java.lang:name=G1 Old Generation,type=GarbageCollector","attribute":["CollectionCount","CollectionTime"]},
{"type":"read","mbean":"kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer","attribute":"Value"},
{"type":"read","mbean":"kafka.network:name=RequestQueueSize,type=RequestChannel","attribute":"Value"},
{"type":"read","mbean":"kafka.network:name=TotalTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max"]},
{"type":"read","mbean":"kafka.network:name=RequestQueueTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max"]},
{"type":"read","mbean":"kafka.network:name=LocalTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max"]},
{"type":"read","mbean":"kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=UnderMinIsrPartitionCount,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=OfflineReplicaCount,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=PartitionCount,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=LeaderCount,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=IsrShrinksPerSec,type=ReplicaManager","attribute":"Count"},
{"type":"read","mbean":"kafka.log:name=OfflineLogDirectoryCount,type=LogManager","attribute":"Value"},
{"type":"read","mbean":"kafka.log:name=LogFlushRateAndTimeMs,type=LogFlushStats","attribute":["Count","Mean","Max"]},
{"type":"read","mbean":"kafka.server:name=ZooKeeperDisconnectsPerSec,type=SessionExpireListener","attribute":"Count"},
{"type":"read","mbean":"kafka.server:name=ZooKeeperExpiresPerSec,type=SessionExpireListener","attribute":"Count"},
{"type":"read","mbean":"kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics","attribute":"Count"},
{"type":"read","mbean":"kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics","attribute":"Count"}
]
'@

function Get-Entry {
    param([array]$Results, [string]$MBean)

    $Results |
        Where-Object { $_.request.mbean -eq $MBean -and $_.status -eq 200 } |
        Select-Object -First 1
}

function Get-Scalar {
    param([array]$Results, [string]$MBean)

    $item = Get-Entry $Results $MBean

    if ($null -eq $item) {
        return $null
    }

    if ($null -ne $item.value.Value) {
        return $item.value.Value
    }

    return $item.value
}

function Get-Object {
    param([array]$Results, [string]$MBean)

    $item = Get-Entry $Results $MBean

    if ($null -eq $item) {
        return $null
    }

    return $item.value
}

function Get-Delta {
    param($Current, $Previous)

    if ($null -eq $Current -or $null -eq $Previous) {
        return 'N/A'
    }

    return [math]::Round(([double]$Current - [double]$Previous), 1)
}

function Print-Metric {
    param(
        [string]$Name,
        $Value,
        [bool]$Bad = $false
    )

    $text = if ($null -eq $Value) { 'N/A' } else { $Value }

    if ($Bad) {
        Write-Host ("{0,-28}: {1}" -f $Name, $text) -ForegroundColor Red
    }
    else {
        Write-Host ("{0,-28}: {1}" -f $Name, $text)
    }
}

$previous = @{}

while ($true) {
    try {
        $r = Invoke-RestMethod `
            -Uri $url `
            -Method POST `
            -ContentType 'application/json' `
            -Body $body `
            -TimeoutSec 15 `
            -ErrorAction Stop

        $memory = Get-Object $r 'java.lang:type=Memory'
        $heap = if ($memory) { $memory.HeapMemoryUsage } else { $null }

        $os = Get-Object $r 'java.lang:type=OperatingSystem'
        $youngGc = Get-Object $r 'java.lang:name=G1 Young Generation,type=GarbageCollector'
        $oldGc = Get-Object $r 'java.lang:name=G1 Old Generation,type=GarbageCollector'

        $fetchTotal = Get-Object $r 'kafka.network:name=TotalTimeMs,request=FetchFollower,type=RequestMetrics'
        $fetchQueue = Get-Object $r 'kafka.network:name=RequestQueueTimeMs,request=FetchFollower,type=RequestMetrics'
        $fetchLocal = Get-Object $r 'kafka.network:name=LocalTimeMs,request=FetchFollower,type=RequestMetrics'
        $flush = Get-Object $r 'kafka.log:name=LogFlushRateAndTimeMs,type=LogFlushStats'

        $state = Get-Scalar $r 'kafka.server:name=BrokerState,type=KafkaServer'
        $networkIdle = Get-Scalar $r 'kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer'
        $requestQueue = Get-Scalar $r 'kafka.network:name=RequestQueueSize,type=RequestChannel'

        $urp = Get-Scalar $r 'kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager'
        $minIsr = Get-Scalar $r 'kafka.server:name=UnderMinIsrPartitionCount,type=ReplicaManager'
        $offlineReplicas = Get-Scalar $r 'kafka.server:name=OfflineReplicaCount,type=ReplicaManager'
        $partitions = Get-Scalar $r 'kafka.server:name=PartitionCount,type=ReplicaManager'
        $leaders = Get-Scalar $r 'kafka.server:name=LeaderCount,type=ReplicaManager'
        $isrShrinks = Get-Scalar $r 'kafka.server:name=IsrShrinksPerSec,type=ReplicaManager'

        $offlineDirs = Get-Scalar $r 'kafka.log:name=OfflineLogDirectoryCount,type=LogManager'
        $zkDisconnects = Get-Scalar $r 'kafka.server:name=ZooKeeperDisconnectsPerSec,type=SessionExpireListener'
        $zkExpires = Get-Scalar $r 'kafka.server:name=ZooKeeperExpiresPerSec,type=SessionExpireListener'
        $bytesIn = Get-Scalar $r 'kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics'
        $bytesOut = Get-Scalar $r 'kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics'

        $heapPct = if ($heap -and $heap.max -gt 0) {
            [math]::Round((100 * $heap.used / $heap.max), 1)
        }
        else {
            'N/A'
        }

        $heapUsedGiB = if ($heap) {
            [math]::Round(($heap.used / 1GB), 2)
        }
        else {
            'N/A'
        }

        $heapMaxGiB = if ($heap) {
            [math]::Round(($heap.max / 1GB), 2)
        }
        else {
            'N/A'
        }

        $cpuPct = if ($os -and $null -ne $os.ProcessCpuLoad) {
            [math]::Round((100 * $os.ProcessCpuLoad), 1)
        }
        else {
            'N/A'
        }

        $netIdlePct = if ($null -ne $networkIdle) {
            [math]::Round((100 * $networkIdle), 1)
        }
        else {
            'N/A'
        }

        $gcYoungDelta = Get-Delta $youngGc.CollectionTime $previous.YoungGcTime
        $gcOldDelta = Get-Delta $oldGc.CollectionTime $previous.OldGcTime
        $fetchDelta = Get-Delta $fetchTotal.Count $previous.FetchCount
        $isrShrinkDelta = Get-Delta $isrShrinks $previous.IsrShrinks
        $zkDisconnectDelta = Get-Delta $zkDisconnects $previous.ZkDisconnects
        $zkExpireDelta = Get-Delta $zkExpires $previous.ZkExpires
        $bytesInDelta = Get-Delta $bytesIn $previous.BytesIn
        $bytesOutDelta = Get-Delta $bytesOut $previous.BytesOut

        $fetchQueueMax = if ($fetchQueue) { $fetchQueue.Max } else { 'N/A' }
        $fetchLocalMax = if ($fetchLocal) { $fetchLocal.Max } else { 'N/A' }
        $flushMax = if ($flush) { $flush.Max } else { 'N/A' }

        Clear-Host
        Write-Host "Kafka broker 16 / cto-cfk-sydc-kafka-6" -ForegroundColor Cyan
        Write-Host "Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') | refresh: $intervalSeconds sec | Ctrl+C to stop"
        Write-Host ('=' * 55)

        Write-Host 'Broker / controller metadata' -ForegroundColor Yellow
        Print-Metric 'Broker state' $state ($state -ne 3)
        Print-Metric 'Local partition count' $partitions
        Print-Metric 'Local leader count' $leaders
        Print-Metric 'Under replicated partitions' $urp ($urp -gt 0)
        Print-Metric 'Under min ISR partitions' $minIsr ($minIsr -gt 0)
        Print-Metric 'Offline replicas' $offlineReplicas ($offlineReplicas -gt 0)
        Print-Metric 'ISR shrinks, last 10 sec' $isrShrinkDelta ($isrShrinkDelta -gt 0)

        Write-Host ''
        Write-Host 'ZooKeeper stability' -ForegroundColor Yellow
        Print-Metric 'ZK disconnects, total' $zkDisconnects
        Print-Metric 'ZK disconnects, last 10 sec' $zkDisconnectDelta ($zkDisconnectDelta -gt 0)
        Print-Metric 'ZK expiries, total' $zkExpires
        Print-Metric 'ZK expiries, last 10 sec' $zkExpireDelta ($zkExpireDelta -gt 0)

        Write-Host ''
        Write-Host 'JVM / broker processing' -ForegroundColor Yellow
        Print-Metric 'Heap used / max GiB' "$heapUsedGiB / $heapMaxGiB"
        Print-Metric 'Heap used percent' "$heapPct%"
        Print-Metric 'Process CPU percent' "$cpuPct%"
        Print-Metric 'G1 young GC ms, last 10 sec' $gcYoungDelta
        Print-Metric 'G1 old GC ms, last 10 sec' $gcOldDelta
        Print-Metric 'Network processor idle percent' "$netIdlePct%" ($networkIdle -ne $null -and $netIdlePct -lt 5)
        Print-Metric 'Request queue size' $requestQueue

        Write-Host ''
        Write-Host 'Follower fetch / storage indicators' -ForegroundColor Yellow
        Print-Metric 'Follower fetches, last 10 sec' $fetchDelta
        Print-Metric 'Follower fetch queue max ms' $fetchQueueMax
        Print-Metric 'Follower fetch local max ms' $fetchLocalMax
        Print-Metric 'Log flush max ms' $flushMax
        Print-Metric 'Offline log directories' $offlineDirs ($offlineDirs -gt 0)
        Print-Metric 'Bytes in, last 10 sec' $bytesInDelta
        Print-Metric 'Bytes out, last 10 sec' $bytesOutDelta

        $previous = @{
            YoungGcTime = if ($youngGc) { $youngGc.CollectionTime } else { $null }
            OldGcTime = if ($oldGc) { $oldGc.CollectionTime } else { $null }
            FetchCount = if ($fetchTotal) { $fetchTotal.Count } else { $null }
            IsrShrinks = $isrShrinks
            ZkDisconnects = $zkDisconnects
            ZkExpires = $zkExpires
            BytesIn = $bytesIn
            BytesOut = $bytesOut
        }
    }
    catch {
        Write-Host "$(Get-Date -Format 'HH:mm:ss') JOLOKIA FAILED: $($_.Exception.Message)" -ForegroundColor Red
    }

    Start-Sleep -Seconds $intervalSeconds
}
