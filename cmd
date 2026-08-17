# jk.ps1
# Kafka broker 16 Jolokia watcher
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

    return $Results |
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

        $line = @(
            "T=$(Get-Date -Format 'HH:mm:ss')"
            "State=$state"
            "URP=$urp"
            "MinISR=$minIsr"
            "OffRep=$offlineReplicas"
            "Part=$partitions"
            "Lead=$leaders"
            "ISRshrinkDelta=$isrShrinkDelta"
            "ZKdiscDelta=$zkDisconnectDelta"
            "ZKexpDelta=$zkExpireDelta"
            "NetIdlePct=$netIdlePct"
            "ReqQ=$requestQueue"
            "HeapPct=$heapPct"
            "CpuPct=$cpuPct"
            "GCYoungDeltaMs=$gcYoungDelta"
            "GCOldDeltaMs=$gcOldDelta"
            "FollowerFetchDelta=$fetchDelta"
            "FetchQueueMaxMs=$fetchQueueMax"
            "FetchLocalMaxMs=$fetchLocalMax"
            "FlushMaxMs=$flushMax"
            "OfflineDirs=$offlineDirs"
            "BytesInDelta=$bytesInDelta"
            "BytesOutDelta=$bytesOutDelta"
        ) -join ' | '

        $bad = (
            $state -ne 3 -or
            $urp -gt 0 -or
            $minIsr -gt 0 -or
            $offlineReplicas -gt 0 -or
            $offlineDirs -gt 0 -or
            $isrShrinkDelta -gt 0 -or
            $zkDisconnectDelta -gt 0 -or
            $zkExpireDelta -gt 0
        )

        if ($bad) {
            Write-Host $line -ForegroundColor Red
        }
        else {
            Write-Host $line -ForegroundColor Green
        }

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
