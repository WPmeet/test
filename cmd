# jk.ps1
# Kafka broker 16 Jolokia watcher
# Fresh bulk Jolokia request every 10 seconds.
# Ctrl+C stops it.

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
{"type":"read","mbean":"kafka.network:name=ResponseQueueTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max"]},
{"type":"read","mbean":"kafka.network:name=ResponseSendTimeMs,request=FetchFollower,type=RequestMetrics","attribute":["Count","Mean","Max"]},
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

function Get-Entry {
    param([array]$Results, [string]$MBean)

    return $Results |
        Where-Object {
            $_.request.mbean -eq $MBean -and $_.status -eq 200
        } |
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

    return [math]::Round(
        ([double]$Current - [double]$Previous),
        1
    )
}

function Get-Value {
    param($Value, [int]$Decimals = 1)

    if ($null -eq $Value) {
        return 'N/A'
    }

    if ($Value -is [string]) {
        return $Value
    }

    return [math]::Round([double]$Value, $Decimals)
}

function Show-Category {
    param(
        [string]$Category,
        [string]$Values,
        [bool]$Bad = $false
    )

    Write-Host $Category -ForegroundColor Yellow -NoNewline

    if ($Bad) {
        Write-Host $Values -ForegroundColor Red
    }
    else {
        Write-Host $Values
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

        # JVM MBeans
        $memory = Get-Object $r 'java.lang:type=Memory'
        $heap = $null

        if ($null -ne $memory) {
            $heap = $memory.HeapMemoryUsage
        }

        $os = Get-Object $r 'java.lang:type=OperatingSystem'

        $youngGc = Get-Object `
            $r `
            'java.lang:name=G1 Young Generation,type=GarbageCollector'

        $oldGc = Get-Object `
            $r `
            'java.lang:name=G1 Old Generation,type=GarbageCollector'

        # Fetch / storage MBeans
        $fetchTotal = Get-Object `
            $r `
            'kafka.network:name=TotalTimeMs,request=FetchFollower,type=RequestMetrics'

        $fetchQueue = Get-Object `
            $r `
            'kafka.network:name=RequestQueueTimeMs,request=FetchFollower,type=RequestMetrics'

        $fetchLocal = Get-Object `
            $r `
            'kafka.network:name=LocalTimeMs,request=FetchFollower,type=RequestMetrics'

        $fetchResponseQueue = Get-Object `
            $r `
            'kafka.network:name=ResponseQueueTimeMs,request=FetchFollower,type=RequestMetrics'

        $fetchResponseSend = Get-Object `
            $r `
            'kafka.network:name=ResponseSendTimeMs,request=FetchFollower,type=RequestMetrics'

        $flush = Get-Object `
            $r `
            'kafka.log:name=LogFlushRateAndTimeMs,type=LogFlushStats'

        # Broker MBeans
        $state = Get-Scalar $r 'kafka.server:name=BrokerState,type=KafkaServer'
        $networkIdle = Get-Scalar $r 'kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer'
        $requestQueue = Get-Scalar $r 'kafka.network:name=RequestQueueSize,type=RequestChannel'

        $urp = Get-Scalar $r 'kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager'
        $minIsr = Get-Scalar $r 'kafka.server:name=UnderMinIsrPartitionCount,type=ReplicaManager'
        $offlineReplicas = Get-Scalar $r 'kafka.server:name=OfflineReplicaCount,type=ReplicaManager'
        $partitions = Get-Scalar $r 'kafka.server:name=PartitionCount,type=ReplicaManager'
        $leaders = Get-Scalar $r 'kafka.server:name=LeaderCount,type=ReplicaManager'
        $reassigning = Get-Scalar $r 'kafka.server:name=ReassigningPartitions,type=ReplicaManager'
        $isrShrinks = Get-Scalar $r 'kafka.server:name=IsrShrinksPerSec,type=ReplicaManager'
        $isrExpands = Get-Scalar $r 'kafka.server:name=IsrExpandsPerSec,type=ReplicaManager'

        $offlineDirs = Get-Scalar $r 'kafka.log:name=OfflineLogDirectoryCount,type=LogManager'
        $zkDisconnects = Get-Scalar $r 'kafka.server:name=ZooKeeperDisconnectsPerSec,type=SessionExpireListener'
        $zkExpires = Get-Scalar $r 'kafka.server:name=ZooKeeperExpiresPerSec,type=SessionExpireListener'

        $bytesIn = Get-Scalar $r 'kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics'
        $bytesOut = Get-Scalar $r 'kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics'
        $messagesIn = Get-Scalar $r 'kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics'
        $activeController = Get-Scalar $r 'kafka.controller:name=ActiveControllerCount,type=KafkaController'

        # Heap: used, max, and percentage.
        $heapUsedGiB = 'N/A'
        $heapMaxGiB = 'N/A'
        $heapPct = 'N/A'

        if ($null -ne $heap) {
            $heapUsedGiB = [math]::Round(
                ([double]$heap.used / 1GB),
                2
            )

            $heapMaxGiB = [math]::Round(
                ([double]$heap.max / 1GB),
                2
            )

            if ([double]$heap.max -gt 0) {
                $heapPct = [math]::Round(
                    (([double]$heap.used / [double]$heap.max) * 100),
                    1
                )
            }
        }

        $cpuPct = 'N/A'
        $systemCpuPct = 'N/A'

        if ($null -ne $os) {
            if ($null -ne $os.ProcessCpuLoad) {
                $cpuPct = [math]::Round(
                    ([double]$os.ProcessCpuLoad * 100),
                    1
                )
            }

            if ($null -ne $os.SystemCpuLoad) {
                $systemCpuPct = [math]::Round(
                    ([double]$os.SystemCpuLoad * 100),
                    1
                )
            }
        }

        $networkIdlePct = 'N/A'

        if ($null -ne $networkIdle) {
            $networkIdlePct = [math]::Round(
                ([double]$networkIdle * 100),
                1
            )
        }

        # Ten-second deltas
        $youngGcDelta = Get-Delta `
            $(if ($youngGc) { $youngGc.CollectionTime } else { $null }) `
            $previous.YoungGcTime

        $oldGcDelta = Get-Delta `
            $(if ($oldGc) { $oldGc.CollectionTime } else { $null }) `
            $previous.OldGcTime

        $fetchCountDelta = Get-Delta `
            $(if ($fetchTotal) { $fetchTotal.Count } else { $null }) `
            $previous.FetchCount

        $isrShrinkDelta = Get-Delta $isrShrinks $previous.IsrShrinks
        $isrExpandDelta = Get-Delta $isrExpands $previous.IsrExpands
        $zkDisconnectDelta = Get-Delta $zkDisconnects $previous.ZkDisconnects
        $zkExpireDelta = Get-Delta $zkExpires $previous.ZkExpires
        $bytesInDelta = Get-Delta $bytesIn $previous.BytesIn
        $bytesOutDelta = Get-Delta $bytesOut $previous.BytesOut
        $messagesInDelta = Get-Delta $messagesIn $previous.MessagesIn

        $flushCountDelta = Get-Delta `
            $(if ($flush) { $flush.Count } else { $null }) `
            $previous.FlushCount

        # Timer values: max/mean/p99 are cumulative/high-water metrics.
        $fetchQueueMax = if ($fetchQueue) { Get-Value $fetchQueue.Max } else { 'N/A' }
        $fetchLocalMax = if ($fetchLocal) { Get-Value $fetchLocal.Max } else { 'N/A' }
        $fetchResponseQueueMax = if ($fetchResponseQueue) { Get-Value $fetchResponseQueue.Max } else { 'N/A' }
        $fetchResponseSendMax = if ($fetchResponseSend) { Get-Value $fetchResponseSend.Max } else { 'N/A' }
        $fetchTotalMax = if ($fetchTotal) { Get-Value $fetchTotal.Max } else { 'N/A' }

        $flushMean = if ($flush) { Get-Value $flush.Mean } else { 'N/A' }
        $flushMax = if ($flush) { Get-Value $flush.Max } else { 'N/A' }
        $flushP99 = if ($flush) { Get-Value $flush.'99thPercentile' } else { 'N/A' }

        Clear-Host

        Write-Host 'Kafka broker 16 / cto-cfk-sydc-kafka-6' -ForegroundColor Cyan
        Write-Host "Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') | fresh poll every 10 sec | Ctrl+C to stop"
        Write-Host ('=' * 170)

        $brokerLine =
            "state=$state | partitions=$partitions | leaders=$leaders | activeController=$activeController | URP=$urp | minISR=$minIsr | offlineReplicas=$offlineReplicas | reassigning=$reassigning | ISRshrink(10s)=$isrShrinkDelta | ISRexpand(10s)=$isrExpandDelta"

        $brokerBad =
            ($state -ne 3) -or
            ($urp -gt 0) -or
            ($minIsr -gt 0) -or
            ($offlineReplicas -gt 0) -or
            ($isrShrinkDelta -gt 0)

        Show-Category 'BROKER    ' $brokerLine $brokerBad

        $zkLine =
            "disconnects(total)=$zkDisconnects | disconnects(10s)=$zkDisconnectDelta | expiries(total)=$zkExpires | expiries(10s)=$zkExpireDelta"

        $zkBad =
            ($zkDisconnectDelta -gt 0) -or
            ($zkExpireDelta -gt 0)

        Show-Category 'ZK        ' $zkLine $zkBad

        $jvmLine =
            "heapUsed=$heapUsedGiB GiB | heapMax=$heapMaxGiB GiB | heapUsedPct=$heapPct% | processCPU=$cpuPct% | systemCPU=$systemCpuPct% | youngGC(10s)=$youngGcDelta ms | oldGC(10s)=$oldGcDelta ms"

        $jvmBad =
            ($oldGcDelta -ne 'N/A' -and [double]$oldGcDelta -gt 1000)

        Show-Category 'JVM       ' $jvmLine $jvmBad

        $networkLine =
            "networkIdle=$networkIdlePct% | requestQueue=$requestQueue | bytesIn(10s)=$bytesInDelta | bytesOut(10s)=$bytesOutDelta | messagesIn(10s)=$messagesInDelta"

        $networkBad =
            ($networkIdlePct -ne 'N/A' -and [double]$networkIdlePct -lt 5)

        Show-Category 'NETWORK   ' $networkLine $networkBad

        $fetchLine =
            "fetches(10s)=$fetchCountDelta | queueMax=$fetchQueueMax ms | localMax=$fetchLocalMax ms | responseQueueMax=$fetchResponseQueueMax ms | responseSendMax=$fetchResponseSendMax ms | totalMax=$fetchTotalMax ms"

        Show-Category 'FETCH     ' $fetchLine $false

        $storageLine =
            "flushes(10s)=$flushCountDelta | flushMean=$flushMean ms | flushP99=$flushP99 ms | flushMax=$flushMax ms | offlineLogDirs=$offlineDirs"

        $storageBad = ($offlineDirs -gt 0)

        Show-Category 'STORAGE   ' $storageLine $storageBad

        Write-Host ''
        Write-Host 'Note: values marked (10s) are new deltas between fresh polls. Timer max/mean/p99 values are historical since metric/JVM startup.' -ForegroundColor DarkGray

        $previous = @{
            YoungGcTime = if ($youngGc) { $youngGc.CollectionTime } else { $null }
            OldGcTime = if ($oldGc) { $oldGc.CollectionTime } else { $null }
            FetchCount = if ($fetchTotal) { $fetchTotal.Count } else { $null }
            IsrShrinks = $isrShrinks
            IsrExpands = $isrExpands
            ZkDisconnects = $zkDisconnects
            ZkExpires = $zkExpires
            BytesIn = $bytesIn
            BytesOut = $bytesOut
            MessagesIn = $messagesIn
            FlushCount = if ($flush) { $flush.Count } else { $null }
        }
    }
    catch {
        Write-Host "$(Get-Date -Format 'HH:mm:ss') JOLOKIA FAILED: $($_.Exception.Message)" -ForegroundColor Red
    }

    Start-Sleep -Seconds $intervalSeconds
}
