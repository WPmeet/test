# jk-5-6.ps1
# Kafka broker 5 + broker 6 Jolokia watcher.
# Appends a fresh block every 10 seconds. Stop with Ctrl+C.

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
    param(
        [array]$Results,
        [string]$MBean
    )

    $item = Get-Entry $Results $MBean

    if ($null -eq $item) {
        return $null
    }

    return $item.value
}

function Get-Delta {
    param(
        $Current,
        $Previous
    )

    if ($null -eq $Current -or $null -eq $Previous) {
        return 'N/A'
    }

    return [math]::Round(
        ([double]$Current - [double]$Previous),
        1
    )
}

function Get-NumberOrNA {
    param(
        $Value,
        [int]$Decimals = 1
    )

    if ($null -eq $Value) {
        return 'N/A'
    }

    if ($Value -is [string]) {
        return $Value
    }

    return [math]::Round(
        [double]$Value,
        $Decimals
    )
}

function Show-Line {
    param(
        [string]$Category,
        [string]$Text,
        [bool]$Bad = $false
    )

    Write-Host $Category -ForegroundColor Yellow -NoNewline

    if ($Bad) {
        Write-Host $Text -ForegroundColor Red
    }
    else {
        Write-Host $Text
    }
}

function Show-BrokerMetrics {
    param(
        [hashtable]$Broker,
        [array]$Results,
        [hashtable]$Previous
    )

    $memory = Get-Object $Results 'java.lang:type=Memory'
    $heap = $null

    if ($null -ne $memory) {
        $heap = $memory.HeapMemoryUsage
    }

    $os = Get-Object $Results 'java.lang:type=OperatingSystem'

    $youngGc = Get-Object `
        $Results `
        'java.lang:name=G1 Young Generation,type=GarbageCollector'

    $oldGc = Get-Object `
        $Results `
        'java.lang:name=G1 Old Generation,type=GarbageCollector'

    $fetchTotal = Get-Object `
        $Results `
        'kafka.network:name=TotalTimeMs,request=FetchFollower,type=RequestMetrics'

    $fetchQueue = Get-Object `
        $Results `
        'kafka.network:name=RequestQueueTimeMs,request=FetchFollower,type=RequestMetrics'

    $fetchLocal = Get-Object `
        $Results `
        'kafka.network:name=LocalTimeMs,request=FetchFollower,type=RequestMetrics'

    $fetchResponseQueue = Get-Object `
        $Results `
        'kafka.network:name=ResponseQueueTimeMs,request=FetchFollower,type=RequestMetrics'

    $fetchResponseSend = Get-Object `
        $Results `
        'kafka.network:name=ResponseSendTimeMs,request=FetchFollower,type=RequestMetrics'

    $flush = Get-Object `
        $Results `
        'kafka.log:name=LogFlushRateAndTimeMs,type=LogFlushStats'

    $state = Get-Scalar $Results 'kafka.server:name=BrokerState,type=KafkaServer'
    $networkIdle = Get-Scalar $Results 'kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer'
    $requestQueue = Get-Scalar $Results 'kafka.network:name=RequestQueueSize,type=RequestChannel'

    $urp = Get-Scalar $Results 'kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager'
    $minIsr = Get-Scalar $Results 'kafka.server:name=UnderMinIsrPartitionCount,type=ReplicaManager'
    $offlineReplicas = Get-Scalar $Results 'kafka.server:name=OfflineReplicaCount,type=ReplicaManager'
    $partitions = Get-Scalar $Results 'kafka.server:name=PartitionCount,type=ReplicaManager'
    $leaders = Get-Scalar $Results 'kafka.server:name=LeaderCount,type=ReplicaManager'
    $reassigning = Get-Scalar $Results 'kafka.server:name=ReassigningPartitions,type=ReplicaManager'
    $isrShrinks = Get-Scalar $Results 'kafka.server:name=IsrShrinksPerSec,type=ReplicaManager'
    $isrExpands = Get-Scalar $Results 'kafka.server:name=IsrExpandsPerSec,type=ReplicaManager'

    $offlineDirs = Get-Scalar $Results 'kafka.log:name=OfflineLogDirectoryCount,type=LogManager'
    $zkDisconnects = Get-Scalar $Results 'kafka.server:name=ZooKeeperDisconnectsPerSec,type=SessionExpireListener'
    $zkExpires = Get-Scalar $Results 'kafka.server:name=ZooKeeperExpiresPerSec,type=SessionExpireListener'

    $bytesIn = Get-Scalar $Results 'kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics'
    $bytesOut = Get-Scalar $Results 'kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics'
    $messagesIn = Get-Scalar $Results 'kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics'

    $activeController = Get-Scalar $Results 'kafka.controller:name=ActiveControllerCount,type=KafkaController'

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

    $youngGcDelta = Get-Delta `
        $(if ($youngGc) { $youngGc.CollectionTime } else { $null }) `
        $Previous.YoungGcTime

    $oldGcDelta = Get-Delta `
        $(if ($oldGc) { $oldGc.CollectionTime } else { $null }) `
        $Previous.OldGcTime

    $fetchCountDelta = Get-Delta `
        $(if ($fetchTotal) { $fetchTotal.Count } else { $null }) `
        $Previous.FetchCount

    $isrShrinkDelta = Get-Delta $isrShrinks $Previous.IsrShrinks
    $isrExpandDelta = Get-Delta $isrExpands $Previous.IsrExpands
    $zkDisconnectDelta = Get-Delta $zkDisconnects $Previous.ZkDisconnects
    $zkExpireDelta = Get-Delta $zkExpires $Previous.ZkExpires
    $bytesInDelta = Get-Delta $bytesIn $Previous.BytesIn
    $bytesOutDelta = Get-Delta $bytesOut $Previous.BytesOut
    $messagesInDelta = Get-Delta $messagesIn $Previous.MessagesIn

    $flushCountDelta = Get-Delta `
        $(if ($flush) { $flush.Count } else { $null }) `
        $Previous.FlushCount

    $fetchQueueMax = if ($fetchQueue) {
        Get-NumberOrNA $fetchQueue.Max
    }
    else {
        'N/A'
    }

    $fetchLocalMax = if ($fetchLocal) {
        Get-NumberOrNA $fetchLocal.Max
    }
    else {
        'N/A'
    }

    $fetchResponseQueueMax = if ($fetchResponseQueue) {
        Get-NumberOrNA $fetchResponseQueue.Max
    }
    else {
        'N/A'
    }

    $fetchResponseSendMax = if ($fetchResponseSend) {
        Get-NumberOrNA $fetchResponseSend.Max
    }
    else {
        'N/A'
    }

    $fetchTotalMax = if ($fetchTotal) {
        Get-NumberOrNA $fetchTotal.Max
    }
    else {
        'N/A'
    }

    $flushMean = if ($flush) {
        Get-NumberOrNA $flush.Mean
    }
    else {
        'N/A'
    }

    $flushMax = if ($flush) {
        Get-NumberOrNA $flush.Max
    }
    else {
        'N/A'
    }

    $flushP99 = if ($flush) {
        Get-NumberOrNA $flush.'99thPercentile'
    }
    else {
        'N/A'
    }

    Write-Host ''
    Write-Host "========== $($Broker.Name) | $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ==========" -ForegroundColor Cyan

    $brokerLine =
        "state=$state | partitions=$partitions | leaders=$leaders | activeController=$activeController | URP=$urp | minISR=$minIsr | offlineReplicas=$offlineReplicas | reassigning=$reassigning | ISRshrink(10s)=$isrShrinkDelta | ISRexpand(10s)=$isrExpandDelta"

    $brokerBad =
        ($state -ne 3) -or
        ($urp -gt 0) -or
        ($minIsr -gt 0) -or
        ($offlineReplicas -gt 0) -or
        ($isrShrinkDelta -gt 0)

    Show-Line 'BROKER    ' $brokerLine $brokerBad

    $zkLine =
        "disconnects(total)=$zkDisconnects | disconnects(10s)=$zkDisconnectDelta | expiries(total)=$zkExpires | expiries(10s)=$zkExpireDelta"

    $zkBad =
        ($zkDisconnectDelta -gt 0) -or
        ($zkExpireDelta -gt 0)

    Show-Line 'ZK        ' $zkLine $zkBad

    $jvmLine =
        "heapUsed=$heapUsedGiB GiB | heapMax=$heapMaxGiB GiB | heapUsedPct=$heapPct% | processCPU=$cpuPct% | systemCPU=$systemCpuPct% | youngGC(10s)=$youngGcDelta ms | oldGC(10s)=$oldGcDelta ms"

    $jvmBad =
        ($oldGcDelta -ne 'N/A' -and [double]$oldGcDelta -gt 1000)

    Show-Line 'JVM       ' $jvmLine $jvmBad

    $networkLine =
        "networkIdle=$networkIdlePct% | requestQueue=$requestQueue | bytesIn(10s)=$bytesInDelta | bytesOut(10s)=$bytesOutDelta | messagesIn(10s)=$messagesInDelta"

    $networkBad =
        ($networkIdlePct -ne 'N/A' -and [double]$networkIdlePct -lt 5)

    Show-Line 'NETWORK   ' $networkLine $networkBad

    $fetchLine =
        "fetches(10s)=$fetchCountDelta | queueMax=$fetchQueueMax ms | localMax=$fetchLocalMax ms | responseQueueMax=$fetchResponseQueueMax ms | responseSendMax=$fetchResponseSendMax ms | totalMax=$fetchTotalMax ms"

    Show-Line 'FETCH     ' $fetchLine $false

    $storageLine =
        "flushes(10s)=$flushCountDelta | flushMean=$flushMean ms | flushP99=$flushP99 ms | flushMax=$flushMax ms | offlineLogDirs=$offlineDirs"

    $storageBad = ($offlineDirs -gt 0)

    Show-Line 'STORAGE   ' $storageLine $storageBad

    return @{
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

$previousByBroker = @{}

Write-Host 'Kafka broker 5 + 6 watcher started. New broker blocks append every 10 seconds. Ctrl+C to stop.' -ForegroundColor Cyan
Write-Host 'Values marked (10s) are deltas. Timer Max/Mean/P99 values are cumulative/high-water values since metric/JVM startup.' -ForegroundColor DarkGray

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

            $old = $previousByBroker[$broker.Name]

            if ($null -eq $old) {
                $old = @{}
            }

            $previousByBroker[$broker.Name] = Show-BrokerMetrics `
                -Broker $broker `
                -Results $result `
                -Previous $old
        }
        catch {
            Write-Host "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $($broker.Name) JOLOKIA FAILED: $($_.Exception.Message)" -ForegroundColor Red
        }
    }

    Start-Sleep -Seconds $intervalSeconds
}
