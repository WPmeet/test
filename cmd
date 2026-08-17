# watch-kafka16-compact.ps1
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
{"type":"read","mbean":"kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=UnderMinIsrPartitionCount,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=OfflineReplicaCount,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=PartitionCount,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=LeaderCount,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=ReassigningPartitions,type=ReplicaManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=IsrShrinksPerSec,type=ReplicaManager","attribute":"Count"},
{"type":"read","mbean":"kafka.server:name=IsrExpandsPerSec,type=ReplicaManager","attribute":"Count"},
{"type":"read","mbean":"kafka.log:name=OfflineLogDirectoryCount,type=LogManager","attribute":"Value"},
{"type":"read","mbean":"kafka.server:name=ZooKeeperDisconnectsPerSec,type=SessionExpireListener","attribute":"Count"},
{"type":"read","mbean":"kafka.server:name=ZooKeeperExpiresPerSec,type=SessionExpireListener","attribute":"Count"},
{"type":"read","mbean":"kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics","attribute":"Count"},
{"type":"read","mbean":"kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics","attribute":"Count"},
{"type":"read","mbean":"kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics","attribute":"Count"},
{"type":"read","mbean":"kafka.controller:name=ActiveControllerCount,type=KafkaController","attribute":"Value"}
]
'@

function Get-MBeanValue {
    param([array]$Results, [string]$MBean)

    $item = $Results |
        Where-Object { $_.request.mbean -eq $MBean -and $_.status -eq 200 } |
        Select-Object -First 1

    if ($null -eq $item) {
        return $null
    }

    if ($null -ne $item.value.Value) {
        return $item.value.Value
    }

    return $item.value
}

function Get-Delta {
    param($Current, $Previous)

    if ($null -eq $Current -or $null -eq $Previous) {
        return 'N/A'
    }

    return [math]::Round(([double]$Current - [double]$Previous), 2)
}

$previous = @{}

Write-Host 'Time     State URP MinISR OffRep Part Lead Reasg ISR- ISR+ ZKDisc ZKExp NetIdle ReqQ Heap% CPU% GCYms GCOms OffDir BinΔ BoutΔ' -ForegroundColor Cyan
Write-Host ('-' * 150)

while ($true) {
    try {
        $r = Invoke-RestMethod `
            -Uri $url `
            -Method POST `
            -ContentType 'application/json' `
            -Body $body `
            -TimeoutSec 15 `
            -ErrorAction Stop

        $heap = Get-MBeanValue $r 'java.lang:type=Memory'
        $os = Get-MBeanValue $r 'java.lang:type=OperatingSystem'
        $youngGc = Get-MBeanValue $r 'java.lang:name=G1 Young Generation,type=GarbageCollector'
        $oldGc = Get-MBeanValue $r 'java.lang:name=G1 Old Generation,type=GarbageCollector'

        $state = Get-MBeanValue $r 'kafka.server:name=BrokerState,type=KafkaServer'
        $networkIdle = Get-MBeanValue $r 'kafka.network:name=NetworkProcessorAvgIdlePercent,type=SocketServer'
        $requestQueue = Get-MBeanValue $r 'kafka.network:name=RequestQueueSize,type=RequestChannel'

        $urp = Get-MBeanValue $r 'kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager'
        $minIsr = Get-MBeanValue $r 'kafka.server:name=UnderMinIsrPartitionCount,type=ReplicaManager'
        $offlineReplicas = Get-MBeanValue $r 'kafka.server:name=OfflineReplicaCount,type=ReplicaManager'
        $partitions = Get-MBeanValue $r 'kafka.server:name=PartitionCount,type=ReplicaManager'
        $leaders = Get-MBeanValue $r 'kafka.server:name=LeaderCount,type=ReplicaManager'
        $reassigning = Get-MBeanValue $r 'kafka.server:name=ReassigningPartitions,type=ReplicaManager'
        $isrShrinks = Get-MBeanValue $r 'kafka.server:name=IsrShrinksPerSec,type=ReplicaManager'
        $isrExpands = Get-MBeanValue $r 'kafka.server:name=IsrExpandsPerSec,type=ReplicaManager'

        $offlineDirs = Get-MBeanValue $r 'kafka.log:name=OfflineLogDirectoryCount,type=LogManager'
        $zkDisconnects = Get-MBeanValue $r 'kafka.server:name=ZooKeeperDisconnectsPerSec,type=SessionExpireListener'
        $zkExpires = Get-MBeanValue $r 'kafka.server:name=ZooKeeperExpiresPerSec,type=SessionExpireListener'

        $bytesIn = Get-MBeanValue $r 'kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics'
        $bytesOut = Get-MBeanValue $r 'kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics'
        $messagesIn = Get-MBeanValue $r 'kafka.server:name=MessagesInPerSec,type=BrokerTopicMetrics'

        $heapPct = if ($heap -and $heap.HeapMemoryUsage.max -gt 0) {
            [math]::Round((100 * $heap.HeapMemoryUsage.used / $heap.HeapMemoryUsage.max), 1)
        } else {
            'N/A'
        }

        $cpuPct = if ($os -and $null -ne $os.ProcessCpuLoad) {
            [math]::Round((100 * $os.ProcessCpuLoad), 1)
        } else {
            'N/A'
        }

        $netIdlePct = if ($null -ne $networkIdle) {
            [math]::Round((100 * $networkIdle), 1)
        } else {
            'N/A'
        }

        $gcYoungDelta = Get-Delta $youngGc.CollectionTime $previous.YoungGcTime
        $gcOldDelta = Get-Delta $oldGc.CollectionTime $previous.OldGcTime
        $isrShrinkDelta = Get-Delta $isrShrinks $previous.IsrShrinks
        $isrExpandDelta = Get-Delta $isrExpands $previous.IsrExpands
        $zkDisconnectDelta = Get-Delta $zkDisconnects $previous.ZkDisconnects
        $zkExpiryDelta = Get-Delta $zkExpires $previous.ZkExpires
        $bytesInDelta = Get-Delta $bytesIn $previous.BytesIn
        $bytesOutDelta = Get-Delta $bytesOut $previous.BytesOut

        $line = '{0,-8} {1,5} {2,3} {3,6} {4,6} {5,4} {6,4} {7,5} {8,5} {9,4} {10,4} {11,6} {12,5} {13,7} {14,4} {15,5} {16,5} {17,5} {18,5} {19,7} {20,7}' -f `
            (Get-Date -Format 'HH:mm:ss'),
            $state,
            $urp,
            $minIsr,
            $offlineReplicas,
            $partitions,
            $leaders,
            $reassigning,
            $isrShrinkDelta,
            $isrExpandDelta,
            $zkDisconnectDelta,
            $zkExpiryDelta,
            $netIdlePct,
            $requestQueue,
            $heapPct,
            $cpuPct,
            $gcYoungDelta,
            $gcOldDelta,
            $offlineDirs,
            $bytesInDelta,
            $bytesOutDelta

        $bad = (
            $state -ne 3 -or
            $urp -gt 0 -or
            $minIsr -gt 0 -or
            $offlineReplicas -gt 0 -or
            $offlineDirs -gt 0 -or
            $isrShrinkDelta -gt 0 -or
            $zkDisconnectDelta -gt 0 -or
            $zkExpiryDelta -gt 0
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
            IsrShrinks = $isrShrinks
            IsrExpands = $isrExpands
            ZkDisconnects = $zkDisconnects
            ZkExpires = $zkExpires
            BytesIn = $bytesIn
            BytesOut = $bytesOut
            MessagesIn = $messagesIn
        }
    }
    catch {
        Write-Host "$(Get-Date -Format 'HH:mm:ss') JOLOKIA FAILED: $($_.Exception.Message)" -ForegroundColor Red
    }

    Start-Sleep -Seconds $intervalSeconds
}
