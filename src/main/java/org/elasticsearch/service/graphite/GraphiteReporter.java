package org.elasticsearch.service.graphite;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.id.IdCacheStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.network.NetworkStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphiteReporter {

    private static final ESLogger logger = ESLoggerFactory.getLogger(GraphiteReporter.class.getName());

    private boolean isPrimary;
    private List<IndexShard> indexShards;
    private NodeStats nodeStats;
    private final long timestamp;
    private final NodeIndicesStats nodeIndicesStats;
    private final String nodeName;
    private final GraphiteSocket graphiteSocket;

    public GraphiteReporter(GraphiteSocket graphiteSocket, String nodeName, boolean isPrimary, NodeIndicesStats nodeIndicesStats,
                            List<IndexShard> indexShards, NodeStats nodeStats) {
        this.graphiteSocket = graphiteSocket;
        this.nodeName = nodeName;
        this.isPrimary = isPrimary;
        this.indexShards = indexShards;
        this.nodeStats = nodeStats;
        this.timestamp = System.currentTimeMillis() / 1000;
        this.nodeIndicesStats = nodeIndicesStats;
    }

    public void run() {
        try {
            sendNodeIndicesStats();
            sendIndexShardStats();
            sendNodeStats();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void sendNodeStats() throws IOException {
        sendNodeFsStats(nodeStats.getFs());
        sendNodeHttpStats(nodeStats.getHttp());
        sendNodeJvmStats(nodeStats.getJvm());
        sendNodeNetworkStats(nodeStats.getNetwork());
        sendNodeOsStats(nodeStats.getOs());
        sendNodeProcessStats(nodeStats.getProcess());
        sendNodeTransportStats(nodeStats.getTransport());
        sendNodeThreadPoolStats(nodeStats.getThreadPool());
    }

    private void sendNodeThreadPoolStats(ThreadPoolStats threadPoolStats) throws IOException {
        String type = buildNodeMetric("threadpool");
        for (ThreadPoolStats.Stats stats : threadPoolStats) {
            String id = type + "." + stats.getName();
            send(id, "threads", stats.getThreads());
            send(id, "queue", stats.getQueue());
            send(id, "active", stats.getActive());
            send(id, "rejected", stats.getRejected());
            send(id, "largest", stats.getLargest());
            send(id, "completed", stats.getCompleted());
        }
    }

    private void sendNodeTransportStats(TransportStats transportStats) throws IOException {
        String type = buildNodeMetric("transport");
        send(type, "serverOpen", transportStats.serverOpen());
        send(type, "rxCount", transportStats.rxCount());
        send(type, "rxSizeBytes", transportStats.rxSize().bytes());
        send(type, "txCount", transportStats.txCount());
        send(type, "txSizeBytes", transportStats.txSize().bytes());
    }

    private void sendNodeProcessStats(ProcessStats processStats) throws IOException {
        String type = buildNodeMetric("process");
        ProcessStats.Cpu processStatsCpu = processStats.cpu();
        ProcessStats.Mem processStatsMem = processStats.mem();

        send(type, "openFileDescriptors", processStats.openFileDescriptors());
        if (processStatsCpu != null) {
            send(type + ".cpu", "percent", processStatsCpu.percent());
            send(type + ".cpu", "sysSeconds", processStatsCpu.sys().seconds());
            send(type + ".cpu", "totalSeconds", processStatsCpu.total().seconds());
            send(type + ".cpu", "userSeconds", processStatsCpu.user().seconds());
        }

        if (processStatsMem != null) {
            send(type + ".mem", "totalVirtual", processStatsMem.totalVirtual().bytes());
            send(type + ".mem", "resident", processStatsMem.resident().bytes());
            send(type + ".mem", "share", processStatsMem.share().bytes());
        }
    }

    private void sendNodeOsStats(OsStats osStats) throws IOException {
        String type = buildNodeMetric("os");

        if (osStats.cpu() != null) {
            send(type + ".cpu", "sys", osStats.cpu().sys());
            send(type + ".cpu", "idle", osStats.cpu().idle());
            send(type + ".cpu", "user", osStats.cpu().user());
        }

        if (osStats.mem() != null) {
            send(type + ".mem", "freeBytes", osStats.mem().free().bytes());
            send(type + ".mem", "usedBytes", osStats.mem().used().bytes());
            send(type + ".mem", "freePercent", osStats.mem().freePercent());
            send(type + ".mem", "usedPercent", osStats.mem().usedPercent());
            send(type + ".mem", "actualFreeBytes", osStats.mem().actualFree().bytes());
            send(type + ".mem", "actualUsedBytes", osStats.mem().actualUsed().bytes());
        }

        if (osStats.swap() != null) {
            send(type + ".swap", "freeBytes", osStats.swap().free().bytes());
            send(type + ".swap", "usedBytes", osStats.swap().used().bytes());
        }
    }

    private void sendNodeNetworkStats(NetworkStats networkStats) throws IOException {
        String type = buildNodeMetric("network.tcp");
        NetworkStats.Tcp tcp = networkStats.tcp();

        // might be null, if sigar isnt loaded
        if (tcp != null) {
            send(type, "activeOpens", tcp.activeOpens());
            send(type, "passiveOpens", tcp.passiveOpens());
            send(type, "attemptFails", tcp.attemptFails());
            send(type, "estabResets", tcp.estabResets());
            send(type, "currEstab", tcp.currEstab());
            send(type, "inSegs", tcp.inSegs());
            send(type, "outSegs", tcp.outSegs());
            send(type, "retransSegs", tcp.retransSegs());
            send(type, "inErrs", tcp.inErrs());
            send(type, "outRsts", tcp.outRsts());
        }
    }

    private void sendNodeJvmStats(JvmStats jvmStats) throws IOException {
        String type = buildNodeMetric("jvm");
        send(type, "uptime", jvmStats.uptime().seconds());

        // mem
        send(type + ".mem", "heapCommitted", jvmStats.mem().heapCommitted().bytes());
        send(type + ".mem", "heapUsed", jvmStats.mem().heapUsed().bytes());
        send(type + ".mem", "nonHeapCommitted", jvmStats.mem().nonHeapCommitted().bytes());
        send(type + ".mem", "nonHeapUsed", jvmStats.mem().nonHeapUsed().bytes());

        Iterator<JvmStats.MemoryPool> memoryPoolIterator = jvmStats.mem().iterator();
        while (memoryPoolIterator.hasNext()) {
            JvmStats.MemoryPool memoryPool = memoryPoolIterator.next();
            String memoryPoolType = type + ".mem.pool." + memoryPool.name();

            send(memoryPoolType, "max", memoryPool.max().bytes());
            send(memoryPoolType, "used", memoryPool.used().bytes());
            send(memoryPoolType, "peakUsed", memoryPool.peakUsed().bytes());
            send(memoryPoolType, "peakMax", memoryPool.peakMax().bytes());
        }

        // threads
        send(type + ".threads", "count", jvmStats.threads().count());
        send(type + ".threads", "peakCount", jvmStats.threads().peakCount());

        // garbage collectors
        for (JvmStats.GarbageCollector collector : jvmStats.gc().collectors()) {
            String id = type + ".gc." + collector.name();
            send(id, "collectionCount", collector.collectionCount());
            send(id, "collectionTimeSeconds", collector.collectionTime().seconds());

            JvmStats.GarbageCollector.LastGc lastGc = collector.lastGc();
            String lastGcType = type + ".lastGc";
            if (lastGc != null) {
                send(lastGcType, "startTime", lastGc.startTime());
                send(lastGcType, "endTime", lastGc.endTime());
                send(lastGcType, "max", lastGc.max().bytes());
                send(lastGcType, "beforeUsed", lastGc.beforeUsed().bytes());
                send(lastGcType, "afterUsed", lastGc.afterUsed().bytes());
                send(lastGcType, "durationSeconds", lastGc.duration().seconds());
            }
        }

        // TODO: bufferPools - where to get them?
    }

    private void sendNodeHttpStats(HttpStats httpStats) throws IOException {
        String type = buildNodeMetric("http");
        send(type, "serverOpen", httpStats.getServerOpen());
        send(type, "totalOpen", httpStats.getTotalOpen());
    }

    private void sendNodeFsStats(FsStats fs) throws IOException {
        int i = 0;
        for (FsStats.Info info : fs) {
            String type = buildNodeMetric("fs") + i;
            send(type, "available", info.getAvailable().bytes());
            send(type, "total", info.getTotal().bytes());
            send(type, "free", info.getFree().bytes());
            send(type, "diskReads", info.getDiskReads());
            send(type, "diskReadsInBytes", info.getDiskReadSizeInBytes());
            send(type, "diskWrites", info.getDiskWrites());
            send(type, "diskWritesInBytes", info.getDiskWriteSizeInBytes());
            send(type, "diskQueue", info.getDiskQueue());
            send(type, "diskService", info.getDiskServiceTime());
            i++;
        }
    }

    private void sendIndexShardStats() throws IOException {
        if (!isPrimary) {
            return;
        }
        for (IndexShard indexShard : indexShards) {
            String type = "indexes." + indexShard.shardId().index().name() + ".id." + indexShard.shardId().id();
            sendIndexShardStats(type, indexShard);
        }
    }

    private void sendIndexShardStats(String type, IndexShard indexShard) throws IOException {
        sendSearchStats(type + ".search", indexShard.searchStats());
        sendGetStats(type + ".get", indexShard.getStats());
        sendDocsStats(type + ".docs", indexShard.docStats());
        sendRefreshStats(type + ".refresh", indexShard.refreshStats());
        sendIndexingStats(type + ".indexing", indexShard.indexingStats("_all"));
        sendMergeStats(type + ".merge", indexShard.mergeStats());
        sendWarmerStats(type + ".warmer", indexShard.warmerStats());
        sendStoreStats(type + ".store", indexShard.storeStats());
    }

    private void sendStoreStats(String type, StoreStats storeStats) throws IOException {
        send(type, "sizeInBytes", storeStats.sizeInBytes());
        send(type, "throttleTimeInNanos", storeStats.throttleTime().getNanos());
    }

    private void sendWarmerStats(String type, WarmerStats warmerStats) throws IOException {
        send(type, "current", warmerStats.current());
        send(type, "total", warmerStats.total());
        send(type, "totalTimeInMillis", warmerStats.totalTimeInMillis());
    }

    private void sendMergeStats(String type, MergeStats mergeStats) throws IOException {
        send(type, "total", mergeStats.getTotal());
        send(type, "totalTimeInMillis", mergeStats.getTotalTimeInMillis());
        send(type, "totalNumDocs", mergeStats.getTotalNumDocs());
        send(type, "current", mergeStats.getCurrent());
        send(type, "currentNumDocs", mergeStats.getCurrentNumDocs());
        send(type, "currentSizeInBytes", mergeStats.getCurrentSizeInBytes());
    }

    private void sendNodeIndicesStats() throws IOException {
        String type = nodeName;
        sendFilterCacheStats(type + ".filtercache", nodeIndicesStats.getFilterCache());
        sendIdCacheStats(type + ".idcache", nodeIndicesStats.getIdCache());
        sendDocsStats(type + ".docs", nodeIndicesStats.getDocs());
        sendFlushStats(type + ".flush", nodeIndicesStats.getFlush());
        sendGetStats(type + ".get", nodeIndicesStats.getGet());
        sendIndexingStats(type + ".indexing", nodeIndicesStats.getIndexing());
        sendRefreshStats(type + ".refresh", nodeIndicesStats.getRefresh());
        sendSearchStats(type + ".search", nodeIndicesStats.getSearch());
        sendFieldDataStats(type + ".fielddata", nodeIndicesStats.getFieldData());
    }

    private void sendSearchStats(String type, SearchStats searchStats) throws IOException {
        SearchStats.Stats totalSearchStats = searchStats.getTotal();
        sendSearchStatsStats(type + "._all", totalSearchStats);

        if (searchStats.getGroupStats() != null ) {
            for (Map.Entry<String, SearchStats.Stats> statsEntry : searchStats.getGroupStats().entrySet()) {
                sendSearchStatsStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendSearchStatsStats(String group, SearchStats.Stats searchStats) throws IOException {
        send(group, "queryCount", searchStats.getQueryCount());
        send(group, "queryTimeInMillis", searchStats.getQueryTimeInMillis());
        send(group, "queryCurrent", searchStats.getQueryCurrent());
        send(group, "fetchCount", searchStats.getFetchCount());
        send(group, "fetchTimeInMillis", searchStats.getFetchTimeInMillis());
        send(group, "fetchCurrent", searchStats.getFetchCurrent());
    }

    private void sendRefreshStats(String type, RefreshStats refreshStats) throws IOException {
        send(type, "total", refreshStats.getTotal());
        send(type, "totalTimeInMillis", refreshStats.getTotalTimeInMillis());
    }

    private void sendIndexingStats(String type, IndexingStats indexingStats) throws IOException {
        IndexingStats.Stats totalStats = indexingStats.getTotal();
        sendStats(type + "._all", totalStats);

        Map<String, IndexingStats.Stats> typeStats = indexingStats.getTypeStats();
        if (typeStats != null) {
            for (Map.Entry<String, IndexingStats.Stats> statsEntry : typeStats.entrySet()) {
                sendStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendStats(String type, IndexingStats.Stats stats) throws IOException {
        send(type, "indexCount", stats.getIndexCount());
        send(type, "indexTimeInMillis", stats.getIndexTimeInMillis());
        send(type, "indexCurrent", stats.getIndexCount());
        send(type, "deleteCount", stats.getDeleteCount());
        send(type, "deleteTimeInMillis", stats.getDeleteTimeInMillis());
        send(type, "deleteCurrent", stats.getDeleteCurrent());
    }

    private void sendGetStats(String type, GetStats getStats) throws IOException {
        send(type, "existsCount", getStats.getExistsCount());
        send(type, "existsTimeInMillis", getStats.getExistsTimeInMillis());
        send(type, "missingCount", getStats.getMissingCount());
        send(type, "missingTimeInMillis", getStats.getMissingTimeInMillis());
        send(type, "current", getStats.current());
    }

    private void sendFlushStats(String type, FlushStats flush) throws IOException {
        send(type, "total", flush.getTotal());
        send(type, "totalTimeInMillis", flush.getTotalTimeInMillis());
    }

    private void sendDocsStats(String name, DocsStats docsStats) throws IOException {
        send(name, "count", docsStats.getCount());
        send(name, "deleted", docsStats.getDeleted());
    }

    private void sendIdCacheStats(String name, IdCacheStats idCache) throws IOException {
        send(name, "memorySizeInBytes", idCache.getMemorySizeInBytes());
    }

    private void sendFilterCacheStats(String name, FilterCacheStats filterCache) throws IOException {
        send(name, "memorySizeInBytes", filterCache.getMemorySizeInBytes());
        send(name, "evictions", filterCache.getEvictions());
    }

    private void sendFieldDataStats(String name, FieldDataStats fieldDataStats) throws IOException {
        send(name, "memorySizeInBytes", fieldDataStats.getMemorySizeInBytes());
        send(name, "evictions", fieldDataStats.getEvictions());
    }

    protected void send(String name, String valueName, Object value) throws IOException {
        graphiteSocket.send(name + '.' + valueName, value, timestamp);
    }

    protected String buildNodeMetric(String metric) {
        return nodeName + '.' + metric;
    }
}
