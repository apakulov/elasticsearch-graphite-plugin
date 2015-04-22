package org.elasticsearch.service.graphite;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.node.service.NodeService;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class GraphiteService extends AbstractLifecycleComponent<GraphiteService> {

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final NodeService nodeService;
    private final GraphiteSettings graphiteSettings;
    private volatile ScheduledFuture graphiteReporterFuture;

    @Inject
    public GraphiteService(Settings settings, ClusterService clusterService, IndicesService indicesService,
                                   NodeService nodeService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.nodeService = nodeService;
        this.graphiteSettings = new GraphiteSettings(settings);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (graphiteSettings.isHostDefined()) {
            graphiteReporterFuture = Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(new GraphiteReporterThread(), 0L, graphiteSettings.refreshInterval().seconds(), TimeUnit.SECONDS);
            logger.info("Graphite reporting triggered with settings={}", graphiteSettings);
        } else {
            logger.error("Graphite reporting disabled, no graphite host configured");
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        if (graphiteReporterFuture != null) {
            graphiteReporterFuture.cancel(true);
        }
        logger.info("Graphite reporter stopped");
    }

    @Override
    protected void doClose() throws ElasticsearchException {}

    public class GraphiteReporterThread implements Runnable {
        @Override
        public void run() {
            DiscoveryNode node = clusterService.localNode();

            if (isClusterReady() && node != null) {
                final NodeIndicesStats nodeIndicesStats = indicesService.stats(false);
                final NodeStats nodeStats = nodeService.stats(new CommonStatsFlags().clear(), true, true, true, true, true, true, true, true, true);
                final List<IndexShard> indexShards = getIndexShards(indicesService);

                try (GraphiteSocket graphiteSocket = new GraphiteSocket(graphiteSettings)) {
                    GraphiteReporter graphiteReporter = new GraphiteReporter(graphiteSocket, node.name(), nodeIndicesStats, indexShards, nodeStats);
                    Executors.newSingleThreadExecutor().submit(graphiteReporter).get();
                } catch (Exception e) {
                    logger.error("Something bad happened during graphite data processing", e);
                }
            } else if (node != null) {
                logger.debug("Cluster hasn't started - not triggering update");
            }
        }

        private boolean isClusterReady() {
            return Lifecycle.State.STARTED.equals(clusterService.lifecycleState());
        }

        private List<IndexShard> getIndexShards(IndicesService indicesService) {
            List<IndexShard> indexShards = Lists.newArrayList();
            for (IndexService indexService : indicesService) {
                for (IndexShard indexShard : indexService) {
                    ShardRouting routingEntry = indexShard.routingEntry();
                    if (null != routingEntry && routingEntry.active() && routingEntry.primary()) {
                        indexShards.add(indexShard);
                    }
                }
            }
            return indexShards;
        }
    }
}
