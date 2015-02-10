package org.elasticsearch.module.graphite;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.service.graphite.GraphiteService;

public class GraphiteModule extends AbstractModule {
    @Override
    protected void configure() {
        bind(GraphiteService.class).asEagerSingleton();
    }
}
