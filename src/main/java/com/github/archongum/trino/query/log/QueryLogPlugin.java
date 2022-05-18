package com.github.archongum.trino.query.log;

import java.util.Collections;
import io.trino.spi.Plugin;
import io.trino.spi.eventlistener.EventListenerFactory;

public class QueryLogPlugin implements Plugin {

    @Override
    public Iterable<EventListenerFactory> getEventListenerFactories() {
        return Collections.singletonList(new QueryLogListenerFactory());
    }
}
