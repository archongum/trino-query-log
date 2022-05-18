package com.github.archongum.trino.query.log;

import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.EventListenerFactory;

public class QueryLogListenerFactory implements EventListenerFactory {

    private static final String QUERY_LOG_PLUGIN_NAME = "trino-query-log";

    @Override
    public String getName() {
        return QUERY_LOG_PLUGIN_NAME;
    }

    @Override
    public EventListener create(Map<String, String> map) {
        // 1. properties
        QueryLogListenerProperties p = QueryLogListenerProperties.of(map);
        // 2. Logger Context
        LoggerContext loggerContext = new LoggerContext();
        ContextInitializer initializer = new ContextInitializer(loggerContext);
        try {
            initializer.configureByResource(Paths.get(p.getConfigFileLocation()).toUri().toURL());
        } catch (JoranException | MalformedURLException ignored) {
        }
        // 3. Object Mapper
        ObjectMapper mapper = new ObjectMapper();
        // handle Instant Class
        mapper.registerModule(new JavaTimeModule()
            .addSerializer(Instant.class, new ISOInstantSerializer())
            .addDeserializer(Instant.class, new ISOInstantDeserializer())
        );
        // handle Optional Class
        mapper.registerModule(new Jdk8Module());
        return new QueryLogListener(loggerContext, mapper, p);
    }
}
