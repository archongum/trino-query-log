package com.github.archongum.trino.query.log;

import java.lang.reflect.Field;
import java.util.Optional;
import ch.qos.logback.classic.LoggerContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.resourcegroups.QueryType;
import org.slf4j.Logger;


public class QueryLogListener implements EventListener {
    private final Logger logger;
    private final ObjectMapper mapper;
    private final QueryLogListenerProperties properties;

    public QueryLogListener(LoggerContext loggerContext, ObjectMapper mapper, QueryLogListenerProperties properties) {
        this.logger = loggerContext.getLogger(QueryLogListener.class);
        this.mapper = mapper;
        this.properties = properties;
    }

    @Override
    public void queryCreated(QueryCreatedEvent event) {
        if (!properties.isQueryCreated()) {
            return;
        }
        Optional<QueryType> queryType = event.getContext().getQueryType();
        if (queryType.isEmpty() || !queryType.get().name().matches(properties.getQueryCreatedQueryTypePattern())) {
            return;
        }
        handleQueryLength(event.getMetadata(), properties.getQueryCreatedQueryMaxLength());
        try {
            logger.info(mapper.writeValueAsString((event)));
        } catch (JsonProcessingException ignored) {
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event) {
        if (!properties.isQueryCompleted()) {
            return;
        }
        Optional<QueryType> queryType = event.getContext().getQueryType();
        if (queryType.isEmpty() || !queryType.get().name().matches(properties.getQueryCompletedQueryTypePattern())) {
            return;
        }
        for (QueryInputMetadata i : event.getIoMetadata().getInputs()) {
            if (i.getCatalogName().matches(properties.getQueryCompletedCatalogPattern())) {
                handleQueryLength(event.getMetadata(), properties.getQueryCompletedQueryMaxLength());
                try {
                    logger.info(mapper.writeValueAsString(CustomQueryCompletedEvent.of(event)));
                } catch (JsonProcessingException ignored) {
                }
                return;
            }
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event) {
        if (!properties.isSplitCompleted()) {
            return;
        }
        try {
            logger.info(mapper.writeValueAsString(event));
        } catch (JsonProcessingException ignored) {
        }
    }

    private void handleQueryLength(QueryMetadata metadata, int maxLength) {
        if (maxLength != -1) {
            // handle query
            String query = metadata.getQuery();
            if (query.length() > maxLength) {
                StringBuilder sb = new StringBuilder(maxLength);
                sb.append(query, 0, (maxLength - 4) / 2);
                sb.append(" <truncated> ");
                sb.append(query, query.length() - (maxLength - 4) / 2, query.length());
                try {
                    Field queryField = metadata.getClass().getDeclaredField("query");
                    queryField.setAccessible(true);
                    queryField.set(metadata, sb.toString());
                } catch (NoSuchFieldException | IllegalAccessException ignore) {
                }
            }
            // handle prepare query
            String preparedQuery = metadata.getPreparedQuery().orElse("");
            if (preparedQuery.length() > maxLength) {
                StringBuilder sb = new StringBuilder(maxLength);
                sb.append(preparedQuery, 0, (maxLength - 4) / 2);
                sb.append(" <truncated> ");
                sb.append(preparedQuery, preparedQuery.length() - (maxLength - 4) / 2, preparedQuery.length());
                try {
                    Field queryField = metadata.getClass().getDeclaredField("preparedQuery");
                    queryField.setAccessible(true);
                    queryField.set(metadata, Optional.of(sb.toString()));
                } catch (NoSuchFieldException | IllegalAccessException ignore) {
                }
            }
        }
    }
}
