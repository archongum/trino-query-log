package com.github.archongum.trino.query.log;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.ContextInitializer;
import ch.qos.logback.core.joran.spi.JoranException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryIOMetadata;
import io.trino.spi.eventlistener.QueryInputMetadata;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import io.trino.spi.eventlistener.SplitStatistics;
import io.trino.spi.resourcegroups.QueryType;
import io.trino.spi.session.ResourceEstimates;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


class QueryLogListenerTest {

    static ObjectMapper mapper;

    @BeforeAll
    static void setup() {
        mapper = new ObjectMapper();
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new JavaTimeModule()
            .addSerializer(Instant.class, new ISOInstantSerializer())
            .addDeserializer(Instant.class, new ISOInstantDeserializer())
        );
    }

    @Test
    void regexMatch() {
        List<String>  catalogs = Arrays.asList("$info", "hive");
        for (String c : catalogs) {
            System.out.printf("catalog=%s, match=%s%n", c, c.matches("^\\w+"));
        }
        List<String>  queryType = Arrays.asList("DATA_DEFINITION", "SELECT");
        for (String q : queryType) {
            System.out.printf("catalog=%s, match=%s%n", q, q.matches("DATA_DEFINITION|SELECT"));
        }
    }

    @Test
    void shrinkTrinoQuery() {
        String s = "123456789012345678901234567890";
        StringBuilder sb = new StringBuilder();
        sb.append(s, 0, 10);
        sb.append("....");
        sb.append(s, s.length()-10, s.length());
        System.out.println(sb);
        System.out.println(sb.length());
    }

    @Test
    void dateFormat() {
        Instant i = Instant.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
        String r = formatter.format(LocalDateTime.ofInstant(i, ZoneId.of("UTC")));
        System.out.println(r);
        String path = new File(QueryLogListenerTest.class.getClassLoader().getResource(".").getFile()).toString();
        System.out.println(path);
    }


    @Test
    void handleOptional() throws JsonProcessingException {
        class Book {
            final String title;
            final Optional<String> subTitle;
            final Instant time;

            @JsonCreator
            public Book(String title, Optional<String> subTitle, Instant time) {
                this.title = title;
                this.subTitle = subTitle;
                this.time = time;
            }

            @JsonProperty
            public String getTitle() {
                return title;
            }

            @JsonProperty
            public Optional<String> getSubTitle() {
                return subTitle;
            }

            @JsonProperty
            public Instant getTime() {
                return time;
            }
        }

        Book book = new Book("title", Optional.of("subtitle"), LocalDateTime.now().toInstant(ZoneOffset.UTC));
        String rs = mapper.writeValueAsString(book);
        System.out.println(rs);
    }

    @Test
    void queryCreatedEvents() throws IOException {
        String logBaseDir = new File(QueryLogListenerTest.class.getClassLoader().getResource(".").getPath()).toString();
        LoggerContext loggerContext = new LoggerContext();
        loggerContext.putProperty("logs.dir", logBaseDir);

        ContextInitializer initializer = new ContextInitializer(loggerContext);
        URL log4j2ConfigLocation = QueryLogListenerTest.class.getClassLoader().getResource("queryCreatedEvents.xml");
        try {
            initializer.configureByResource(log4j2ConfigLocation);
        } catch (JoranException e) {
            e.printStackTrace();
        }

        QueryLogListener listener = new QueryLogListener(
            loggerContext,
            mapper,
            QueryLogListenerProperties.defaultInstance()
                .setSplitCompleted(false)
        );
        QueryLogListener truncateListener = new QueryLogListener(
            loggerContext,
            mapper,
            QueryLogListenerProperties.defaultInstance()
                .setQueryCreatedQueryMaxLength(20)
        );

        listener.splitCompleted(prepareSplitCompletedEvent());
        listener.queryCreated(prepareQueryCreatedEvent());
        truncateListener.queryCreated(prepareQueryCreatedEvent());

        List<String> lines = Files.lines(Paths.get(logBaseDir, "queryCreatedEvents.log")).collect(Collectors.toList());
        assertEquals(2, lines.size());
        assertTrue(lines.get(1).contains("<truncated>"));
    }


    @Test
    void queryCompletedEvent() throws IOException {
        String logBaseDir = new File(QueryLogListenerTest.class.getClassLoader().getResource(".").getPath()).toString();
        LoggerContext loggerContext = new LoggerContext();
        loggerContext.putProperty("logs.dir", logBaseDir);

        ContextInitializer initializer = new ContextInitializer(loggerContext);
        URL log4j2ConfigLocation = QueryLogListenerTest.class.getClassLoader().getResource("queryCompletedEvents.xml");
        try {
            initializer.configureByResource(log4j2ConfigLocation);
        } catch (JoranException e) {
            e.printStackTrace();
        }

        QueryLogListener listener = new QueryLogListener(
            loggerContext,
            mapper,
            QueryLogListenerProperties.defaultInstance()
                .setSplitCompleted(false)
                .setQueryCreated(false)
        );
        QueryLogListener truncateListener = new QueryLogListener(
            loggerContext,
            mapper,
            QueryLogListenerProperties.defaultInstance()
                .setQueryCompletedQueryMaxLength(20)
        );
        QueryLogListener wontTruncateListener = new QueryLogListener(
            loggerContext,
            mapper,
            QueryLogListenerProperties.defaultInstance()
                .setQueryCompletedQueryMaxLength(Integer.MAX_VALUE)
        );

        listener.splitCompleted(prepareSplitCompletedEvent());
        listener.queryCreated(prepareQueryCreatedEvent());
        listener.queryCompleted(prepareCompletedEvent());
        truncateListener.queryCompleted(prepareCompletedEvent());
        wontTruncateListener.queryCompleted(prepareCompletedEvent());

        List<String> lines = Files.lines(Paths.get(logBaseDir, "queryCompletedEvents.log")).collect(Collectors.toList());
        assertEquals(3, lines.size());
        assertTrue(lines.get(1).contains("<truncated>"));
        assertFalse(lines.get(2).contains("<truncated>"));
    }

    private QueryCreatedEvent prepareQueryCreatedEvent() {
        return new QueryCreatedEvent(
                Instant.now(),
                getQueryContext(),
                getQueryMetadata()
        );
    }

    private SplitCompletedEvent prepareSplitCompletedEvent() {
        return new SplitCompletedEvent(
                "queryId",
                "stageId",
                "taskId",
                Optional.empty(),
                Instant.now(),
                Optional.of(Instant.now()),
                Optional.of(Instant.now()),
                getSplitStatistics(),
                Optional.empty(),
                "payload"
        );
    }

    private QueryCompletedEvent prepareCompletedEvent() {
        return new QueryCompletedEvent(
            getQueryMetadata(),
            getQueryStatistics(),
            getQueryContext(),
            getQueryIOMetadata(),
            Optional.empty(),
            Collections.emptyList(),
            LocalDateTime.now().toInstant(ZoneOffset.UTC),
            LocalDateTime.now().toInstant(ZoneOffset.UTC),
            LocalDateTime.now().toInstant(ZoneOffset.UTC)
        );
    }

    private SplitStatistics getSplitStatistics() {
        return new SplitStatistics(
                ofMillis(1000),
                ofMillis(2000),
                ofMillis(3000),
                ofMillis(4000),
                1,
                2,
                Optional.of(Duration.ofMillis(100)),
                Optional.of(Duration.ofMillis(200))
        );
    }

    private QueryMetadata getQueryMetadata() {
        return new QueryMetadata(
            "queryId",
            Optional.empty(),
            "select * from dim_date limit 10000",
            Optional.of("updateType"),
            Optional.of("prepare s1 from select * from dim_date limit 10000"),
            "queryState",
            new ArrayList<>(),
            new ArrayList<>(),
            URI.create("http://localhost:18010/"),
            Optional.empty(),
            Optional.empty()
        );
    }

    private QueryContext getQueryContext() {
        return new QueryContext(
            "user",
            Optional.of("principal"),
            new HashSet<>(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            new HashSet<>(),
            new HashSet<>(),
            Optional.empty(),
            Optional.of("hive"),
            Optional.empty(),
            Optional.empty(),
            new HashMap<>(),
            new ResourceEstimates(Optional.empty(), Optional.empty(), Optional.empty()),
            "serverAddress",
            "serverVersion",
            "environment",
            Optional.of(QueryType.SELECT)
        );
    }

    private QueryIOMetadata getQueryIOMetadata() {
        return new QueryIOMetadata(
            Collections.singletonList(new QueryInputMetadata(
                "hive",
                "ads",
                "dim_date",
                Collections.emptyList(),
                Optional.empty(),
                OptionalLong.of(1),
                OptionalLong.of(2)
            )),
            Optional.empty()
        );
    }

    private QueryStatistics getQueryStatistics() {
        return new QueryStatistics(
            ofMillis(1000),
            ofMillis(2000),
            ofMillis(3000),
            ofMillis(4000),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            Optional.of(Duration.ofMillis(100)),
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            1.1,
            1.2,
            Collections.emptyList(),
            11,
            true,
            Collections.emptyList(),
            Collections.emptyList(),
            Optional.of("plan")
        );
    }
}
