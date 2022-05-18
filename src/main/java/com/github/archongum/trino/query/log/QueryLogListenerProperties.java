package com.github.archongum.trino.query.log;

import java.util.HashMap;
import java.util.Map;
import static java.util.Objects.requireNonNull;


public class QueryLogListenerProperties {

    private static final String QUERY_LOG_CONFIG_FILE_LOCATION                   = "trino.query.log.config.fileLocation";
    private static final String QUERY_LOG_LOG_SPLIT_COMPLETED                    = "trino.query.log.log.splitCompletedEvent";
    private static final String QUERY_LOG_LOG_QUERY_CREATED                      = "trino.query.log.log.queryCreatedEvent";
    private static final String QUERY_LOG_LOG_QUERY_CREATED_QUERY_TYPE_PATTERN   = "trino.query.log.log.queryCreatedEvent.queryTypePattern";
    private static final String QUERY_LOG_LOG_QUERY_CREATED_QUERY_LENGTH         = "trino.query.log.log.queryCreatedEvent.queryMaxLength";
    private static final String QUERY_LOG_LOG_QUERY_COMPLETED                    = "trino.query.log.log.queryCompletedEvent";
    private static final String QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_TYPE_PATTERN = "trino.query.log.log.queryCompletedEvent.queryTypePattern";
    private static final String QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_LENGTH       = "trino.query.log.log.queryCompletedEvent.queryMaxLength";
    private static final String QUERY_LOG_LOG_QUERY_COMPLETED_CATALOG_PATTERN    = "trino.query.log.log.queryCompletedEvent.catalogPattern";

    private static final String DEFAULT_VALUE_QUERY_LOG_CONFIG_FILE_LOCATION                   = "etc/event-listener-trino-query-log-logback.xml";
    private static final boolean DEFAULT_VALUE_QUERY_LOG_LOG_SPLIT_COMPLETED                   = true;
    private static final boolean DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED                     = true;
    private static final String DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED_QUERY_TYPE_PATTERN   = ".*";
    private static final int DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED_QUERY_LENGTH            = -1;
    private static final boolean DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED                   = true;
    private static final String DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_TYPE_PATTERN = ".*";
    private static final int DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_LENGTH          = -1;
    private static final String DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_CATALOG_PATTERN    = ".*";

    private String configFileLocation;
    private boolean splitCompleted;
    private boolean queryCreated;
    private String queryCreatedQueryTypePattern;
    private int queryCreatedQueryMaxLength;
    private boolean queryCompleted;
    private String queryCompletedQueryTypePattern;
    private int queryCompletedQueryMaxLength;
    private String queryCompletedCatalogPattern;

    private QueryLogListenerProperties(Map<String, String> map) {
        this.configFileLocation = requireNonNull(map.get(QUERY_LOG_CONFIG_FILE_LOCATION), QUERY_LOG_CONFIG_FILE_LOCATION + " is empty");
        this.splitCompleted = getBooleanConfig(map, QUERY_LOG_LOG_SPLIT_COMPLETED, DEFAULT_VALUE_QUERY_LOG_LOG_SPLIT_COMPLETED);
        this.queryCreated = getBooleanConfig(map, QUERY_LOG_LOG_QUERY_CREATED, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED);
        this.queryCreatedQueryTypePattern = getStringConfig(map, QUERY_LOG_LOG_QUERY_CREATED_QUERY_TYPE_PATTERN, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED_QUERY_TYPE_PATTERN);
        this.queryCreatedQueryMaxLength = getIntegerConfig(map, QUERY_LOG_LOG_QUERY_CREATED_QUERY_LENGTH, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED_QUERY_LENGTH);
        this.queryCompleted = getBooleanConfig(map, QUERY_LOG_LOG_QUERY_COMPLETED, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED);
        this.queryCompletedQueryTypePattern = getStringConfig(map, QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_TYPE_PATTERN, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_TYPE_PATTERN);
        this.queryCompletedQueryMaxLength = getIntegerConfig(map, QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_LENGTH, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_LENGTH);
        this.queryCompletedCatalogPattern = getStringConfig(map, QUERY_LOG_LOG_QUERY_COMPLETED_CATALOG_PATTERN, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_CATALOG_PATTERN);
    }

    public static QueryLogListenerProperties of(Map<String, String> map) {
        return new QueryLogListenerProperties(map);
    }

    public static QueryLogListenerProperties defaultInstance() {
        Map<String, String> map = new HashMap<>(9);
        map.put(QUERY_LOG_CONFIG_FILE_LOCATION, DEFAULT_VALUE_QUERY_LOG_CONFIG_FILE_LOCATION);
        map.put(QUERY_LOG_LOG_SPLIT_COMPLETED, String.valueOf(DEFAULT_VALUE_QUERY_LOG_LOG_SPLIT_COMPLETED));
        map.put(QUERY_LOG_LOG_QUERY_CREATED, String.valueOf(DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED));
        map.put(QUERY_LOG_LOG_QUERY_CREATED_QUERY_TYPE_PATTERN, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED_QUERY_TYPE_PATTERN);
        map.put(QUERY_LOG_LOG_QUERY_CREATED_QUERY_LENGTH, String.valueOf(DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_CREATED_QUERY_LENGTH));
        map.put(QUERY_LOG_LOG_QUERY_COMPLETED, String.valueOf(DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED));
        map.put(QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_TYPE_PATTERN, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_TYPE_PATTERN);
        map.put(QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_LENGTH, String.valueOf(DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_QUERY_LENGTH));
        map.put(QUERY_LOG_LOG_QUERY_COMPLETED_CATALOG_PATTERN, DEFAULT_VALUE_QUERY_LOG_LOG_QUERY_COMPLETED_CATALOG_PATTERN);
        return new QueryLogListenerProperties(map);
    }

    /**
     * Get {@code boolean} parameter value, or return default.
     *
     * @param params       Map of parameters
     * @param paramName    Parameter name
     * @param paramDefault Parameter default value
     * @return Parameter value or default.
     */
    private boolean getBooleanConfig(Map<String, String> params, String paramName, boolean paramDefault) {
        String value = params.get(paramName);
        if (value != null && !value.trim().isEmpty()) {
            return Boolean.parseBoolean(value);
        }
        return paramDefault;
    }

    /**
     * Get {@code int} parameter value, or return default.
     *
     * @param params       Map of parameters
     * @param paramName    Parameter name
     * @param paramDefault Parameter default value
     * @return Parameter value or default.
     */
    private int getIntegerConfig(Map<String, String> params, String paramName, int paramDefault) {
        String value = params.get(paramName);
        if (value != null && !value.trim().isEmpty()) {
            return Integer.parseInt(value);
        }
        return paramDefault;
    }

    /**
     * Get {@code String} parameter value, or return default.
     *
     * @param params       Map of parameters
     * @param paramName    Parameter name
     * @param paramDefault Parameter default value
     * @return Parameter value or default.
     */
    private String getStringConfig(Map<String, String> params, String paramName, String paramDefault) {
        String value = params.get(paramName);
        if (value != null && !value.trim().isEmpty()) {
            return value;
        }
        return paramDefault;
    }

    public String getConfigFileLocation() {
        return configFileLocation;
    }

    public QueryLogListenerProperties setConfigFileLocation(String configFileLocation) {
        this.configFileLocation = configFileLocation;
        return this;
    }

    public boolean isSplitCompleted() {
        return splitCompleted;
    }

    public QueryLogListenerProperties setSplitCompleted(boolean splitCompleted) {
        this.splitCompleted = splitCompleted;
        return this;
    }

    public boolean isQueryCreated() {
        return queryCreated;
    }

    public QueryLogListenerProperties setQueryCreated(boolean queryCreated) {
        this.queryCreated = queryCreated;
        return this;
    }

    public String getQueryCreatedQueryTypePattern() {
        return queryCreatedQueryTypePattern;
    }

    public QueryLogListenerProperties setQueryCreatedQueryTypePattern(String queryCreatedQueryTypePattern) {
        this.queryCreatedQueryTypePattern = queryCreatedQueryTypePattern;
        return this;
    }

    public int getQueryCreatedQueryMaxLength() {
        return queryCreatedQueryMaxLength;
    }

    public QueryLogListenerProperties setQueryCreatedQueryMaxLength(int queryCreatedQueryMaxLength) {
        this.queryCreatedQueryMaxLength = queryCreatedQueryMaxLength;
        return this;
    }

    public boolean isQueryCompleted() {
        return queryCompleted;
    }

    public QueryLogListenerProperties setQueryCompleted(boolean queryCompleted) {
        this.queryCompleted = queryCompleted;
        return this;
    }

    public String getQueryCompletedQueryTypePattern() {
        return queryCompletedQueryTypePattern;
    }

    public QueryLogListenerProperties setQueryCompletedQueryTypePattern(String queryCompletedQueryTypePattern) {
        this.queryCompletedQueryTypePattern = queryCompletedQueryTypePattern;
        return this;
    }

    public int getQueryCompletedQueryMaxLength() {
        return queryCompletedQueryMaxLength;
    }

    public QueryLogListenerProperties setQueryCompletedQueryMaxLength(int queryCompletedQueryMaxLength) {
        this.queryCompletedQueryMaxLength = queryCompletedQueryMaxLength;
        return this;
    }

    public String getQueryCompletedCatalogPattern() {
        return queryCompletedCatalogPattern;
    }

    public QueryLogListenerProperties setQueryCompletedCatalogPattern(String queryCompletedCatalogPattern) {
        this.queryCompletedCatalogPattern = queryCompletedCatalogPattern;
        return this;
    }

    @Override
    public String toString() {
        return "QueryLogListenerProperties{" + "configFileLocation='" + configFileLocation + '\'' + ", splitCompleted=" + splitCompleted + ", queryCreated=" + queryCreated
            + ", queryCreatedQueryTypePattern='" + queryCreatedQueryTypePattern + '\'' + ", queryCreatedQueryMaxLength=" + queryCreatedQueryMaxLength + ", queryCompleted=" + queryCompleted
            + ", queryCompletedQueryTypePattern='" + queryCompletedQueryTypePattern + '\'' + ", queryCompletedQueryMaxLength=" + queryCompletedQueryMaxLength + ", queryCompletedCatalogPattern='"
            + queryCompletedCatalogPattern + '\'' + '}';
    }
}
