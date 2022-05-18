package com.github.archongum.trino.query.log;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryMetadata;
import io.trino.spi.eventlistener.QueryStatistics;


/**
 * @author Archon  2019/10/29
 * @since 0.3
 */
public class CustomQueryCompletedEvent {

    private static final long MB_BYTES = 1_048_576;

    private Metadata metadata;

    private Statistics statistics;

    private QueryContext context;

    private List<InputMetaData> inputMetaDataList;

    private Instant createTime;
    private Instant startTime;
    private Instant endTime;

    public static CustomQueryCompletedEvent of(QueryCompletedEvent event) {
        return new CustomQueryCompletedEvent(event);
    }

    private CustomQueryCompletedEvent(QueryCompletedEvent event) {
        Metadata metadata = new Metadata();
        QueryMetadata _m = event.getMetadata();
        metadata.setQueryId(_m.getQueryId());
        metadata.setTransactionId(_m.getTransactionId().orElse(null));
        metadata.setQuery(_m.getQuery());
        metadata.setPreparedQuery(_m.getPreparedQuery().orElse(null));
        metadata.setQueryState(_m.getQueryState());
        metadata.setUri(_m.getUri());
        this.setMetadata(metadata);

        Statistics statistics = new Statistics();
        QueryStatistics _s = event.getStatistics();
        statistics.setCpuSecond(_s.getCpuTime().getSeconds());
        statistics.setFailedCpuSecond(_s.getFailedCpuTime().getSeconds());
        statistics.setWallSecond(_s.getWallTime().getSeconds());
        statistics.setQueuedSecond(_s.getQueuedTime().getSeconds());
        statistics.setScheduledSecond(_s.getScheduledTime().orElse(Duration.ZERO).getSeconds());
        statistics.setFailedScheduledSecond(_s.getFailedScheduledTime().orElse(Duration.ZERO).getSeconds());
        statistics.setAnalysisSecond(_s.getAnalysisTime().orElse(Duration.ZERO).getSeconds());
        statistics.setPlanningSecond(_s.getPlanningTime().orElse(Duration.ZERO).getSeconds());
        statistics.setExecutionSecond(_s.getExecutionTime().orElse(Duration.ZERO).getSeconds());
        statistics.setInputBlockedSecond(_s.getInputBlockedTime().orElse(Duration.ZERO).getSeconds());
        statistics.setFailedInputBlockedSecond(_s.getFailedInputBlockedTime().orElse(Duration.ZERO).getSeconds());
        statistics.setOutputBlockedSecond(_s.getOutputBlockedTime().orElse(Duration.ZERO).getSeconds());
        statistics.setFailedOutputBlockedSecond(_s.getFailedOutputBlockedTime().orElse(Duration.ZERO).getSeconds());
        statistics.setPeakUserMemoryMB(_s.getPeakUserMemoryBytes()/MB_BYTES);
        statistics.setPeakTaskUserMemoryMB(_s.getPeakTaskUserMemory()/MB_BYTES);
        statistics.setPeakTaskTotalMemoryMB(_s.getPeakTaskTotalMemory()/MB_BYTES);
        statistics.setPhysicalInputMB(_s.getPhysicalInputBytes()/MB_BYTES);
        statistics.setPhysicalInputRows(_s.getPhysicalInputRows());
        statistics.setProcessedInputMB(_s.getProcessedInputBytes()/MB_BYTES);
        statistics.setProcessedInputRows(_s.getProcessedInputRows());
        statistics.setInternalNetworkMB(_s.getInternalNetworkBytes()/MB_BYTES);
        statistics.setInternalNetworkRows(_s.getInternalNetworkRows());
        statistics.setTotalMB(_s.getTotalBytes()/MB_BYTES);
        statistics.setTotalRows(_s.getTotalRows());
        statistics.setOutputMB(_s.getOutputBytes()/MB_BYTES);
        statistics.setOutputRows(_s.getOutputRows());
        statistics.setWrittenMB(_s.getWrittenBytes()/MB_BYTES);
        statistics.setWrittenRows(_s.getWrittenRows());
        statistics.setCumulativeMemoryMB(_s.getCumulativeMemory()/MB_BYTES);
        statistics.setFailedCumulativeMemoryMB(_s.getFailedCumulativeMemory()/MB_BYTES);
        statistics.setCompletedSplits(_s.getCompletedSplits());
        statistics.setComplete(_s.isComplete());
        statistics.setResourceWaitingSecond(_s.getResourceWaitingTime().orElse(Duration.ZERO).getSeconds());
        this.setStatistics(statistics);

        this.setContext(event.getContext());

        List<InputMetaData> inputMetaDataList = new ArrayList<>(event.getIoMetadata().getInputs().size());
        event.getIoMetadata().getInputs().forEach(i -> {
            InputMetaData inputMetaData = new InputMetaData();
            inputMetaData.setCatalogName(i.getCatalogName());
            inputMetaData.setSchema(i.getSchema());
            inputMetaData.setTable(i.getTable());
            inputMetaData.setConnectorInfo(i.getConnectorInfo());
            inputMetaData.setPhysicalInputMB(i.getPhysicalInputBytes().orElse(0)/MB_BYTES);
            inputMetaData.setPhysicalInputRows(i.getPhysicalInputRows().orElse(0));
            inputMetaDataList.add(inputMetaData);
        });
        this.setInputMetaDataList(inputMetaDataList);

        this.setCreateTime(event.getCreateTime());
        this.setStartTime(event.getExecutionStartTime());
        this.setEndTime(event.getEndTime());
    }

    public Metadata getMetadata() {
        return metadata;
    }

    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public void setStatistics(Statistics statistics) {
        this.statistics = statistics;
    }

    public QueryContext getContext() {
        return context;
    }

    public void setContext(QueryContext context) {
        this.context = context;
    }

    public List<InputMetaData> getInputMetaDataList() {
        return inputMetaDataList;
    }

    public void setInputMetaDataList(List<InputMetaData> inputMetaDataList) {
        this.inputMetaDataList = inputMetaDataList;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Instant createTime) {
        this.createTime = createTime;
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(Instant startTime) {
        this.startTime = startTime;
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(Instant endTime) {
        this.endTime = endTime;
    }

    static class Metadata {
        private String queryId;
        private String transactionId;
        private String query;
        private String preparedQuery;
        private String queryState;
        private URI uri;

        public String getQueryId() {
            return queryId;
        }

        public void setQueryId(String queryId) {
            this.queryId = queryId;
        }

        public String getTransactionId() {
            return transactionId;
        }

        public void setTransactionId(String transactionId) {
            this.transactionId = transactionId;
        }

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getPreparedQuery() {
            return preparedQuery;
        }

        public void setPreparedQuery(String preparedQuery) {
            this.preparedQuery = preparedQuery;
        }

        public String getQueryState() {
            return queryState;
        }

        public void setQueryState(String queryState) {
            this.queryState = queryState;
        }

        public URI getUri() {
            return uri;
        }

        public void setUri(URI uri) {
            this.uri = uri;
        }
    }

    static class Statistics {
        private long cpuSecond;
        private long failedCpuSecond;
        private long wallSecond;
        private long queuedSecond;
        private long scheduledSecond;
        private long failedScheduledSecond;
        private long analysisSecond;
        private long planningSecond;
        private long executionSecond;
        private long inputBlockedSecond;
        private long failedInputBlockedSecond;
        private long outputBlockedSecond;
        private long failedOutputBlockedSecond;
        private long peakUserMemoryMB;
        private long peakTaskUserMemoryMB;
        private long peakTaskTotalMemoryMB;
        private long physicalInputMB;
        private long physicalInputRows;
        private long processedInputMB;
        private long processedInputRows;
        private long internalNetworkMB;
        private long internalNetworkRows;
        private long totalMB;
        private long totalRows;
        private long outputMB;
        private long outputRows;
        private long writtenMB;
        private long writtenRows;
        private double cumulativeMemoryMB;
        private double failedCumulativeMemoryMB;
        private long completedSplits;
        private boolean complete;
        private long resourceWaitingSecond;

        public long getCpuSecond() {
            return cpuSecond;
        }

        public void setCpuSecond(long cpuSecond) {
            this.cpuSecond = cpuSecond;
        }

        public long getFailedCpuSecond() {
            return failedCpuSecond;
        }

        public void setFailedCpuSecond(long failedCpuSecond) {
            this.failedCpuSecond = failedCpuSecond;
        }

        public long getWallSecond() {
            return wallSecond;
        }

        public void setWallSecond(long wallSecond) {
            this.wallSecond = wallSecond;
        }

        public long getQueuedSecond() {
            return queuedSecond;
        }

        public void setQueuedSecond(long queuedSecond) {
            this.queuedSecond = queuedSecond;
        }

        public long getScheduledSecond() {
            return scheduledSecond;
        }

        public void setScheduledSecond(long scheduledSecond) {
            this.scheduledSecond = scheduledSecond;
        }

        public long getFailedScheduledSecond() {
            return failedScheduledSecond;
        }

        public void setFailedScheduledSecond(long failedScheduledSecond) {
            this.failedScheduledSecond = failedScheduledSecond;
        }

        public long getAnalysisSecond() {
            return analysisSecond;
        }

        public void setAnalysisSecond(long analysisSecond) {
            this.analysisSecond = analysisSecond;
        }

        public long getPlanningSecond() {
            return planningSecond;
        }

        public void setPlanningSecond(long planningSecond) {
            this.planningSecond = planningSecond;
        }

        public long getExecutionSecond() {
            return executionSecond;
        }

        public void setExecutionSecond(long executionSecond) {
            this.executionSecond = executionSecond;
        }

        public long getInputBlockedSecond() {
            return inputBlockedSecond;
        }

        public void setInputBlockedSecond(long inputBlockedSecond) {
            this.inputBlockedSecond = inputBlockedSecond;
        }

        public long getFailedInputBlockedSecond() {
            return failedInputBlockedSecond;
        }

        public void setFailedInputBlockedSecond(long failedInputBlockedSecond) {
            this.failedInputBlockedSecond = failedInputBlockedSecond;
        }

        public long getOutputBlockedSecond() {
            return outputBlockedSecond;
        }

        public void setOutputBlockedSecond(long outputBlockedSecond) {
            this.outputBlockedSecond = outputBlockedSecond;
        }

        public long getFailedOutputBlockedSecond() {
            return failedOutputBlockedSecond;
        }

        public void setFailedOutputBlockedSecond(long failedOutputBlockedSecond) {
            this.failedOutputBlockedSecond = failedOutputBlockedSecond;
        }

        public long getPeakUserMemoryMB() {
            return peakUserMemoryMB;
        }

        public void setPeakUserMemoryMB(long peakUserMemoryMB) {
            this.peakUserMemoryMB = peakUserMemoryMB;
        }

        public long getPeakTaskUserMemoryMB() {
            return peakTaskUserMemoryMB;
        }

        public void setPeakTaskUserMemoryMB(long peakTaskUserMemoryMB) {
            this.peakTaskUserMemoryMB = peakTaskUserMemoryMB;
        }

        public long getPeakTaskTotalMemoryMB() {
            return peakTaskTotalMemoryMB;
        }

        public void setPeakTaskTotalMemoryMB(long peakTaskTotalMemoryMB) {
            this.peakTaskTotalMemoryMB = peakTaskTotalMemoryMB;
        }

        public long getPhysicalInputMB() {
            return physicalInputMB;
        }

        public void setPhysicalInputMB(long physicalInputMB) {
            this.physicalInputMB = physicalInputMB;
        }

        public long getPhysicalInputRows() {
            return physicalInputRows;
        }

        public void setPhysicalInputRows(long physicalInputRows) {
            this.physicalInputRows = physicalInputRows;
        }

        public long getProcessedInputMB() {
            return processedInputMB;
        }

        public void setProcessedInputMB(long processedInputMB) {
            this.processedInputMB = processedInputMB;
        }

        public long getProcessedInputRows() {
            return processedInputRows;
        }

        public void setProcessedInputRows(long processedInputRows) {
            this.processedInputRows = processedInputRows;
        }

        public long getInternalNetworkMB() {
            return internalNetworkMB;
        }

        public void setInternalNetworkMB(long internalNetworkMB) {
            this.internalNetworkMB = internalNetworkMB;
        }

        public long getInternalNetworkRows() {
            return internalNetworkRows;
        }

        public void setInternalNetworkRows(long internalNetworkRows) {
            this.internalNetworkRows = internalNetworkRows;
        }

        public long getTotalMB() {
            return totalMB;
        }

        public void setTotalMB(long totalMB) {
            this.totalMB = totalMB;
        }

        public long getTotalRows() {
            return totalRows;
        }

        public void setTotalRows(long totalRows) {
            this.totalRows = totalRows;
        }

        public long getOutputMB() {
            return outputMB;
        }

        public void setOutputMB(long outputMB) {
            this.outputMB = outputMB;
        }

        public long getOutputRows() {
            return outputRows;
        }

        public void setOutputRows(long outputRows) {
            this.outputRows = outputRows;
        }

        public long getWrittenMB() {
            return writtenMB;
        }

        public void setWrittenMB(long writtenMB) {
            this.writtenMB = writtenMB;
        }

        public long getWrittenRows() {
            return writtenRows;
        }

        public void setWrittenRows(long writtenRows) {
            this.writtenRows = writtenRows;
        }

        public double getCumulativeMemoryMB() {
            return cumulativeMemoryMB;
        }

        public void setCumulativeMemoryMB(double cumulativeMemoryMB) {
            this.cumulativeMemoryMB = cumulativeMemoryMB;
        }

        public double getFailedCumulativeMemoryMB() {
            return failedCumulativeMemoryMB;
        }

        public void setFailedCumulativeMemoryMB(double failedCumulativeMemoryMB) {
            this.failedCumulativeMemoryMB = failedCumulativeMemoryMB;
        }

        public long getCompletedSplits() {
            return completedSplits;
        }

        public void setCompletedSplits(long completedSplits) {
            this.completedSplits = completedSplits;
        }

        public boolean isComplete() {
            return complete;
        }

        public void setComplete(boolean complete) {
            this.complete = complete;
        }

        public long getResourceWaitingSecond() {
            return resourceWaitingSecond;
        }

        public void setResourceWaitingSecond(long resourceWaitingSecond) {
            this.resourceWaitingSecond = resourceWaitingSecond;
        }
    }

    static class InputMetaData {
        private String catalogName;
        private String schema;
        private String table;
        private Optional<Object> connectorInfo;
        private long physicalInputMB;
        private long physicalInputRows;

        public String getCatalogName() {
            return catalogName;
        }

        public void setCatalogName(String catalogName) {
            this.catalogName = catalogName;
        }

        public String getSchema() {
            return schema;
        }

        public void setSchema(String schema) {
            this.schema = schema;
        }

        public String getTable() {
            return table;
        }

        public void setTable(String table) {
            this.table = table;
        }

        public Optional<Object> getConnectorInfo() {
            return connectorInfo;
        }

        public void setConnectorInfo(Optional<Object> connectorInfo) {
            this.connectorInfo = connectorInfo;
        }

        public long getPhysicalInputMB() {
            return physicalInputMB;
        }

        public void setPhysicalInputMB(long physicalInputMB) {
            this.physicalInputMB = physicalInputMB;
        }

        public long getPhysicalInputRows() {
            return physicalInputRows;
        }

        public void setPhysicalInputRows(long physicalInputRows) {
            this.physicalInputRows = physicalInputRows;
        }
    }
}
