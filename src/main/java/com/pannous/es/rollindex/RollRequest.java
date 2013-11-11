package com.pannous.es.rollindex;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;

public class RollRequest {

    private final String indexPrefix;
    private final int rollIndices;
    private final int searchIndices;
    private final boolean deleteAfterRoll;
    private final boolean closeAfterRoll;
    private final String indexTimestampPattern;
    private final CreateIndexRequest createIndexRequest;
    private final String cronSchedule;

    public RollRequest(String indexPrefix, int rollIndices, int searchIndices, String indexTimestampPattern) {
        this.indexPrefix = indexPrefix;
        this.rollIndices = rollIndices;
        this.searchIndices = searchIndices;
        this.deleteAfterRoll = false;
        this.closeAfterRoll = true;
        this.indexTimestampPattern = indexTimestampPattern;
        this.createIndexRequest = new CreateIndexRequest("").source("");
        this.cronSchedule = "";
    }

    public RollRequest(RestRequest request) throws IOException {
        indexPrefix = request.param("indexPrefix", "");

        if (indexPrefix.isEmpty()) {
            throw new IllegalArgumentException("indexPrefix missing");
        }

        cronSchedule = request.param("cronSchedule", "");

        searchIndices = request.paramAsInt("searchIndices", 1);
        rollIndices = request.paramAsInt("rollIndices", 1);
        deleteAfterRoll = request.paramAsBoolean("deleteAfterRoll", false);
        closeAfterRoll = request.paramAsBoolean("closeAfterRoll", true);
        if (deleteAfterRoll && closeAfterRoll) {
            if (request.hasParam("closeAfterRoll"))
                throw new IllegalArgumentException("Cannot delete and close an index at the same time");
        }

        final int shards = request.paramAsInt("newIndexShards", 2);
        final int replicas = request.paramAsInt("newIndexReplicas", 1);
        final String refresh = request.param("newIndexRefresh", "10s");

        indexTimestampPattern = request.param("indexTimestampPattern");

        if (request.hasContent())
            createIndexRequest = new CreateIndexRequest("").source(request.content().toUtf8());
        else
            createIndexRequest = new CreateIndexRequest("").settings(toSettings(createIndexSettings(shards, replicas, refresh).string()));
    }


    private XContentBuilder createIndexSettings(int shards, int replicas, String refresh) {
        try {
            XContentBuilder createIndexSettings = JsonXContent.contentBuilder().startObject().
                field("index.number_of_shards", shards).
                field("index.number_of_replicas", replicas).
                field("index.refresh_interval", refresh).endObject();
            return createIndexSettings;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }


    Settings toSettings(String str) {
        return ImmutableSettings.settingsBuilder().loadFromSource(str).build();
    }

    public String getIndexPrefix() {
        return indexPrefix;
    }

    public int getRollIndices() {
        return rollIndices;
    }

    public int getSearchIndices() {
        return searchIndices;
    }

    public boolean getDeleteAfterRoll() {
        return deleteAfterRoll;
    }

    public boolean getCloseAfterRoll() {
        return closeAfterRoll;
    }

    public String getIndexTimestampPattern() {
        return indexTimestampPattern;
    }

    public CreateIndexRequest getCreateIndexRequest() {
        return createIndexRequest;
    }

    public String getCronSchedule() {
        return cronSchedule;
    }

    public boolean hasCronSchedule() {
        return cronSchedule != null && !cronSchedule.isEmpty();
    }
}
