package com.pannous.es.rollindex;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;

import static org.elasticsearch.rest.RestRequest.Method.*;
import static org.elasticsearch.rest.RestStatus.*;
import static org.elasticsearch.rest.action.support.RestXContentBuilder.*;

/**
 * @see issue 1500 https://github.com/elasticsearch/elasticsearch/issues/1500
 *
 * Only indices with the rolling alias are involved into rolling.
 * @author Peter Karich
 */
public class RollAction extends BaseRestHandler {

    private final IndexRoller indexRoller;

    @Inject public RollAction(Settings settings, Client client, RestController controller) {
        super(settings, client);

        // Define REST endpoints to do a roll further
        controller.registerHandler(PUT, "/_rollindex", this);
        controller.registerHandler(POST, "/_rollindex", this);

        indexRoller = new IndexRoller(client);
    }

    @Override public void handleRequest(RestRequest request, RestChannel channel) {
        logger.info("RollAction.handleRequest [{}]", request.params());
        try {
            final XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);

            final RollRequest rollRequest = new RollRequest(request);

            final Map<String, Object> rollIndexResult = indexRoller.rollIndex(rollRequest);

            builder.startObject();
            for (Map.Entry<String, Object> e : rollIndexResult.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();

            if (rollRequest.hasCronSchedule())
                RollScheduler.getInstance().scheduleRollingCron(indexRoller, rollRequest);

            channel.sendResponse(new XContentRestResponse(request, OK, builder));
        } catch (IOException ex) {
            try {
                channel.sendResponse(new XContentThrowableRestResponse(request, ex));
            } catch (Exception ex2) {
                logger.error("problem while rolling index", ex2);
            }
        }
    }
}
