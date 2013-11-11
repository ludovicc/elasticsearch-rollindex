package com.pannous.es.rollindex;

import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.io.IOException;
import java.util.*;

public class IndexRoller {

    private static final ESLogger logger = Loggers.getLogger(RollingIndexPlugin.class);
    private static final String feedEnd = "feed";
    private static final String searchEnd = "search";
    private static final String rollEnd = "roll";

    private final Client client;

    public IndexRoller(Client client) {
        this.client = client;
    }

    public DateTimeFormatter createFormatter() {
        return createFormatter(null);
    }

    public DateTimeFormatter createFormatter(String pattern) {
        return DateTimeFormat.forPattern(pattern == null ? "yyyy-MM-dd-HH-mm" : pattern);
    }
    // TODO make client calls async, see RestCreateIndexAction
    Map<String, Object> rollIndex(String indexPrefix, int maxRollIndices,
                                         int maxSearchIndices, boolean deleteAfterRoll, boolean closeAfterRoll,
                                         String indexTimestampPattern, CreateIndexRequest request) {
        String rollAlias = getRoll(indexPrefix);
        DateTimeFormatter formatter = createFormatter(indexTimestampPattern);
        if (maxRollIndices < 1 || maxSearchIndices < 1)
            throw new RuntimeException("remaining indices, search indices and feeding indices must be at least 1");
        if (maxSearchIndices > maxRollIndices)
            throw new RuntimeException("rollIndices must be higher or equal to searchIndices");

        // get old aliases
        Map<String, AliasMetaData> allRollingAliases = getAliases(rollAlias);

        // always create new index and append aliases
        String searchAlias = getSearch(indexPrefix);
        String feedAlias = getFeed(indexPrefix);
        String newIndexName = indexPrefix + "_" + formatter.print(System.currentTimeMillis());

        client.admin().indices().create(request.index(newIndexName)).actionGet();
        addAlias(newIndexName, searchAlias);
        addAlias(newIndexName, rollAlias);

        String deletedIndices = "";
        String removedAlias = "";
        String closedIndices = "";
        String oldFeedIndexName = null;
        if (allRollingAliases.isEmpty()) {
            // do nothing for now
        } else {
            // latest indices comes first
            TreeMap<Long, String> sortedIndices = new TreeMap<Long, String>(reverseSorter);
            // Map<String, String> indexToConcrete = new HashMap<String, String>();
            String[] concreteIndices = getConcreteIndices(allRollingAliases.keySet());
            Arrays.sort(concreteIndices);
            logger.info("aliases:{}, indices:{}", allRollingAliases, Arrays.toString(concreteIndices));
            // if we cannot parse the time from the index name we just treat them as old indices of time == 0
            long timeFake = 0;
            for (String index : concreteIndices) {
                long timeLong = timeFake++;
                int pos = index.indexOf("_");
                if (pos >= 0) {
                    String indexDateStr = index.substring(pos + 1);
                    try {
                        timeLong = formatter.parseMillis(indexDateStr);
                    } catch (Exception ex) {
                        logger.warn("index " + index + " is not in the format " + formatter + " error:" + ex.getMessage());
                    }
                } else
                    logger.warn("index " + index + " is not in the format " + formatter);

                String old = sortedIndices.put(timeLong, index);
                if (old != null)
                    throw new IllegalStateException("Indices with the identical date are not supported! " + old + " vs. " + index);
            }
            int counter = 1;
            Iterator<String> indexIter = sortedIndices.values().iterator();
            while (indexIter.hasNext()) {
                String currentIndexName = indexIter.next();
                if (counter >= maxRollIndices) {
                    if (deleteAfterRoll) {
                        deleteIndex(currentIndexName);
                        deletedIndices += currentIndexName + " ";
                    } else {
                        removeAlias(currentIndexName, feedAlias);
                        removeAlias(currentIndexName, rollAlias);
                        removeAlias(currentIndexName, searchAlias);

                        if (closeAfterRoll) {
                            closeIndex(currentIndexName);
                            closedIndices += currentIndexName + " ";
                        } else {
                            addAlias(currentIndexName, indexPrefix + "_closed");
                        }
                        removedAlias += currentIndexName + " ";
                        removedAlias += currentIndexName + " ";
                    }
                    // close/delete all the older indices
                    continue;
                }

                if (counter == 1)
                    oldFeedIndexName = currentIndexName;

                if (counter >= maxSearchIndices) {
                    removeAlias(currentIndexName, searchAlias);
                    removedAlias += currentIndexName + " ";
                }

                counter++;
            }
        }
        if (oldFeedIndexName != null)
            moveAlias(oldFeedIndexName, newIndexName, feedAlias);
        else
            addAlias(newIndexName, feedAlias);

        Map<String, Object> map = new HashMap<String, Object>();
        map.put("created", newIndexName);
        map.put("deleted", deletedIndices.trim());
        map.put("closed", closedIndices.trim());
        map.put("removedAlias", removedAlias.trim());
        return map;
    }

    public void deleteIndex(String indexName) {
        client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
    }

    public void closeIndex(String indexName) {
        client.admin().indices().close(new CloseIndexRequest(indexName)).actionGet();
    }

    public void addAlias(String indexName, String alias) {
        client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(indexName, alias)).actionGet();
    }

    public void removeAlias(String indexName, String alias) {
        client.admin().indices().aliases(new IndicesAliasesRequest().removeAlias(indexName, alias)).actionGet();
    }

    public void moveAlias(String oldIndexName, String newIndexName, String alias) {
        IndicesAliasesResponse r = client.admin().indices().aliases(new IndicesAliasesRequest().addAlias(newIndexName, alias).
            removeAlias(oldIndexName, alias)).actionGet();
        logger.info("({}) moved {} from {} to {} ", r.isAcknowledged(), alias, oldIndexName, newIndexName);
    }

    public Map<String, AliasMetaData> getAliases(String alias) {
        Map<String, AliasMetaData> md = client.admin().cluster().state(new ClusterStateRequest()).
            actionGet().getState().getMetaData().aliases().get(alias);
        if (md == null)
            return Collections.emptyMap();

        return md;
    }
    private static Comparator<Long> reverseSorter = new Comparator<Long>() {
        @Override
        public int compare(Long o1, Long o2) {
            return -o1.compareTo(o2);
        }
    };

    public String[] getConcreteIndices(Set<String> set) {
        return client.admin().cluster().state(new ClusterStateRequest()).actionGet().getState().
            getMetaData().concreteIndices(set.toArray(new String[set.size()]));
    }

    String getRoll(String indexName) {
        return indexName + "_" + rollEnd;
    }

    String getFeed(String indexName) {
        if (feedEnd.isEmpty())
            return indexName;
        return indexName + "_" + feedEnd;
    }

    String getSearch(String indexName) {
        if (searchEnd.isEmpty())
            return indexName;
        return indexName + "_" + searchEnd;
    }

    public Map<String, Object> rollIndex(RollRequest rollRequest) throws IOException {

        return rollIndex(
            rollRequest.getIndexPrefix(),
            rollRequest.getRollIndices(),
            rollRequest.getSearchIndices(),
            rollRequest.getDeleteAfterRoll(),
            rollRequest.getCloseAfterRoll(),
            rollRequest.getIndexTimestampPattern(),
            rollRequest.getCreateIndexRequest());
    }
}
