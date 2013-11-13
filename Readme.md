
# Install

> $ES_HOME/bin/plugin --install rollindex --url https://github.com/razvan/elasticsearch-rollindex/releases/download/0.90.6-SNAPSHOT/rollindex-0.90.6-SNAPSHOT.zip

you should see 'loaded [rollindex], sites []' in the logs

# Deinstallation

> $ES_HOME/bin/plugin --remove rollindex

# Usage

The following command:
> curl -XPUT 'http://localhost:9200/_rollindex?indexPrefix=test&searchIndices=2&rollIndices=3'

Creates a new index with 3 aliases: 
 * 'test_feed' points to the latest index and acts as a feeding alias
 * 'test_search' which spans over the last 2 indices
 * 'test_roll' which spans over the last 3 indices, all older indices will be closed

This:
> curl -XPUT 'http://localhost:9200/_rollindex?indexPrefix=test&searchIndices=2&rollIndices=3&cronSchedule=0+0%2F2+%2A+%2A+%2A+%3F'

Does the same as the previous command and in addition it schedules a rolling operation to run roughly every two minutes.

# TODO

 * add possibility to list and delete scheduled rolling indexes.
 * make schedules persistent, resumable after node/cluster restart

