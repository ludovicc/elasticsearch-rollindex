
# Install

> $ES_HOME/bin/plugin --install rollindex --url file:/path/to/download/rollindex-<version>.zip

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

Call the command several times and you get a feeling of what it does.
Then create a cron job which calls it with the periodic you like - you can use the roll.sh script for that.
But of course you can even
call it manually in aperiodic cycles.

To change the index creation settings just specify them in the request body. For other settings have a look in the source.

# FAQ

 * Why do I'm getting IndexAlreadyExistsException? You roll too often, reduce to per minute at maximum. 
   Or change the pattern to include the seconds.
 * Q: In your readme file it says you have 3 versions of a particular index, one that corresponds to search (servicing reads), the other that fills in the latest data (servicing writes) and the other called roll.
   A: Not really versions. that are simple aliases.
 * Q: I was wondering what happens under the covers when its time to roll an index, do the search related indices get refreshed with data from the roll indices?  I will start reading the rollAction code but a deeper explanation would be much appreciated
   A: when it is time to roll then all indices with a 'roll' alias are sorted by name and time (time is calculated from index name). Then a new index is created and the 'feed' alias is moved (atomic operation) from the current to the newly created one, so all writes will go into the new index.
      Also a 'search' alias is added to the new index to include it for searching. Keep in mind that if you want to update document, then you'll have to use the same concrete index to do so. Just searching (via search alias) and then feeding (into feed alias) can result in a duplicate entry (if the old document is in an older index and the new doc is written to the latest index).

# TODO

 * add possibility to list and delete scheduled rolling indexes.

