Elasticsearch Graphite Plugin [![Build Status][travis-image]][travis-url]
=============================

This plugin creates a little push service, which regularly updates a graphite host with indices stats and nodes stats. Only primary master node reports about individual indices.

The data sent to the graphite server tries to be roughly equivalent to [Indices Stats API](http://www.elasticsearch.org/guide/reference/api/admin-indices-stats.html) and [Nodes Stats Api](http://www.elasticsearch.org/guide/reference/api/admin-cluster-nodes-stats.html).


Install
-------
```
elasticsearch/bin/plugin --install com.pakulov.elasticsearch/elasticsearch-plugin-graphite/0.4.0
```

Usage
-----
Plugin has a set of possible parameters:

* `metrics.graphite.host`: The graphite host to connect to (default: none)
* `metrics.graphite.port`: The port to connect to (default: 2003)
* `metrics.graphite.every`: The interval to push data (default: 1m)
* `metrics.graphite.prefix`: The metric prefix that's sent with metric names (default: elasticsearch.your_cluster_name)
* `metrics.graphite.exclude`: A regular expression allowing you to exclude certain metrics (note that the node does not start if the regex does not compile)
* `metrics.graphite.include`: A regular expression to explicitely include certain metrics even though they matched the exclude (note that the node does not start if the regex does not compile)

Check your elasticsearch log file for a line like this after adding the configuration parameters below to the configuration file

```
[2013-02-08 16:01:49,153][INFO ][service.graphite         ] [Sea Urchin] Graphite reporting triggered every [1m] to host [graphite.example.com:2003]
```

TODO-List
---------
* In case of a master node failover, counts are starting from 0 again (in case you are wondering about spikes)


Credits
-------
Original plugin has been written by [Alexander Reelsen](https://github.com/spinscale), which was heavily inspired by the excellent [metrics library](http://metrics.codahale.com) by Coda Hale and its [GraphiteReporter add-on](http://metrics.codahale.com/manual/graphite/).

License
-------
    Copyright 2015 Alexander Pakulov

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.

[travis-url]: https://travis-ci.org/apakulov/elasticsearch-graphite-plugin
[travis-image]: https://travis-ci.org/apakulov/elasticsearch-graphite-plugin.svg?branch=master