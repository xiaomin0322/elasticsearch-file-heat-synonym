File watcher synonym for ElasticSearch
======================================

The file watcher synonym plugin adds a synonym token filter that reloads the synonym file at given intervals (default 60s).

Example:

	{
	    "index" : {
	        "analysis" : {
	            "analyzer" : {
	                "synonym" : {
	                    "tokenizer" : "whitespace",
	                    "filter" : ["synonym"]
 	               }
	            },
	            "filter" : {
	                "synonym" : {
	                    "type" : "file_heat_synonym",
	                    "synonyms_path" : "analysis/synonym.txt"
	                    "interval" : "10"
	                }
	            }
	        }
	    }
	}

## Installation

Using the plugin command (inside your elasticsearch/bin directory) the plugin can be installed by:
```
bin/plugin -install analysis-heat-watcher-synonym  -url https://github.com/lindstromhenrik/elasticsearch-analysis-file-watcher-synonym/releases/download/v0.90.9-0.1.0/elasticsearch-file-watcher-synonym-0.90.9-0.1.0.zip
```

### Compatibility


**Note**: Please make sure the plugin version matches with your elasticsearch version. Follow this compatibility matrix

    ------------------------------------------------------
    | analysis file heat synonym   | Elasticsearch    |
    ------------------------------------------------------
    | 0.2.0                           | 1.1.0 -> master  |
    ------------------------------------------------------