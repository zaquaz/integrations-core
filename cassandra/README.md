# Cassandra Integration
{{< img src="integrations/cassandra/cassandra.png" alt="Cassandra default dashboard" responsive="true" popup="true">}}
## Overview

Get metrics from Cassandra service in real time to:

* Visualize and monitor Cassandra states
* Be notified about Cassandra failovers and events.

## Setup
### Installation

The Cassandra check is packaged with the Agent, so simply [install the Agent](https://app.datadoghq.com/account/settings#agent) on your Cassandra nodes.

We recommend the use of Oracle's JDK for this integration.

This check has a limit of 350 metrics per instance. The number of returned metrics is indicated in the info page. You can specify the metrics you are interested in by editing the configuration below. To learn how to customize the metrics to collect visit the [JMX Checks documentation](https://docs.datadoghq.com/integrations/java/) for more detailed instructions. If you need to monitor more metrics, please send us an email at support@datadoghq.com

### Configuration

Edit the `cassandra.d/conf.yaml` file, in the `conf.d/` folder at the root of your Agent's directory, to start collecting your Cassandra metrics and logs. 

#### Metric Collection

1. The default configuration of your `cassandra.d/conf.yaml` file activate the collection of your [Cassandra metrics](#metrics).  
  See the [sample  cassandra.d/conf.yaml](https://github.com/DataDog/integrations-core/blob/master/cassandra/conf.yaml.example) for all available configuration options.
 
2. [Restart the Agent](https://docs.datadoghq.com/agent/faq/agent-commands/#start-stop-restart-the-agent).

#### Log Collection

**Available for Agent >6.0**

* Collecting logs is disabled by default in the Datadog Agent, enable it in your `datadog.yaml` file:

  ```
  logs_enabled: true
  ```

* Add this configuration setup to your `cassandra.yaml` file to start collecting your Cassandra logs:

  ```
    logs:
        - type: file
          path: /var/log/cassandra/*.log
          source: cassandra
          sourcecategory: database
          service: myapplication
  ```

  Change the `path` and `service` parameter values and configure them for your environment.
See the [sample cassandra.d/conf.yaml](https://github.com/DataDog/integrations-core/blob/master/cassandra/conf.yaml.example) for all available configuration options.
   
* [Restart the Agent](https://docs.datadoghq.com/agent/faq/agent-commands/#start-stop-restart-the-agent).

### Validation

[Run the Agent's `status` subcommand](https://docs.datadoghq.com/agent/faq/agent-commands/#agent-status-and-information) and look for `cassandra` under the Checks section.

## Data Collected
### Metrics
See [metadata.csv](https://github.com/DataDog/integrations-core/blob/master/cassandra/metadata.csv) for a list of metrics provided by this integration.

### Events
The Cassandra check does not include any event at this time.

### Service Checks
**cassandra.can_connect**

Returns `CRITICAL` if the Agent is unable to connect to and collect metrics from the monitored Cassandra instance. Returns `OK` otherwise.

## Troubleshooting
Need help? Contact [Datadog Support](http://docs.datadoghq.com/help/).

## Further Reading

* [How to monitor Cassandra performance metrics](https://www.datadoghq.com/blog/how-to-monitor-cassandra-performance-metrics/)
* [How to collect Cassandra metrics](https://www.datadoghq.com/blog/how-to-collect-cassandra-metrics/)
* [Monitoring Cassandra with Datadog](https://www.datadoghq.com/blog/monitoring-cassandra-with-datadog/)
