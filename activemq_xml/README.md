# Activemq_xml Integration

## Overview

Get metrics from activemq_xml service in real time to:

* Visualize and monitor activemq_xml states
* Be notified about activemq_xml failovers and events.

## Setup
### Installation

The Activemq XML check is packaged with the Agent, so simply [install the Agent](https://app.datadoghq.com/account/settings#agent) on your servers.

### Configuration

Edit the `activemq_xml.d/conf.yaml` file, in the `conf.d/` folder at the root of your Agent's directory, to start collecting your Agent metrics.  

See the [sample activemq_xml.d/conf.yaml](https://github.com/DataDog/integrations-core/blob/master/activemq_xml/conf.yaml.example) for all available configuration options.

### Validation

[Run the Agent's `status` subcommand](https://docs.datadoghq.com/agent/faq/agent-commands/#agent-status-and-information) and look for `activemq_xml` under the Checks section.

## Data Collected
### Metrics
See [metadata.csv](https://github.com/DataDog/integrations-core/blob/master/activemq_xml/metadata.csv) for a list of metrics provided by this integration.

### Events
The Activemq_xml check does not include any event at this time.

### Service Checks
The Activemq_xml check does not include any service check at this time.

## Troubleshooting
Need help? Contact [Datadog Support](http://docs.datadoghq.com/help/).

## Further Reading

* [Monitor ActiveMQ metrics and performance](https://www.datadoghq.com/blog/monitor-activemq-metrics-performance/)
