# active_directory Integration

## Overview

Get metrics from Microsoft Active Directory

* Visualize and monitor Active Directory performance

## Setup
### Installation

The Agent's Active directory check is packaged with the Agent, so simply [install the Agent](https://app.datadoghq.com/account/settings#agent) on your server.

### Configuration

Edit the `active_directory.d/conf.yaml` file, in the `conf.d/` folder at the root of your Agent's directory, to start collecting your Active Directory performance data.  

See the [sample active_directory.d/conf.yaml](https://github.com/DataDog/integrations-core/blob/master/active_directory/conf.yaml.example) for all available configuration options.

### Validation

[Run the Agent's `info` subcommand](https://help.datadoghq.com/hc/en-us/articles/203764635-Agent-Status-and-Information) and look for `active_directory` under the Checks section.

## Data Collected
### Metrics
See [metadata.csv](https://github.com/DataDog/integrations-core/blob/master/active_directory/metadata.csv) for a list of metrics provided by this integration.

### Events
The active directory check does not include any event at this time.

### Service Checks
The active directory check does not include any service check at this time.
