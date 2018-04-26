# ASP.NET Integration

## Overview

Get metrics from ASP.NET service in real time to:

* Visualize and monitor ASP.NET states
* Be notified about ASP.NET failovers and events.

## Installation

The ASP.NET check is packaged with the Agent, so simply [install the Agent](https://app.datadoghq.com/account/settings#agent) on your servers.

## Configuration

Edit the `aspdotnet.d/conf.yaml` file, in the `conf.d/` folder at the root of your Agent's directory, to start collecting your ASP.NET metrics.  

See the [sample aspdotnet.d/conf.yaml](https://github.com/DataDog/integrations-core/blob/master/aspdotnet/conf.yaml.example) for all available configuration options.

## Validation

[Run the Agent's `status` subcommand](https://docs.datadoghq.com/agent/faq/agent-commands/#agent-status-and-information) and look for `aspdotnet` under the Checks section.
