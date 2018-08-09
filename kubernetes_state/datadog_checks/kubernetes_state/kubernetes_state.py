# (C) Datadog, Inc. 2016-2017
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

import re
import time
from collections import defaultdict, Counter

from datadog_checks.errors import CheckException
from datadog_checks.checks.openmetrics import OpenMetricsBaseCheck
from datadog_checks.config import is_affirmative


METRIC_TYPES = ['counter', 'gauge']

# As case can vary depending on Kubernetes versions, we match the lowercase string
WHITELISTED_WAITING_REASONS = ['errimagepull', 'imagepullbackoff', 'crashloopbackoff']
WHITELISTED_TERMINATED_REASONS = ['oomkilled', 'containercannotrun', 'error']


class KubernetesState(OpenMetricsBaseCheck):
    """
    Collect kube-state-metrics metrics in the Prometheus format
    See https://github.com/kubernetes/kube-state-metrics
    """

    METRIC_TRANSFORMERS = {}

    def __init__(self, name, init_config, agentConfig, instances=None):
        generic_instances = []
        if instances is not None:
            # We do not support more than one instance of kube-state-metrics
            extra_labels = instances[0].get('label_joins', {})
            hostname_override = is_affirmative(instances[0].get('hostname_override', True))

            for instance in instances:
                kubernetes_state_instance = self._create_kubernetes_state_prometheus_instance(
                        instance,
                        extra_labels,
                        hostname_override
                )
                generic_instances.append(kubernetes_state_instance)

        super(KubernetesState, self).__init__(name, init_config, agentConfig, generic_instances)

        self.pod_phase_to_status = {
            'Pending':   self.WARNING,
            'Running':   self.OK,
            'Succeeded': self.OK,
            'Failed':    self.CRITICAL,
            'Unknown':   self.UNKNOWN
        }

        self.condition_to_status_positive = {
            'true':      self.OK,
            'false':     self.CRITICAL,
            'unknown':   self.UNKNOWN
        }

        self.condition_to_status_negative = {
            'true':      self.CRITICAL,
            'false':     self.OK,
            'unknown':   self.UNKNOWN
        }

        self.METRIC_TRANSFORMERS = {
            'kube_pod_status_phase': self.kube_pod_status_phase,
            'kube_pod_container_status_waiting_reason': self.kube_pod_container_status_waiting_reason,
            'kube_pod_container_status_terminated_reason': self.kube_pod_container_status_terminated_reason,
            'kube_cronjob_next_schedule_time': self.kube_cronjob_next_schedule_time,
            'kube_job_complete': self.kube_job_complete,
            'kube_job_failed': self.kube_job_failed,
            'kube_job_status_failed': self.kube_job_status_failed,
            'kube_job_status_succeeded': self.kube_job_status_succeeded,
            'kube_node_status_condition': self.kube_node_status_condition,
            'kube_node_status_ready': self.kube_node_status_ready,
            'kube_node_status_out_of_disk': self.kube_node_status_out_of_disk,
            'kube_node_status_memory_pressure': self.kube_node_status_memory_pressure,
            'kube_node_status_disk_pressure': self.kube_node_status_disk_pressure,
            'kube_node_status_network_unavailable': self.kube_node_status_network_unavailable,
            'kube_node_spec_unschedulable': self.kube_node_spec_unschedulable,
            'kube_resourcequota': self.kube_resourcequota,
            'kube_limitrange': self.kube_limitrange
        }

    def check(self, instance):
        endpoint = instance.get('kube_state_url')

        # Job counters are monotonic: they increase at every run of the job
        # We want to send the delta via the `monotonic_count` method
        self.job_succeeded_count = defaultdict(int)
        self.job_failed_count = defaultdict(int)

        scraper_config = self.config_map[endpoint]
        self.process(scraper_config, metric_transformers=self.METRIC_TRANSFORMERS)

        for job_tags, job_count in self.job_succeeded_count.iteritems():
            self.monotonic_count(scraper_config['NAMESPACE'] + '.job.succeeded', job_count, list(job_tags))
        for job_tags, job_count in self.job_failed_count.iteritems():
            self.monotonic_count(scraper_config['NAMESPACE'] + '.job.failed', job_count, list(job_tags))

    def _create_kubernetes_state_prometheus_instance(self, instance, extra_labels, hostname_override):
        """
        Set up the kubernetes_state instance so it can be used in OpenMetricsBaseCheck
        """
        endpoint = instance.get('kube_state_url')
        if endpoint is None:
            raise CheckException("Unable to find kube_state_url in config file.")

        instance.update({
            'namespace': 'kubernetes_state',
            'metrics': [{
                'kube_daemonset_status_current_number_scheduled': 'daemonset.scheduled',
                'kube_daemonset_status_desired_number_scheduled': 'daemonset.desired',
                'kube_daemonset_status_number_misscheduled': 'daemonset.misscheduled',
                'kube_daemonset_status_number_ready': 'daemonset.ready',
                'kube_deployment_spec_paused': 'deployment.paused',
                'kube_deployment_spec_replicas': 'deployment.replicas_desired',
                'kube_deployment_spec_strategy_rollingupdate_max_unavailable': 'deployment.rollingupdate.max_unavailable',  # noqa: E501
                'kube_deployment_status_replicas': 'deployment.replicas',
                'kube_deployment_status_replicas_available': 'deployment.replicas_available',
                'kube_deployment_status_replicas_unavailable': 'deployment.replicas_unavailable',
                'kube_deployment_status_replicas_updated': 'deployment.replicas_updated',
                'kube_hpa_spec_min_replicas': 'hpa.min_replicas',
                'kube_hpa_spec_max_replicas': 'hpa.max_replicas',
                'kube_hpa_status_desired_replicas': 'hpa.desired_replicas',
                'kube_hpa_status_current_replicas': 'hpa.current_replicas',
                'kube_node_status_allocatable_cpu_cores': 'node.cpu_allocatable',
                'kube_node_status_allocatable_memory_bytes': 'node.memory_allocatable',
                'kube_node_status_allocatable_pods': 'node.pods_allocatable',
                'kube_node_status_capacity_cpu_cores': 'node.cpu_capacity',
                'kube_node_status_capacity_memory_bytes': 'node.memory_capacity',
                'kube_node_status_capacity_pods': 'node.pods_capacity',
                'kube_node_status_allocatable_nvidia_gpu_cards': 'node.gpu.cards_allocatable',
                'kube_node_status_capacity_nvidia_gpu_cards': 'node.gpu.cards_capacity',
                'kube_pod_container_status_terminated': 'container.terminated',
                'kube_pod_container_status_waiting': 'container.waiting',
                'kube_persistentvolumeclaim_status_phase': 'persistentvolumeclaim.status',
                'kube_pod_container_resource_limits_cpu_cores': 'container.cpu_limit',
                'kube_pod_container_resource_limits_memory_bytes': 'container.memory_limit',
                'kube_pod_container_resource_requests_cpu_cores': 'container.cpu_requested',
                'kube_pod_container_resource_requests_memory_bytes': 'container.memory_requested',
                'kube_pod_container_status_ready': 'container.ready',
                'kube_pod_container_status_restarts': 'container.restarts',   # up to kube-state-metrics 1.1.x
                'kube_pod_container_status_restarts_total': 'container.restarts',  # from kube-state-metrics 1.2.0
                'kube_pod_container_status_running': 'container.running',
                'kube_pod_container_resource_requests_nvidia_gpu_devices': 'container.gpu.request',
                'kube_pod_container_resource_limits_nvidia_gpu_devices': 'container.gpu.limit',
                'kube_pod_status_ready': 'pod.ready',
                'kube_pod_status_scheduled': 'pod.scheduled',
                'kube_replicaset_spec_replicas': 'replicaset.replicas_desired',
                'kube_replicaset_status_fully_labeled_replicas': 'replicaset.fully_labeled_replicas',
                'kube_replicaset_status_ready_replicas': 'replicaset.replicas_ready',
                'kube_replicaset_status_replicas': 'replicaset.replicas',
                'kube_replicationcontroller_spec_replicas': 'replicationcontroller.replicas_desired',
                'kube_replicationcontroller_status_available_replicas': 'replicationcontroller.replicas_available',
                'kube_replicationcontroller_status_fully_labeled_replicas': 'replicationcontroller.fully_labeled_replicas',  # noqa: E501
                'kube_replicationcontroller_status_ready_replicas': 'replicationcontroller.replicas_ready',
                'kube_replicationcontroller_status_replicas': 'replicationcontroller.replicas',
                'kube_statefulset_replicas': 'statefulset.replicas_desired',
                'kube_statefulset_status_replicas': 'statefulset.replicas',
                'kube_statefulset_status_replicas_current': 'statefulset.replicas_current',
                'kube_statefulset_status_replicas_ready': 'statefulset.replicas_ready',
                'kube_statefulset_status_replicas_updated': 'statefulset.replicas_updated',
            }],
            'ignore_metrics': [
                # _info, _labels and _created don't convey any metric
                'kube_cronjob_info',
                'kube_cronjob_created',
                'kube_daemonset_created',
                'kube_deployment_created',
                'kube_deployment_labels',
                'kube_job_created',
                'kube_job_info',
                'kube_limitrange_created',
                'kube_namespace_created',
                'kube_namespace_labels',
                'kube_node_created',
                'kube_node_info',
                'kube_node_labels',
                'kube_pod_created'
                'kube_pod_container_info',
                'kube_pod_info',
                'kube_pod_owner',
                'kube_pod_start_time',
                'kube_pod_labels',
                'kube_replicaset_created',
                'kube_replicationcontroller_created',
                'kube_resourcequota_created',
                'kube_service_created',
                'kube_service_info',
                'kube_service_labels',
                'kube_statefulset_labels',
                'kube_statefulset_created',
                # _generation metrics are more metadata than metrics, no real use case for now
                'kube_daemonset_metadata_generation',
                'kube_deployment_metadata_generation',
                'kube_deployment_status_observed_generation',
                'kube_replicaset_metadata_generation',
                'kube_replicaset_status_observed_generation',
                'kube_replicationcontroller_metadata_generation',
                'kube_replicationcontroller_status_observed_generation',
                'kube_statefulset_metadata_generation',
                'kube_statefulset_status_observed_generation',
                'kube_hpa_metadata_generation',
                # kube_node_status_phase and kube_namespace_status_phase have no use case as a service check
                'kube_namespace_status_phase',
                'kube_node_status_phase',
                # These CronJob and Job metrics need use cases to determine how do implement
                'kube_cronjob_status_active',
                'kube_cronjob_status_last_schedule_time',
                'kube_cronjob_spec_suspend',
                'kube_cronjob_spec_starting_deadline_seconds',
                'kube_job_spec_active_dealine_seconds',
                'kube_job_spec_completions',
                'kube_job_spec_parallelism',
                'kube_job_status_active',
                'kube_job_status_completion_time',  # We could compute the duration=completion-start as a gauge
                'kube_job_status_start_time',
            ],
            'label_joins': {
                'kube_pod_info': {
                    'label_to_match': 'pod',
                    'labels_to_get': ['node']
                }
            }
        })

        instance['prometheus_url'] = endpoint
        instance['label_joins'].update(extra_labels)
        instance['label_to_hostname'] = 'node' if hostname_override else None

        if 'labels_mapper' in instance and not isinstance(instance['labels_mapper'], dict):
                self.log.warning("Option labels_mapper should be a dictionary for {}".format(endpoint))

        return instance

    def _condition_to_service_check(self, metric, sc_name, mapping, tags=None):
        """
        Some metrics contains conditions, labels that have "condition" as name and "true", "false", or "unknown"
        as value. The metric value is expected to be a gauge equal to 0 or 1 in this case.
        For example:

        metric {
          label { name: "condition", value: "true"
          }
          # other labels here
          gauge { value: 1.0 }
        }

        This function evaluates metrics containing conditions and sends a service check
        based on a provided condition->check mapping dict
        """
        if bool(metric.gauge.value) is False:
            return  # Ignore if gauge is not 1
        for label in metric.label:
            if label.name == 'condition':
                if label.value in mapping:
                    self.service_check(sc_name, mapping[label.value], tags=tags)
                else:
                    self.log.debug("Unable to handle %s - unknown condition %s" % (sc_name, label.value))

    def _condition_to_tag_check(self, metric, base_sc_name, mapping, scraper_config, tags=None):
        """
        Metrics from kube-state-metrics have changed
        For example:
        kube_node_status_condition{condition="Ready",node="ip-172-33-39-189.eu-west-1.compute",status="true"} 1
        kube_node_status_condition{condition="OutOfDisk",node="ip-172-33-57-130.eu-west-1.compute",status="false"} 1
        metric {
          label { name: "condition", value: "true"
          }
          # other labels here
          gauge { value: 1.0 }
        }

        This function evaluates metrics containing conditions and sends a service check
        based on a provided condition->check mapping dict
        """
        if bool(metric.gauge.value) is False:
            return  # Ignore if gauge is not 1 and we are not processing the pod phase check

        label_value, condition_map = self._get_metric_condition_map(base_sc_name, metric.label)
        service_check_name = condition_map['service_check_name']
        mapping = condition_map['mapping']

        if base_sc_name == 'kubernetes_state.pod.phase':
            pod = self._label_to_tag('pod', metric.label, scraper_config)
            phase = self._label_to_tag('phase', metric.label, scraper_config)
            message = "{} is currently reporting {}".format(pod, phase)
        else:
            node = self._label_to_tag('node', metric.label, scraper_config)
            condition = self._label_to_tag('condition', metric.label, scraper_config)
            message = "{} is currently reporting {}".format(node, condition)

        if condition_map['service_check_name'] is None:
            self.log.debug("Unable to handle {} - unknown condition {}".format(service_check_name, label_value))
        else:
            self.service_check(service_check_name, mapping[label_value], tags=tags, message=message)
            self.log.debug("{} {} {}".format(service_check_name, mapping[label_value], tags))

    def _get_metric_condition_map(self, base_sc_name, labels):
        if base_sc_name == 'kubernetes_state.node':
            switch = {
                'Ready': {
                    'service_check_name': base_sc_name + '.ready',
                    'mapping': self.condition_to_status_positive
                },
                'OutOfDisk': {
                    'service_check_name': base_sc_name + '.out_of_disk',
                    'mapping': self.condition_to_status_negative
                },
                'DiskPressure': {
                    'service_check_name': base_sc_name + '.disk_pressure',
                    'mapping': self.condition_to_status_negative
                },
                'NetworkUnavailable': {
                    'service_check_name': base_sc_name + '.network_unavailable',
                    'mapping': self.condition_to_status_negative
                },
                'MemoryPressure': {
                    'service_check_name': base_sc_name + '.memory_pressure',
                    'mapping': self.condition_to_status_negative
                }
            }
            label_value = self._extract_label_value('status', labels)
            return label_value, switch.get(self._extract_label_value('condition', labels),
                                           {'service_check_name': None, 'mapping': None})

        elif base_sc_name == 'kubernetes_state.pod.phase':
            label_value = self._extract_label_value('phase', labels)
            return label_value, {'service_check_name': base_sc_name, 'mapping': self.pod_phase_to_status}

    def _extract_label_value(self, name, labels):
        """
        Search for `name` in labels name and returns
        corresponding value.
        Returns None if name was not found.
        """
        for label in labels:
            if label.name == name:
                return label.value
        return None

    def _format_tag(self, name, value, scraper_config):
        """
        Lookups the labels_mapper table to see if replacing the tag name is
        necessary, then returns a "name:value" tag string
        """
        return '%s:%s' % (scraper_config['labels_mapper'].get(name, name), value)

    def _label_to_tag(self, name, labels, scraper_config, tag_name=None):
        """
        Search for `name` in labels name and returns corresponding tag string.
        Tag name is label name if not specified.
        Returns None if name was not found.
        """
        value = self._extract_label_value(name, labels)
        if value:
            return self._format_tag(tag_name or name, value, scraper_config)
        else:
            return None

    def _trim_job_tag(self, name):
        """
        Trims suffix of job names if they match -(\d{4,10}$)
        """
        pattern = "(-\d{4,10}$)"
        return re.sub(pattern, '', name)

    # Labels attached: namespace, pod
    # As a message the phase=Pending|Running|Succeeded|Failed|Unknown
    # From the phase the check will update its status
    # Also submits as an aggregated count with minimal tags so it is
    # visualisable over time per namespace and phase
    def kube_pod_status_phase(self, message, scraper_config):
        """ Phase a pod is in. """
        metric_name = scraper_config['NAMESPACE'] + '.pod.status_phase'
        # Will submit a service check which status is given by its phase.
        # More details about the phase in the message of the check.
        check_basename = scraper_config['NAMESPACE'] + '.pod.phase'
        status_phase_counter = Counter()

        for metric in message.metric:
            pod_tag = self._label_to_tag("pod", metric.label, scraper_config)
            namespace_tag = self._label_to_tag("namespace", metric.label, scraper_config)
            self._condition_to_tag_check(metric, check_basename, self.pod_phase_to_status, scraper_config,
                                         tags=[pod_tag, namespace_tag] + scraper_config['custom_tags'])

            # Counts aggregated cluster-wide to avoid no-data issues on pod churn,
            # pod granularity available in the service checks
            tags = [
                self._label_to_tag("namespace", metric.label, scraper_config),
                self._label_to_tag("phase", metric.label, scraper_config)
            ] + scraper_config['custom_tags']
            status_phase_counter[tuple(sorted(tags))] += metric.gauge.value

        for tags, count in status_phase_counter.iteritems():
            self.gauge(metric_name, count, tags=list(tags))

    def kube_pod_container_status_waiting_reason(self, message, scraper_config):
        metric_name = scraper_config['NAMESPACE'] + '.container.status_report.count.waiting'
        for metric in message.metric:
            tags = []
            skip_metric = False
            for label in metric.label:
                if label.name == "reason":
                    if label.value.lower() in WHITELISTED_WAITING_REASONS:
                        tags.append(self._format_tag(label.name, label.value, scraper_config))
                    else:
                        skip_metric = True
                elif label.name == "container":
                    tags.append(self._format_tag("kube_container_name", label.value, scraper_config))
                elif label.name == "namespace":
                    tags.append(self._format_tag(label.name, label.value, scraper_config))
            if not skip_metric:
                self.count(metric_name, metric.gauge.value, tags + scraper_config['custom_tags'])

    def kube_pod_container_status_terminated_reason(self, message, scraper_config):
        metric_name = scraper_config['NAMESPACE'] + '.container.status_report.count.terminated'
        for metric in message.metric:
            tags = []
            skip_metric = False
            for label in metric.label:
                if label.name == "reason":
                    if label.value.lower() in WHITELISTED_TERMINATED_REASONS:
                        tags.append(self._format_tag(label.name, label.value, scraper_config))
                    else:
                        skip_metric = True
                elif label.name == "container":
                    tags.append(self._format_tag("kube_container_name", label.value, scraper_config))
                elif label.name == "namespace":
                    tags.append(self._format_tag(label.name, label.value, scraper_config))
            if not skip_metric:
                self.count(metric_name, metric.gauge.value, tags + scraper_config['custom_tags'])

    def kube_cronjob_next_schedule_time(self, message, scraper_config):
        """ Time until the next schedule """
        # Used as a service check so that one can be alerted if the cronjob's next schedule is in the past
        check_basename = scraper_config['NAMESPACE'] + '.cronjob.on_schedule_check'
        curr_time = int(time.time())
        for metric in message.metric:
            on_schedule = int(metric.gauge.value) - curr_time
            tags = [self._format_tag(label.name, label.value, scraper_config) for label in metric.label]
            tags += scraper_config['custom_tags']
            if on_schedule < 0:
                message = "The service check scheduled at {} is {} seconds late".format(
                    time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(int(metric.gauge.value))), on_schedule
                )
                self.service_check(check_basename, self.CRITICAL, tags=tags, message=message)
            else:
                self.service_check(check_basename, self.OK, tags=tags)

    def kube_job_complete(self, message, scraper_config):
        service_check_name = scraper_config['NAMESPACE'] + '.job.complete'
        for metric in message.metric:
            tags = []
            for label in metric.label:
                if label.name == 'job':
                    trimmed_job = self._trim_job_tag(label.value)
                    tags.append(self._format_tag(label.name, trimmed_job, scraper_config))
                else:
                    tags.append(self._format_tag(label.name, label.value, scraper_config))
            self.service_check(service_check_name, self.OK, tags=tags + scraper_config['custom_tags'])

    def kube_job_failed(self, message, scraper_config):
        service_check_name = scraper_config['NAMESPACE'] + '.job.complete'
        for metric in message.metric:
            tags = []
            for label in metric.label:
                if label.name == 'job':
                    trimmed_job = self._trim_job_tag(label.value)
                    tags.append(self._format_tag(label.name, trimmed_job, scraper_config))
                else:
                    tags.append(self._format_tag(label.name, label.value, scraper_config))
            self.service_check(service_check_name, self.CRITICAL, tags=tags + scraper_config['custom_tags'])

    def kube_job_status_failed(self, message, scraper_config):
        for metric in message.metric:
            tags = [] + scraper_config['custom_tags']
            for label in metric.label:
                if label.name == 'job':
                    trimmed_job = self._trim_job_tag(label.value)
                    tags.append(self._format_tag(label.name, trimmed_job, scraper_config))
                else:
                    tags.append(self._format_tag(label.name, label.value, scraper_config))
            self.job_failed_count[frozenset(tags)] += metric.gauge.value

    def kube_job_status_succeeded(self, message, scraper_config):
        for metric in message.metric:
            tags = [] + scraper_config['custom_tags']
            for label in metric.label:
                if label.name == 'job':
                    trimmed_job = self._trim_job_tag(label.value)
                    tags.append(self._format_tag(label.name, trimmed_job, scraper_config))
                else:
                    tags.append(self._format_tag(label.name, label.value, scraper_config))
            self.job_succeeded_count[frozenset(tags)] += metric.gauge.value

    def kube_node_status_condition(self, message, scraper_config):
        """ The ready status of a cluster node. v1.0+"""
        base_check_name = scraper_config['NAMESPACE'] + '.node'
        metric_name = scraper_config['NAMESPACE'] + '.nodes.by_condition'
        by_condition_counter = Counter()

        for metric in message.metric:
            node_tag = self._label_to_tag("node", metric.label, scraper_config)
            self._condition_to_tag_check(metric, base_check_name, self.condition_to_status_positive, scraper_config,
                                         tags=[node_tag] + scraper_config['custom_tags'])

            # Counts aggregated cluster-wide to avoid no-data issues on node churn,
            # node granularity available in the service checks
            tags = [
                self._label_to_tag("condition", metric.label, scraper_config),
                self._label_to_tag("status", metric.label, scraper_config)
            ] + scraper_config['custom_tags']
            by_condition_counter[tuple(sorted(tags))] += metric.gauge.value

        for tags, count in by_condition_counter.iteritems():
            self.gauge(metric_name, count, tags=list(tags))

    def kube_node_status_ready(self, message, scraper_config):
        """ The ready status of a cluster node (legacy)"""
        service_check_name = scraper_config['NAMESPACE'] + '.node.ready'
        for metric in message.metric:
            node_tag = self._label_to_tag("node", metric.label, scraper_config)
            self._condition_to_service_check(metric, service_check_name, self.condition_to_status_positive,
                                             tags=[node_tag] + scraper_config['custom_tags'])

    def kube_node_status_out_of_disk(self, message, scraper_config):
        """ Whether the node is out of disk space (legacy)"""
        service_check_name = scraper_config['NAMESPACE'] + '.node.out_of_disk'
        for metric in message.metric:
            node_tag = self._label_to_tag("node", metric.label, scraper_config)
            self._condition_to_service_check(metric, service_check_name, self.condition_to_status_negative,
                                             tags=[node_tag] + scraper_config['custom_tags'])

    def kube_node_status_memory_pressure(self, message, scraper_config):
        """ Whether the node is in a memory pressure state (legacy)"""
        service_check_name = scraper_config['NAMESPACE'] + '.node.memory_pressure'
        for metric in message.metric:
            node_tag = self._label_to_tag("node", metric.label, scraper_config)
            self._condition_to_service_check(metric, service_check_name, self.condition_to_status_negative,
                                             tags=[node_tag] + scraper_config['custom_tags'])

    def kube_node_status_disk_pressure(self, message, scraper_config):
        """ Whether the node is in a disk pressure state (legacy)"""
        service_check_name = scraper_config['NAMESPACE'] + '.node.disk_pressure'
        for metric in message.metric:
            node_tag = self._label_to_tag("node", metric.label, scraper_config)
            self._condition_to_service_check(metric, service_check_name, self.condition_to_status_negative,
                                             tags=[node_tag] + scraper_config['custom_tags'])

    def kube_node_status_network_unavailable(self, message, scraper_config):
        """ Whether the node is in a network unavailable state (legacy)"""
        service_check_name = scraper_config['NAMESPACE'] + '.node.network_unavailable'
        for metric in message.metric:
            node_tag = self._label_to_tag("node", metric.label, scraper_config)
            self._condition_to_service_check(metric, service_check_name, self.condition_to_status_negative,
                                             tags=[node_tag] + scraper_config['custom_tags'])

    def kube_node_spec_unschedulable(self, message, scraper_config):
        """ Whether a node can schedule new pods. """
        metric_name = scraper_config['NAMESPACE'] + '.node.status'
        statuses = ('schedulable', 'unschedulable')
        if message.type < len(METRIC_TYPES):
            for metric in message.metric:
                tags = [self._format_tag(label.name, label.value, scraper_config) for label in metric.label]
                tags += scraper_config['custom_tags']
                status = statuses[int(getattr(metric, METRIC_TYPES[message.type]).value)]  # value can be 0 or 1
                tags.append(self._format_tag('status', status, scraper_config))
                self.gauge(metric_name, 1, tags)  # metric value is always one, value is on the tags
        else:
            self.log.error("Metric type %s unsupported for metric %s" % (message.type, message.name))

    def kube_resourcequota(self, message, scraper_config):
        """ Quota and current usage by resource type. """
        metric_base_name = scraper_config['NAMESPACE'] + '.resourcequota.{}.{}'
        suffixes = {'used': 'used', 'hard': 'limit'}
        if message.type < len(METRIC_TYPES):
            for metric in message.metric:
                mtype = self._extract_label_value("type", metric.label)
                resource = self._extract_label_value("resource", metric.label)
                tags = [
                    self._label_to_tag("namespace", metric.label, scraper_config),
                    self._label_to_tag("resourcequota", metric.label, scraper_config)
                ] + scraper_config['custom_tags']
                val = getattr(metric, METRIC_TYPES[message.type]).value
                self.gauge(metric_base_name.format(resource, suffixes[mtype]), val, tags)
        else:
            self.log.error("Metric type %s unsupported for metric %s" % (message.type, message.name))

    def kube_limitrange(self, message, scraper_config):
        """ Resource limits by consumer type. """
        # type's cardinality's low: https://github.com/kubernetes/kubernetes/blob/v1.6.1/pkg/api/v1/types.go#L3872-L3879
        # idem for resource: https://github.com/kubernetes/kubernetes/blob/v1.6.1/pkg/api/v1/types.go#L3342-L3352
        # idem for constraint: https://github.com/kubernetes/kubernetes/blob/v1.6.1/pkg/api/v1/types.go#L3882-L3901
        metric_base_name = scraper_config['NAMESPACE'] + '.limitrange.{}.{}'
        constraints = {
            'min': 'min',
            'max': 'max',
            'default': 'default',
            'defaultRequest': 'default_request',
            'maxLimitRequestRatio': 'max_limit_request_ratio',
        }

        if message.type < len(METRIC_TYPES):
            for metric in message.metric:
                constraint = self._extract_label_value('constraint', metric.label)
                if constraint in constraints:
                    constraint = constraints[constraint]
                else:
                    self.error("Constraint %s unsupported for metric %s" % (constraint, message.name))
                    continue
                resource = self._extract_label_value("resource", metric.label)
                tags = [
                    self._label_to_tag("namespace", metric.label, scraper_config),
                    self._label_to_tag("limitrange", metric.label, scraper_config),
                    self._label_to_tag("type", metric.label, scraper_config, tag_name="consumer_type")
                ] + scraper_config['custom_tags']
                val = getattr(metric, METRIC_TYPES[message.type]).value
                self.gauge(metric_base_name.format(resource, constraint), val, tags)
        else:
            self.log.error("Metric type %s unsupported for metric %s" % (message.type, message.name))
