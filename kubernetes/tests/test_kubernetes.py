import pytest
import os
import subprocess
import logging
import requests
import time

from datadog_checks.kubernetes import Kubernetes

log = logging.getLogger('test_kong')

CPU = "CPU"
MEM = "MEM"
FS = "fs"
NET = "net"
NET_ERRORS = "net_errors"
DISK = "disk"
DISK_USAGE = "disk_usage"
PODS = "pods"
LIM = "limits"
REQ = "requests"
CAP = "capacity"

log = logging.getLogger('test_kong')

HERE = os.path.dirname(os.path.abspath(__file__))

FIXTURE_DIR = os.path.join(HERE, 'fixtures')

METRICS = [
    ('kubernetes.memory.usage', MEM),
    ('kubernetes.filesystem.usage', FS),
    ('kubernetes.filesystem.usage_pct', FS),
    ('kubernetes.cpu.usage.total', CPU),
    ('kubernetes.network.tx_bytes', NET),
    ('kubernetes.network.rx_bytes', NET),
    ('kubernetes.network_errors', NET_ERRORS),
    ('kubernetes.diskio.io_service_bytes.stats.total', DISK),
    ('kubernetes.filesystem.usage_pct', DISK_USAGE),
    ('kubernetes.filesystem.usage', DISK_USAGE),
    ('kubernetes.pods.running', PODS),
    ('kubernetes.cpu.limits', LIM),
    ('kubernetes.cpu.requests', REQ),
    ('kubernetes.cpu.capacity', CAP),
    ('kubernetes.memory.limits', LIM),
    ('kubernetes.memory.requests', REQ),
    ('kubernetes.memory.capacity', CAP),
]


class MockResponse:
    """
    Helper class to mock a json response from requests
    """
    def __init__(self, json_data, status_code):
        self.json_data = json_data
        self.status_code = status_code

    def json(self):
        return self.json_data


class MockIterLinesResponse:
    """
    Helper class to mock a text response from requests
    """
    def __init__(self, lines_array, status_code):
        self.lines_array = lines_array
        self.status_code = status_code

    def iter_lines(self):
        for line in self.lines_array:
            yield line

def KubeUtil_fake_retrieve_json_auth(url, timeout=10, params=None):
    if url.endswith("/namespaces"):
        return MockResponse(json.loads(Fixtures.read_file("namespaces.json", sdk_dir=FIXTURE_DIR, string_escape=False)), 200)
    if url.endswith("/events"):
        return MockResponse(json.loads(Fixtures.read_file("events.json", sdk_dir=FIXTURE_DIR, string_escape=False)), 200)
    return {}

@pytest.fixture
def check():
    return Kubernetes(CHECK_NAME, {}, {})


@pytest.fixture
def aggregator():
    from datadog_checks.stubs import aggregator
    aggregator.reset()
    return aggregator

def read_file(filename):
    with open(os.path.join(FIXTURE_DIR, filename)) as f:
        return f.read()

@pytest.fixture
def metrics_1():
    metrics = json.loads(read_file('metrics_1.1.json'))
    p = mock.patch('utils.kubernetes.KubeUtil.retrieve_metrics', side_effect=lambda: metrics)

    yield p.start()

    p.stop()

@pytest.fixture
def pods_1():
    pods = json.loads(read_file("pods_list_1.1.json"))
    p = mock.patch('utils.kubernetes.KubeUtil.retrieve_pods_list', side_effect=lambda: pods)

    yield p.start()

    p.stop()

@pytest.fixture
def mocks():
    p1 = mock.patch('utils.kubernetes.KubeUtil.retrieve_json_auth')
    p2 = mock.patch('utils.kubernetes.KubeUtil.retrieve_machine_info')
    p3 = mock.patch('utils.kubernetes.KubeUtil._locate_kubelet', return_value='http://172.17.0.1:10255')


    yield p1.start(), p2.start(), p3.start()

    p1.stop()
    p2.stop()
    p3.stop()


def test_fail_1_1(check, aggregator, metrics_1, pods_1, mocks):
    # To avoid the disparition of some gauges during the second check
    config = {
        "instances": [{"host": "foo"}]
    }

    check.check(config)
    aggregator.assert_service_check('kong.can_connect', status=Kubernetes.CRITICAL, tags=None, count=1)
