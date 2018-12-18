# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)

from ...errors import CheckException

from kubernetes import client, config
from six import PY3, iteritems, string_types

try:
    import datadog_agent
except ImportError:
    from ...stubs import datadog_agent

from .. import AgentCheck
from .record import ElectionRecord

# Known names of the leader election annotation,
# will be tried in the order of the list
ELECTION_ANNOTATION_NAMES = ["control-plane.alpha.kubernetes.io/leader"]


class KubeLeaderElectionMixin(object):
    # pylint: disable=E1101
    # This class is not supposed to be used by itself, it provides scraping behavior but
    # need to be within a check in the end

    # config:
    #   namespace:
    #   record_kind: endpoint/configmap
    #   record_name:
    #   record_namespace:
    #   tags:

    # from agent config:
    #  kubernetes_kubeconfig_path: defaut is to use in-cluster config

    def __init__(self, *args, **kwargs):
        # Initialize AgentCheck's base class
        super(KubeLeaderElectionMixin, self).__init__(*args, **kwargs)

        # `NAMESPACE` is taken from the child check class as a fallback
        # if no namespace is given in the configuration.
        self.NAMESPACE = ''

        kubeconfig_path = datadog_agent.get_config('kubernetes_kubeconfig_path')
        if kubeconfig_path:
            config.load_kube_config(config_file=kubeconfig_path)
        else:
            config.load_incluster_config()

    def check_election_status(self, config):
        try:
            record = self._get_record(
                config.get("record_kind", ""), config.get("record_name", ""), config.get("record_namespace", "")
            )
            self._report_status(record)
        except Exception as e:
            self.warn("Cannot retrieve leader election record {}: {}".format(config.get("record_name", ""), e))

    @staticmethod
    def _get_record(kind, name, namespace):
        v1 = client.CoreV1Api()
        obj = None

        if kind.lower() == "endpoints":
            obj = v1.read_namespaced_endpoints(name, namespace)
        elif kind.lower() == "configmap":
            obj = v1.read_namespaced_endpoints(name, namespace)
        else:
            raise ValueError("Unknown kind {}".format(kind))

        if not obj:
            return ValueError("Empty input object")

        # Can raise AttributeError if object is not a v1 kube object
        annotations = obj.metadata.annotations

        for name in ELECTION_ANNOTATION_NAMES:
            if name in annotations:
                return ElectionRecord(annotations[name])

        # Could not find annotation
        raise ValueError("Object has no valid leader election annotation")

    def _report_status(self, config, record):
        # Compute prefix for gauges and service check
        prefix = config.get("namespace", self.NAMESPACE)
        if prefix.len() < 1:
            raise ValueError("metric namespace is empty")
        prefix += ".leader_election"

        # Compute tags for gauges and service check
        tags = config.get("tags", [])
        for n in ["record_kind", "record_name", "record_namespace"]:
            if n in config:
                tags += "{}:{}".format(n, config[n])

        # Sanity check on the record
        valid, reason = record.validate()
        if not valid:
            self.service_check(prefix, AgentCheck.CRITICAL, tags=tags, message=reason)
            return  # Stop here

        # Report gauges
        self.gauge(prefix + ".transitions", record.transitions, tags)
        self.gauge(prefix + ".lease_duration", record.lease_duration, tags)

        leader_status = AgentCheck.OK
        if record.seconds_until_renew + record.lease_duration < 0:
            leader_status = AgentCheck.CRITICAL
        self.service_check(prefix, leader_status, tags=tags, message=record)
