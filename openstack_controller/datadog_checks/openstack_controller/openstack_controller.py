# (C) Datadog, Inc. 2010-2017
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)
import re
import copy
import requests

from six import iteritems, itervalues, next
from datetime import datetime, timedelta

from datadog_checks.checks import AgentCheck
from datadog_checks.config import is_affirmative
from datadog_checks.utils.common import pattern_filter

from .api import ApiFactory
from .utils import get_instance_name, traced
from .retry import BackOffRetry
from .exceptions import (InstancePowerOffFailure, IncompleteConfig, IncompleteIdentity, MissingNovaEndpoint,
                         MissingNeutronEndpoint, KeystoneUnreachable, AuthenticationNeeded)


try:
    # Agent >= 6.0: the check pushes tags invoking `set_external_tags`
    from datadog_agent import set_external_tags
except ImportError:
    # Agent < 6.0: the Agent pulls tags invoking `OpenStackControllerCheck.get_external_host_tags`
    set_external_tags = None


SOURCE_TYPE = 'openstack'

NOVA_HYPERVISOR_METRICS = [
    'current_workload',
    'disk_available_least',
    'free_disk_gb',
    'free_ram_mb',
    'local_gb',
    'local_gb_used',
    'memory_mb',
    'memory_mb_used',
    'running_vms',
    'vcpus',
    'vcpus_used',
]

NOVA_SERVER_METRICS = [
    "hdd_errors",
    "hdd_read",
    "hdd_read_req",
    "hdd_write",
    "hdd_write_req",
    "memory",
    "memory-actual",
    "memory-rss",
    "cpu0_time",
    "vda_errors",
    "vda_read",
    "vda_read_req",
    "vda_write",
    "vda_write_req",
]

NOVA_SERVER_INTERFACE_SEGMENTS = ['_rx', '_tx']

PROJECT_METRICS = dict(
    [
        ("maxImageMeta", "max_image_meta"),
        ("maxPersonality", "max_personality"),
        ("maxPersonalitySize", "max_personality_size"),
        ("maxSecurityGroupRules", "max_security_group_rules"),
        ("maxSecurityGroups", "max_security_groups"),
        ("maxServerMeta", "max_server_meta"),
        ("maxTotalCores", "max_total_cores"),
        ("maxTotalFloatingIps", "max_total_floating_ips"),
        ("maxTotalInstances", "max_total_instances"),
        ("maxTotalKeypairs", "max_total_keypairs"),
        ("maxTotalRAMSize", "max_total_ram_size"),
        ("totalImageMetaUsed", "total_image_meta_used"),
        ("totalPersonalityUsed", "total_personality_used"),
        ("totalPersonalitySizeUsed", "total_personality_size_used"),
        ("totalSecurityGroupRulesUsed", "total_security_group_rules_used"),
        ("totalSecurityGroupsUsed", "total_security_groups_used"),
        ("totalServerMetaUsed", "total_server_meta_used"),
        ("totalCoresUsed", "total_cores_used"),
        ("totalFloatingIpsUsed", "total_floating_ips_used"),
        ("totalInstancesUsed", "total_instances_used"),
        ("totalKeypairsUsed", "total_keypairs_used"),
        ("totalRAMUsed", "total_ram_used"),
    ]
)

DIAGNOSTICABLE_STATES = ['ACTIVE']

REMOVED_STATES = ['DELETED', 'SHUTOFF']

SERVER_FIELDS_REQ = [
    'server_id',
    'state',
    'server_name',
    'hypervisor_hostname',
    'tenant_id',
]


class OpenStackControllerCheck(AgentCheck):
    CACHE_TTL = {"aggregates": 300, "physical_hosts": 300, "hypervisors": 300}  # seconds

    FETCH_TIME_ACCESSORS = {
        "aggregates": "_last_aggregate_fetch_time",
        "physical_hosts": "_last_host_fetch_time",
        "hypervisors": "_last_hypervisor_fetch_time",
    }

    HYPERVISOR_STATE_UP = 'up'
    HYPERVISOR_STATE_DOWN = 'down'
    NETWORK_STATE_UP = 'UP'

    NETWORK_API_SC = 'openstack.neutron.api.up'
    COMPUTE_API_SC = 'openstack.nova.api.up'
    IDENTITY_API_SC = 'openstack.keystone.api.up'

    # Service checks for individual hypervisors and networks
    HYPERVISOR_SC = 'openstack.nova.hypervisor.up'
    NETWORK_SC = 'openstack.neutron.network.up'

    HYPERVISOR_CACHE_EXPIRY = 120  # seconds

    def __init__(self, name, init_config, agentConfig, instances=None):
        super(OpenStackControllerCheck, self).__init__(name, init_config, agentConfig, instances)
        # Global Variables
        self._backoff = BackOffRetry()
        # We cache all api instances.
        # This allows to cache connection if the underlying implementation support it
        # Ex: _apis = {
        #   <instance_name>: <api object>
        # }
        self._apis = {}
        # Ex: servers_cache = {
        #   <instance_name>: {
        #       'servers': {<server_id>: <server_metadata>},
        #       'changes_since': <ISO8601 date time>
        #   }
        # }
        self.servers_cache = {}
        # Cache some things between runs for values that change rarely
        self._aggregate_list = {}

        # Instance Variables
        self.instance_name = None
        self.keystone_server_url = None
        self.proxy_config = None
        self.ssl_verify = False
        self.exclude_network_id_rules = []
        self.exclude_server_id_rules = []
        self.include_project_name_rules = []
        self.exclude_project_name_rules = []
        # Mapping of Nova-managed servers to tags
        self.external_host_tags = {}

    def collect_networks_metrics(self, tags):
        """
        Collect stats for all reachable networks
        """
        networks = self.get_networks()
        network_ids = self.init_config.get('network_ids', [])
        filtered_networks = []
        if not network_ids:
            # Filter out excluded networks
            filtered_networks = [
                network
                for network in networks
                if not any([re.match(exclude_id, network.get('id')) for exclude_id in self.exclude_network_id_rules])
            ]
        else:
            for network in networks:
                if network.get('id') in network_ids:
                    filtered_networks.append(network)

        for network in filtered_networks:
            network_id = network.get('id')
            service_check_tags = ['network:{}'.format(network_id)] + tags

            network_name = network.get('name')
            if network_name:
                service_check_tags.append('network_name:{}'.format(network_name))

            tenant_id = network.get('tenant_id')
            if tenant_id:
                service_check_tags.append('tenant_id:{}'.format(tenant_id))

            if network.get('admin_state_up'):
                self.service_check(self.NETWORK_SC, AgentCheck.OK, tags=service_check_tags)
            else:
                self.service_check(self.NETWORK_SC, AgentCheck.CRITICAL, tags=service_check_tags)

    # Compute
    def _parse_uptime_string(self, uptime):
        """ Parse u' 16:53:48 up 1 day, 21:34,  3 users,  load average: 0.04, 0.14, 0.19\n' """
        uptime = uptime.strip()
        load_averages = uptime[uptime.find('load average:'):].split(':')[1].strip().split(',')
        load_averages = [float(load_avg) for load_avg in load_averages]
        return load_averages

    def get_all_aggregate_hypervisors(self):
        hypervisor_aggregate_map = {}
        try:
            aggregate_list = self.get_os_aggregates()
            for v in aggregate_list:
                for host in v['hosts']:
                    hypervisor_aggregate_map[host] = {
                        'aggregate': v['name'],
                        'availability_zone': v['availability_zone'],
                    }

        except Exception as e:
            self.warning('Unable to get the list of aggregates: {}'.format(e))
            raise e

        return hypervisor_aggregate_map

    def get_loads_for_single_hypervisor(self, hyp_id):
        uptime = self.get_os_hypervisor_uptime(hyp_id)
        return self._parse_uptime_string(uptime)

    def collect_hypervisors_metrics(self, custom_tags=None,
                                    use_shortname=False,
                                    collect_hypervisor_metrics=True,
                                    collect_hypervisor_load=False):
        """
        Submits stats for all hypervisors registered to this control plane
        Raises specific exceptions based on response code
        """
        resp = self.get_os_hypervisors_detail()
        hypervisors = resp.get('hypervisors', [])
        for hyp in hypervisors:
            self.get_stats_for_single_hypervisor(hyp, custom_tags=custom_tags,
                                                 use_shortname=use_shortname,
                                                 collect_hypervisor_metrics=collect_hypervisor_metrics,
                                                 collect_hypervisor_load=collect_hypervisor_load)

        if not hypervisors:
            self.log.warn("Unable to collect any hypervisors from Nova response: {}".format(resp))

    def get_stats_for_single_hypervisor(self, hyp, custom_tags=None,
                                        use_shortname=False,
                                        collect_hypervisor_metrics=True,
                                        collect_hypervisor_load=False):
        hyp_hostname = hyp.get('hypervisor_hostname')
        custom_tags = custom_tags or []
        tags = [
            'hypervisor:{}'.format(hyp_hostname),
            'hypervisor_id:{}'.format(hyp['id']),
            'virt_type:{}'.format(hyp['hypervisor_type']),
            'status:{}'.format(hyp['status']),
        ]
        host_tags = self._get_host_aggregate_tag(hyp_hostname, use_shortname=use_shortname)
        tags.extend(host_tags)
        tags.extend(custom_tags)
        service_check_tags = list(custom_tags)

        hyp_state = hyp.get('state', None)

        if not hyp_state:
            self.service_check(self.HYPERVISOR_SC, AgentCheck.UNKNOWN, hostname=hyp_hostname, tags=service_check_tags)
        elif hyp_state != self.HYPERVISOR_STATE_UP:
            self.service_check(self.HYPERVISOR_SC, AgentCheck.CRITICAL, hostname=hyp_hostname, tags=service_check_tags)
        else:
            self.service_check(self.HYPERVISOR_SC, AgentCheck.OK, hostname=hyp_hostname, tags=service_check_tags)

        if not collect_hypervisor_metrics:
            return

        for label, val in iteritems(hyp):
            if label in NOVA_HYPERVISOR_METRICS:
                metric_label = "openstack.nova.{}".format(label)
                self.gauge(metric_label, val, tags=tags)

        # This makes a request per hypervisor and only sends hypervisor_load 1/5/15
        # Disable this by default for higher performance in a large environment
        # If the Agent is installed on the hypervisors, system.load.1/5/15 is available
        if collect_hypervisor_load:
            try:
                load_averages = self.get_loads_for_single_hypervisor(hyp['id'])
            except Exception as e:
                self.warning('Unable to get loads averages for hypervisor {}: {}'.format(hyp['id'], e))
                load_averages = []
            if load_averages and len(load_averages) == 3:
                for i, avg in enumerate([1, 5, 15]):
                    self.gauge('openstack.nova.hypervisor_load.{}'.format(avg), load_averages[i], tags=tags)
            else:
                self.log.debug("Load Averages didn't return expected values: {}".format(load_averages))

    def get_active_servers(self, tenant_to_name):
        query_params = {
            "all_tenants": True,
            'status': 'ACTIVE',
        }
        servers = self.get_servers_detail(query_params)

        return {server.get('id'): self.create_server_object(server, tenant_to_name) for server in servers
                if tenant_to_name[server.get('tenant_id')]}

    def update_servers_cache(self, cached_servers, tenant_to_name, changes_since):
        servers = copy.deepcopy(cached_servers)

        query_params = {
            "all_tenants": True,
            'changes-since': changes_since
        }
        updated_servers = self.get_servers_detail(query_params)

        # For each updated servers, we update the servers cache accordingly
        for updated_server in updated_servers:
            updated_server_status = updated_server.get('status')
            updated_server_id = updated_server.get('id')

            if updated_server_status == 'ACTIVE':
                # Add or update the cache
                if tenant_to_name[updated_server.get('tenant_id')]:
                    servers[updated_server_id] = self.create_server_object(updated_server, tenant_to_name)
            else:
                # Remove from the cache if it exists
                if updated_server_id in servers:
                    del servers[updated_server_id]
        return servers

    def create_server_object(self, server, tenant_to_name):
        result = {
            'server_id': server.get('id'),
            'state': server.get('status'),
            'server_name': server.get('name'),
            'hypervisor_hostname': server.get('OS-EXT-SRV-ATTR:hypervisor_hostname'),
            'tenant_id': server.get('tenant_id'),
            'availability_zone': server.get('OS-EXT-AZ:availability_zone'),
            'project_name': tenant_to_name[server.get('tenant_id')]
        }
        # starting version 2.47, flavors infos are contained within the `servers/detail` endpoint
        # See https://developer.openstack.org/api-ref/compute/
        # ?expanded=list-servers-detailed-detail#list-servers-detailed-detail
        # TODO: Instead of relying on the structure of the response, we could use specified versions
        # provided in the config. Both have pros and cons.
        flavor = server.get('flavor', {})
        if 'id' in flavor:
            # Available until version 2.46
            result['flavor_id'] = flavor.get('id')
        if 'disk' in flavor:
            # New in version 2.47
            result['flavor'] = self.create_flavor_object(flavor)
        return result

    # Get all of the server IDs and their metadata and cache them
    # After the first run, we will only get servers that have changed state since the last collection run
    def get_all_servers(self, tenant_to_name, instance_name):
        cached_servers = self.servers_cache.get(instance_name, {}).get('servers')
        # NOTE: updated_time need to be set at the beginning of this method in order to no miss servers changes.
        changes_since = datetime.utcnow().isoformat()
        if cached_servers is None:
            updated_servers = self.get_active_servers(tenant_to_name)
        else:
            previous_changes_since = self.servers_cache.get(instance_name, {}).get('changes_since')
            updated_servers = self.update_servers_cache(cached_servers, tenant_to_name, previous_changes_since)

        # Initialize or update cache for this instance
        self.servers_cache[instance_name] = {
            'servers': updated_servers,
            'changes_since': changes_since
        }

    def collect_server_diagnostic_metrics(self, server_details, tags=None, use_shortname=False):
        def _is_valid_metric(label):
            return label in NOVA_SERVER_METRICS or any(seg in label for seg in NOVA_SERVER_INTERFACE_SEGMENTS)

        def _is_interface_metric(label):
            return any(seg in label for seg in NOVA_SERVER_INTERFACE_SEGMENTS)

        tags = tags or []
        tags = copy.deepcopy(tags)
        tags.append("nova_managed_server")
        hypervisor_hostname = server_details.get('hypervisor_hostname')
        host_tags = self._get_host_aggregate_tag(hypervisor_hostname, use_shortname=use_shortname)
        host_tags.append('availability_zone:{}'.format(server_details.get('availability_zone', 'NA')))
        self.external_host_tags[server_details.get('server_name')] = host_tags

        server_id = server_details.get('server_id')
        server_name = server_details.get('server_name')
        hypervisor_hostname = server_details.get('hypervisor_hostname')
        project_name = server_details.get('project_name')

        try:
            server_stats = self.get_server_diagnostics(server_id)
        except InstancePowerOffFailure:  # 409 response code came back fro nova
            self.log.debug("Server %s is powered off and cannot be monitored", server_id)
            return
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                self.log.debug("Server %s is not in an ACTIVE state and cannot be monitored, %s", server_id, e)
            else:
                self.log.debug("Received HTTP Error when reaching the nova endpoint")
            return
        except Exception as e:
            self.warning("Unknown error when monitoring %s : %s" % (server_id, e))
            return

        if server_stats:
            if project_name:
                tags.append("project_name:{}".format(project_name))
            if hypervisor_hostname:
                tags.append("hypervisor:{}".format(hypervisor_hostname))
            if server_name:
                tags.append("server_name:{}".format(server_name))

            # microversion pre 2.48
            for m in server_stats:
                if _is_interface_metric(m):
                    # Example of interface metric
                    # tap123456_rx_errors
                    metric_pre = re.split("(_rx|_tx)", m)
                    interface = "interface:{}".format(metric_pre[0])
                    self.gauge(
                        "openstack.nova.server.{}{}".format(metric_pre[1].replace("_", ""), metric_pre[2]),
                        server_stats[m],
                        tags=tags+host_tags+[interface],
                        hostname=server_id,
                    )
                elif _is_valid_metric(m):
                    self.gauge(
                        "openstack.nova.server.{}".format(m.replace("-", "_")),
                        server_stats[m],
                        tags=tags+host_tags,
                        hostname=server_id,
                    )

    def collect_project_limit(self, project, tags=None):
        # NOTE: starting from Version 3.10 (Queens)
        # We can use /v3/limits (Unified Limits API) if not experimental any more.
        def _is_valid_metric(label):
            return label in PROJECT_METRICS

        tags = tags or []

        server_tags = copy.deepcopy(tags)
        project_name = project.get('name')
        project_id = project.get('id')

        self.log.debug("Collecting metrics for project. name: {} id: {}".format(project_name, project['id']))
        server_stats = self.get_project_limits(project['id'])
        server_tags.append('tenant_id:{}'.format(project_id))

        if project_name:
            server_tags.append('project_name:{}'.format(project_name))

        try:
            for st in server_stats:
                if _is_valid_metric(st):
                    metric_key = PROJECT_METRICS[st]
                    self.gauge(
                        "openstack.nova.limits.{}".format(metric_key),
                        server_stats[st],
                        tags=server_tags,
                    )
        except KeyError:
            self.log.warn("Unexpected response, not submitting limits metrics for project id".format(project['id']))

    def get_flavors(self):
        flavors = self.get_flavors_detail({})
        return {flavor.get('id'): self.create_flavor_object(flavor) for flavor in flavors}

    @staticmethod
    def create_flavor_object(flavor):
        return {
            'id': flavor.get('id'),
            'disk': flavor.get('disk'),
            'vcpus': flavor.get('vcpus'),
            'ram': flavor.get('ram'),
            'ephemeral': flavor.get('OS-FLV-EXT-DATA:ephemeral'),
            'swap': 0 if flavor.get('swap') == '' else flavor.get('swap')
        }

    def collect_server_flavor_metrics(self, server_details, flavors, tags=None, use_shortname=False):
        tags = tags or []
        tags = copy.deepcopy(tags)
        tags.append("nova_managed_server")
        hypervisor_hostname = server_details.get('hypervisor_hostname')
        host_tags = self._get_host_aggregate_tag(hypervisor_hostname, use_shortname=use_shortname)
        host_tags.append('availability_zone:{}'.format(server_details.get('availability_zone', 'NA')))
        self.external_host_tags[server_details.get('server_name')] = host_tags

        server_id = server_details.get('server_id')
        server_name = server_details.get('server_name')
        hypervisor_hostname = server_details.get('hypervisor_hostname')
        project_name = server_details.get('project_name')

        flavor_id = server_details.get('flavor_id')
        if flavor_id and flavors:
            # Available until version 2.46
            flavor = flavors.get(flavor_id)
        else:
            # New in version 2.47
            flavor = server_details.get('flavor')
        if not flavor:
            return

        if project_name:
            tags.append("project_name:{}".format(project_name))
        if hypervisor_hostname:
            tags.append("hypervisor:{}".format(hypervisor_hostname))
        if server_name:
            tags.append("server_name:{}".format(server_name))

        self.gauge("openstack.nova.server.flavor.disk", flavor.get('disk'),
                   tags=tags + host_tags, hostname=server_id)
        self.gauge("openstack.nova.server.flavor.vcpus", flavor.get('vcpus'),
                   tags=tags + host_tags, hostname=server_id)
        self.gauge("openstack.nova.server.flavor.ram", flavor.get('ram'),
                   tags=tags + host_tags, hostname=server_id)
        self.gauge("openstack.nova.server.flavor.ephemeral", flavor.get('ephemeral'),
                   tags=tags + host_tags, hostname=server_id)
        self.gauge("openstack.nova.server.flavor.swap", flavor.get('swap'),
                   tags=tags + host_tags, hostname=server_id)

    def init_api(self, instance):
        custom_tags = instance.get('tags', [])
        if custom_tags is None:
            custom_tags = []
        if self.instance_name not in self._apis:
            # We are missing the entire instance api either because it is the first time we initialize it or because
            # authentication previously failed and got removed from the cache
            # Let's populate it now
            try:
                self.log.debug("initialize API for instance: {}".format(instance))
                self._apis[self.instance_name] = ApiFactory.create(self.log, self.proxy_config, instance)
                self.service_check(
                    self.IDENTITY_API_SC,
                    AgentCheck.OK,
                    tags=["keystone_server: {}".format(self.keystone_server_url)] + custom_tags,
                )
            except KeystoneUnreachable as e:
                self.log.warning("The agent could not contact the specified identity server at {} . "
                                 "Are you sure it is up at that address?".format(self.keystone_server_url))
                self.log.debug("Problem grabbing auth token: %s", e)
                self.service_check(
                    self.IDENTITY_API_SC,
                    AgentCheck.CRITICAL,
                    tags=["keystone_server: {}".format(self.keystone_server_url)] + custom_tags,
                )

                # If Keystone is down/unreachable, we default the
                # Nova and Neutron APIs to UNKNOWN since we cannot access the service catalog
                self.service_check(
                    self.NETWORK_API_SC,
                    AgentCheck.UNKNOWN,
                    tags=["keystone_server: {}".format(self.keystone_server_url)] + custom_tags,
                )
                self.service_check(
                    self.COMPUTE_API_SC,
                    AgentCheck.UNKNOWN,
                    tags=["keystone_server: {}".format(self.keystone_server_url)] + custom_tags,
                )

            except MissingNovaEndpoint as e:
                self.warning("The agent could not find a compatible Nova endpoint in your service catalog!")
                self.log.debug("Failed to get nova endpoint for response catalog: %s", e)
                self.service_check(
                    self.COMPUTE_API_SC,
                    AgentCheck.CRITICAL,
                    tags=["keystone_server: {}".format(self.keystone_server_url)] + custom_tags,
                )

            except MissingNeutronEndpoint:
                self.warning("The agent could not find a compatible Neutron endpoint in your service catalog!")
                self.service_check(
                    self.NETWORK_API_SC,
                    AgentCheck.CRITICAL,
                    tags=["keystone_server: {}".format(self.keystone_server_url)] + custom_tags,
                )

        if self.instance_name not in self._apis:
            # Fast fail in the absence of an api
            raise IncompleteConfig()

    @traced
    def check(self, instance):
        # have we been backed off
        if not self._backoff.should_run(instance):
            self.log.info('Skipping run due to exponential backoff in effect')
            return

        self.keystone_server_url = instance.get("keystone_server_url")
        if not self.keystone_server_url:
            raise IncompleteConfig()
        self.proxy_config = self.get_instance_proxy(instance, self.keystone_server_url)
        self.ssl_verify = is_affirmative(instance.get("ssl_verify", True))
        exclude_network_id_patterns = set(instance.get('exclude_network_ids', []))
        self.exclude_network_id_rules = [re.compile(ex) for ex in exclude_network_id_patterns]
        exclude_server_id_patterns = set(instance.get('exclude_server_ids', []))
        self.exclude_server_id_rules = [re.compile(ex) for ex in exclude_server_id_patterns]
        include_project_name_patterns = set(instance.get('whitelist_project_names', []))
        self.include_project_name_rules = [re.compile(ex) for ex in include_project_name_patterns]
        exclude_project_name_patterns = set(instance.get('blacklist_project_names', []))
        self.exclude_project_name_rules = [re.compile(ex) for ex in exclude_project_name_patterns]

        custom_tags = instance.get("tags", [])
        collect_project_metrics = is_affirmative(instance.get('collect_project_metrics', True))
        collect_hypervisor_metrics = is_affirmative(instance.get('collect_hypervisor_metrics', True))
        collect_hypervisor_load = is_affirmative(instance.get('collect_hypervisor_load', True))
        collect_network_metrics = is_affirmative(instance.get('collect_network_metrics', True))
        collect_server_diagnostic_metrics = is_affirmative(instance.get('collect_server_diagnostic_metrics', True))
        collect_server_flavor_metrics = is_affirmative(instance.get('collect_server_flavor_metrics', True))
        use_shortname = is_affirmative(instance.get('use_shortname', False))
        service_check_tags = ["keystone_server: {}".format(self.keystone_server_url)] + custom_tags

        try:
            self.instance_name = get_instance_name(instance)
            # Initialize API
            self.init_api(instance)
            # TODO the below cache is not configured per instance as of today
            # Re initialize it in order to prevent cross instance contamination
            self._aggregate_list = {}

            # List projects and filter them and submit service check for compute service
            projects = []
            try:
                projects = self.get_projects(self.include_project_name_rules, self.exclude_project_name_rules)
                self.service_check(self.COMPUTE_API_SC, AgentCheck.OK, tags=service_check_tags)
            except (requests.exceptions.HTTPError, requests.exceptions.Timeout, requests.exceptions.ConnectionError,
                    AuthenticationNeeded, InstancePowerOffFailure):
                self.service_check(self.COMPUTE_API_SC, AgentCheck.CRITICAL, tags=service_check_tags)

            if collect_project_metrics:
                for name, project in iteritems(projects):
                    self.collect_project_limit(project, custom_tags)

            self.collect_hypervisors_metrics(custom_tags=custom_tags,
                                             use_shortname=use_shortname,
                                             collect_hypervisor_metrics=collect_hypervisor_metrics,
                                             collect_hypervisor_load=collect_hypervisor_load)

            if collect_server_diagnostic_metrics or collect_server_flavor_metrics:
                # This updates the server cache directly
                tenant_id_to_name = {}
                for name, p in iteritems(projects):
                    tenant_id_to_name[p.get('id')] = name
                self.get_all_servers(tenant_id_to_name, self.instance_name)

                servers = self.servers_cache[self.instance_name]['servers']
                if collect_server_diagnostic_metrics:
                    self.log.debug("Fetch stats from %s server(s)" % len(servers))
                    for _, server in iteritems(servers):
                        self.collect_server_diagnostic_metrics(server, tags=custom_tags,
                                                               use_shortname=use_shortname)
                if collect_server_flavor_metrics:
                    if len(servers) >= 1 and 'flavor_id' in next(itervalues(servers)):
                        self.log.debug("Fetch server flavors")
                        # If flavors are not part of servers detail (new in version 2.47) then we need to fetch them
                        flavors = self.get_flavors()
                    else:
                        flavors = None
                    for _, server in iteritems(servers):
                        self.collect_server_flavor_metrics(server, flavors, tags=custom_tags,
                                                           use_shortname=use_shortname)

            if collect_network_metrics:
                # Collect network metrics and submit network service check
                try:
                    self.collect_networks_metrics(custom_tags)
                    self.service_check(self.NETWORK_API_SC, AgentCheck.OK, tags=service_check_tags)
                except (requests.exceptions.HTTPError, requests.exceptions.Timeout, requests.exceptions.ConnectionError,
                        AuthenticationNeeded, InstancePowerOffFailure):
                    self.service_check(self.NETWORK_API_SC, AgentCheck.CRITICAL, tags=service_check_tags)

            if set_external_tags is not None:
                set_external_tags(self.get_external_host_tags())

        except IncompleteConfig as e:
            if isinstance(e, IncompleteIdentity):
                self.warning(
                    "Please specify the user via the `user` variable in your init_config.\n"
                    + "This is the user you would use to authenticate with Keystone v3 via password auth.\n"
                    + "The user should look like:"
                    + "{'password': 'my_password', 'name': 'my_name', 'domain': {'id': 'my_domain_id'}}"
                )
            else:
                self.warning("Configuration Incomplete! Check your openstack.yaml file")
        except AuthenticationNeeded:
            # Delete the api, we'll populate a new one on the next run for this instance
            del self._apis[self.instance_name]
        except (requests.exceptions.HTTPError, requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code < 500:
                self.warning("Error reaching nova API: %s" % e)
            else:
                # exponential backoff
                self.do_backoff(instance)
                return

        self._backoff.reset_backoff(instance)

    def do_backoff(self, instance):
        backoff_interval, retries = self._backoff.do_backoff(instance)
        tags = instance.get('tags', [])

        self.gauge("openstack.backoff.interval", backoff_interval, tags=tags)
        self.gauge("openstack.backoff.retries", retries, tags=tags)
        self.warning("There were some problems reaching the nova API - applying exponential backoff")

    # Cache util
    def _is_expired(self, entry):
        assert entry in ["aggregates", "physical_hosts", "hypervisors"]
        ttl = self.CACHE_TTL.get(entry)
        last_fetch_time = getattr(self, self.FETCH_TIME_ACCESSORS.get(entry))
        return datetime.now() - last_fetch_time > timedelta(seconds=ttl)

    def _get_and_set_aggregate_list(self):
        if not self._aggregate_list or self._is_expired("aggregates"):
            self._aggregate_list = self.get_all_aggregate_hypervisors()
            self._last_aggregate_fetch_time = datetime.now()

        return self._aggregate_list

    def _get_host_aggregate_tag(self, hyp_hostname, use_shortname=False):
        tags = []
        hyp_hostname = hyp_hostname.split('.')[0] if use_shortname else hyp_hostname
        if hyp_hostname in self._get_and_set_aggregate_list():
            tags.append('aggregate:{}'.format(self._aggregate_list[hyp_hostname].get('aggregate', "unknown")))
            # Need to check if there is a value for availability_zone
            # because it is possible to have an aggregate without an AZ
            try:
                if self._aggregate_list[hyp_hostname].get('availability_zone'):
                    availability_zone = self._aggregate_list[hyp_hostname]['availability_zone']
                    tags.append('availability_zone:{}'.format(availability_zone))
            except KeyError:
                self.log.debug('Unable to get the availability_zone for hypervisor: {}'.format(hyp_hostname))
        else:
            self.log.info('Unable to find hostname %s in aggregate list. Assuming this host is unaggregated',
                          hyp_hostname)

        return tags

    # For attaching tags to hosts that are not the host running the agent
    def get_external_host_tags(self):
        """ Returns a list of tags for every guest server that is detected by the OpenStack
        integration.
        List of pairs (hostname, list_of_tags)
        """
        self.log.debug("Collecting external_host_tags now")
        external_host_tags = []
        for k, v in iteritems(self.external_host_tags):
            external_host_tags.append((k, {SOURCE_TYPE: v}))

        self.log.debug("Sending external_host_tags: %s", external_host_tags)
        return external_host_tags

    # Nova Proxy methods
    def get_os_hypervisor_uptime(self, hyp_id):
        return self._apis.get(self.instance_name).get_os_hypervisor_uptime(hyp_id)

    def get_os_aggregates(self):
        return self._apis.get(self.instance_name).get_os_aggregates()

    def get_os_hypervisors_detail(self):
        return self._apis.get(self.instance_name).get_os_hypervisors_detail()

    def get_servers_detail(self, query_params):
        return self._apis.get(self.instance_name).get_servers_detail(query_params)

    def get_server_diagnostics(self, server_id):
        return self._apis.get(self.instance_name).get_server_diagnostics(server_id)

    def get_project_limits(self, tenant_id):
        return self._apis.get(self.instance_name).get_project_limits(tenant_id)

    def get_flavors_detail(self, query_params):
        return self._apis.get(self.instance_name).get_flavors_detail(query_params)

    # Keystone Proxy Methods
    def get_projects(self, include_project_name_rules, exclude_project_name_rules):
        projects = self._apis.get(self.instance_name).get_projects()
        project_by_name = {}
        for project in projects:
            name = project.get('name')
            project_by_name[name] = project
        filtered_project_names = pattern_filter([p for p in project_by_name],
                                                whitelist=include_project_name_rules,
                                                blacklist=exclude_project_name_rules)
        result = {name: v for (name, v) in iteritems(project_by_name) if name in filtered_project_names}
        return result

    # Neutron Proxy Methods
    def get_networks(self):
        return self._apis.get(self.instance_name).get_networks()
