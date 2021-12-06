#
# (C) Copyright IBM Corp. 2021
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import concurrent.futures as cf
import inspect
import logging
import re
import threading
import time
from uuid import uuid4

from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_platform_services import GlobalSearchV2, GlobalTaggingV1
from ibm_vpc import VpcV1
from ray.autoscaler._private.cli_logger import cli_logger
from ray.autoscaler.node_provider import NodeProvider
from ray.autoscaler.tags import (TAG_RAY_CLUSTER_NAME, TAG_RAY_LAUNCH_CONFIG,
                                 TAG_RAY_NODE_KIND, TAG_RAY_NODE_NAME)

logger = logging.getLogger(__name__)

INSTANCE_NAME_UUID_LEN = 8
INSTANCE_NAME_MAX_LEN = 64
PENDING_TIMEOUT = 120

# TODO: move to constants
PROFILE_NAME_DEFAULT = 'cx2-2x4'
VOLUME_TIER_NAME_DEFAULT = 'general-purpose'
RAY_RECYCLABLE = 'ray-recyclable'

ALL_STATUS_TAGS = ['ray-node-status:uninitialized', 'ray-node-status:waiting-for-ssh', 'ray-node-status:setting-up',
                   'ray-node-status:syncing-files', 'ray-node-status:up-to-date', 'ray-node-status:update-failed']
RETRIES = 10
WORKER = 'worker'
HEAD = 'head'


def _create_vpc_client(endpoint, authenticator):
    """
    Creates an IBM VPC python-sdk instance
    """
    ibm_vpc_client = VpcV1('2021-01-19', authenticator=authenticator)
    ibm_vpc_client.set_service_url(endpoint + '/v1')

    return ibm_vpc_client


class Gen2NodeProvider(NodeProvider):

    def retry_on_except(func):
        def decorated_func(*args, **kwargs):
            name = func.__name__
            e = None
            for retry in range(RETRIES):
                try:
                    result = func(*args, **kwargs)
                    return result
                except Exception as e:
                    msg = f"Error in {name}, {e}, retries left {RETRIES - retry}"
                    cli_logger.error(msg)
                    logger.exception(msg)

                    logger.info("reiniting clients and waiting few seconds")
                    args[0]._init()
                    time.sleep(1)

            # we got here only because run out of retries
            # TODO: consider to return default empty instead of raising. raising may kill monitor
            raise e
        return decorated_func

    # TODO: To be removed, needed for debugging purposes only
    def log_in_out(func):

        def decorated_func(*args, **kwargs):
            name = func.__name__
            logger.info(
                f"Enter {name} from {inspect.stack()[0][3]} {inspect.stack()[1][3]}  {inspect.stack()[2][3]} with args: {args} and kwargs {kwargs}")
            try:
                result = func(*args, **kwargs)
                logger.info(
                    f"Leave {name} from {inspect.stack()[1][3]} with result {result}, entered with args: {args}")
            except:
                cli_logger.error(f"Error in {name}")
                raise
            return result
        return decorated_func

    @log_in_out
    def _init(self):
        with self.lock:
            self.ibm_vpc_client = _create_vpc_client(
                self.endpoint, IAMAuthenticator(self.iam_api_key))
            self.global_tagging_service = GlobalTaggingV1(
                IAMAuthenticator(self.iam_api_key))
            self.global_search_service = GlobalSearchV2(
                IAMAuthenticator(self.iam_api_key))

    def __init__(self, provider_config, cluster_name):
        NodeProvider.__init__(self, provider_config, cluster_name)

        self.lock = threading.RLock()

        self.endpoint = self.provider_config["endpoint"]
        self.iam_api_key = self.provider_config["iam_api_key"]

        self._init()

        # Cache of node objects from the last nodes() call
        self.cached_nodes = {}
        self.pending_nodes = {}
        self.prev_nodes = {HEAD: set()}
        self.deleted_nodes = []

        self.cache_stopped_nodes = provider_config.get(
            "cache_stopped_nodes", True)

    def _get_node_type(self, name):
        if f'{self.cluster_name}-{WORKER}' in name:
            return WORKER
        elif f'{self.cluster_name}-{HEAD}' in name:
            return HEAD

    @log_in_out
    @retry_on_except
    def non_terminated_nodes(self, tag_filters):
        logger.info(f'---------------------------------------in non_terminated_nodes')
        logger.info(f'TAG_FILTERS: {tag_filters}')
        logger.info(f'---------------------------------------')

        # get all nodes tagged for cluster "query":"tags:\"ray-cluster-name:default\" AND tags:\"ray-node-type:head\""
        CLUSTER_NAME_QUERY = f"tags:\"{TAG_RAY_CLUSTER_NAME}:{self.cluster_name}\""
        query = CLUSTER_NAME_QUERY
        for k, v in sorted(tag_filters.items()):
            query += f" AND tags:\"{k}:{v}\""

        with self.lock:
            # retry search few times in case node misteriously dissapeared
            logger.info(f'in non_terminated_nodes')
            NT_RETRIES = 20
            missing_nodes = []
            retry = 0
            while retry < NT_RETRIES:
                retry > 0 and logger.info(
                    f'nt_retry #{retry}, self.global_search_service.search with query {query}')
                retry += 1
                result = self.global_search_service.search(
                    query=query, fields=['*'], limit=1000).get_result()

                items = result['items']
                nodes = []

                # update objects with floating ips.
                for node in items:

                    # check if node marked for delete
                    if node['resource_id'] in self.deleted_nodes:
                        logger.info(
                            f"{node['resource_id']} been scheduled for delete")
                        continue

                    # TODO: should be special case here in case of cache_stopped_nodes?
                    if node['doc']['status'] not in ['pending', 'starting', 'running']:
                        logger.info(
                            f"{node['resource_id']} status {node['doc']['status']} not in ['pending', 'starting', 'running'], skipping")
                        continue

                    instance_id = node['resource_id']
                    try:
                        # this one is needed to filter out zombie resources
                        self.ibm_vpc_client.get_instance(instance_id)
                    except Exception as e:
                        cli_logger.warning(instance_id)
                        if e.message == 'Instance not found':
                            logger.error(
                                f"failed to find instance {instance_id}, skipping")
                            continue
                        logger.error(
                            f"failed to find instance {instance_id}, raising")
                        raise e

                    node_type = self._get_node_type(node['name'])
                    if node_type == 'head':
                        nic_id = node['doc']['network_interfaces'][0]['id']
                        res = self.ibm_vpc_client.list_instance_network_interface_floating_ips(
                            instance_id, nic_id).get_result()

                        floating_ips = res['floating_ips']
                        if len(floating_ips) == 0:
                            # not adding a head node that is missing floating ip
                            continue
                        else:
                            # currently head node should always have floating ip.
                            # in case floating ip present we want to add it
                            node['floating_ips'] = floating_ips
                    nodes.append(node)

                found_nodes = {node["resource_id"]: node for node in nodes}

                # TODO: remove all the tag search retries once tagging issue resolved
                prev_nodes_keys = []

                # we manage a single set of HEAD node
                if HEAD in query:
                    prev_nodes_keys = self.prev_nodes.get(HEAD, set())

                # and set of nodes unique for each query
                else:
                    prev_nodes_keys = self.prev_nodes.get(query, set())
                    if not prev_nodes_keys:
                        self.prev_nodes[query] = prev_nodes_keys

                # check if previously cached node present in the list of found nodes
                if prev_nodes_keys - found_nodes.keys():

                    # TODO: bug in ray when called to refresh head ip with wrong unintialized tags after head node already created
                    if "ray-node-status:uninitialized" in query and "ray-node-type:head" in query:
                        break

                    # nodes shouldn't dissapear too frequently. if happened, most likely it is a glitch in search, retry
                    # save currently missing nodes
                    if missing_nodes and sorted(prev_nodes_keys - found_nodes.keys()) != missing_nodes:
                        logger.info(
                            f'missing nodes detected in previous retry don\'t match currently missing nodes')
                        # TODO: Is there a chance for infinite loop here?
                        retry = 0

                    missing_nodes = sorted(
                        prev_nodes_keys - found_nodes.keys())

                    logger.warning(
                        f'{prev_nodes_keys - found_nodes.keys()} from perviously cached node missing in the list of found_nodes {found_nodes.keys()} found using query {query}, retrying search, retries left: {NT_RETRIES - retry}')
                    time.sleep(3)
                    continue
                else:
                    logger.info("found nodes not missing any perviously cached nodes")
                    missing_nodes = []

                    bad_tags = False
                    for node in nodes:
#                        if not any("ray-node-status" in t for t in node['tags']):
#                            logger.warning(
#                                f'{node["resource_id"]} is missing ray-node-status in tags: {node["tags"]}, retrying')
#                            bad_tags = True
#                            break
                        if 'ray-node-type:head' in node['tags']:
                            self.prev_nodes[HEAD].add(node["resource_id"])
                        else:
                            self.prev_nodes[query].add(node["resource_id"])
                    if bad_tags:
                        continue

                    break

            # in case one or more nodes dissapeared, update caches
            for n in missing_nodes:
                logger.info(f'node {n} dissapeared, updating caches')
                self.prev_nodes[HEAD].discard(n)
                self.prev_nodes[query].discard(n)
                self.cached_nodes.pop(n, None)

            for key in found_nodes.keys():
                self.cached_nodes[key] = found_nodes[key]

            node_ids = [node["resource_id"] for node in nodes]

            # workaround for tagging + create not atomic
            for pend_id in list(self.pending_nodes.keys()):
                # check if instance already been found by tag search
                if pend_id in found_nodes:
                    if found_nodes[pend_id]['doc']['status'] == 'running':
                        #the instance seems to be running, lets remove it from pendings
                        self.pending_nodes.pop(pend_id, None)
                else:
                    # if instance in pendings for more than PENDING_TIMEOUT it is time to delete it
                    pending_time = self.pending_nodes[pend_id] - time.time()
                    logger.info(f'the instance {pend_id} is in pendings list for {pending_time}')
                    if pending_time > PENDING_TIMEOUT:
                        logger.error(f'pending timeout {PENDING_TIMEOUT} reached, deleting instance {pend_id}')
                        self._delete_node(pend_id)
                        self.pending_nodes.pop(pend_id)
                    elif pend_id not in node_ids:
                        # add it to the list of node ids. it is missing tags so it is not going to be updated
                        node_ids.append(pend_id)
                        self.cached_nodes[pend_id] = self.pending_nodes[pend_id]
        
        return node_ids

    @log_in_out
    def is_running(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return node['doc']['status'] == 'running'

    @log_in_out
    def is_terminated(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            state = node['doc']['status']
            return state not in ["running", "starting", "pending"]

    def _tags_to_dict(self, node):
        tags_dict = {}
        if self.cluster_name in node['name']:
            tags_dict[TAG_RAY_CLUSTER_NAME] = self.cluster_name

            if f'{self.cluster_name}-{WORKER}' in node['name']:
                tags_dict[TAG_RAY_NODE_KIND] = WORKER
            else:
                tags_dict[TAG_RAY_NODE_KIND] = HEAD

            tags_dict[TAG_RAY_NODE_NAME] = node['name']

            for tag in node['tags']:
                _tags_split = tag.split(':')
                tags_dict[_tags_split[0]] = _tags_split[1]

        return tags_dict

    @log_in_out
    def node_tags(self, node_id):
        with self.lock:
            node = self._get_cached_node(node_id)
            return self._tags_to_dict(node)

    def _get_hybrid_ip(self, node_id):
        node = self._get_cached_node(node_id)
        node_type = self._get_node_type(node['name'])
        if node_type == 'head':
            fip = node.get("floating_ips")
            if fip:
                return fip[0]['address']

            node = self._get_node(node_id)
            fip = node.get("floating_ips")
            if fip:
                return fip[0]['address']
        else:
            return self.internal_ip(node_id)

    @log_in_out
    def external_ip(self, node_id):
        with self.lock:
            if self.provider_config.get('use_hybrid_ips'):
                return self._get_hybrid_ip(node_id)

            node = self._get_cached_node(node_id)

            fip = node.get("floating_ips")
            if fip:
                return fip[0]['address']

            node = self._get_node(node_id)
            fip = node.get("floating_ips")
            if fip:
                return fip[0]['address']

    def internal_ip(self, node_id):
        node = self._get_cached_node(node_id)

        try:
            primary_ipv4_address = node['doc']['network_interfaces'][0].get(
                'primary_ipv4_address')
            if primary_ipv4_address is None:
                node = self._get_node(node_id)
        except Exception:
            node = self._get_node(node_id)

        return node['doc']['network_interfaces'][0].get('primary_ipv4_address')

    @log_in_out
    def _global_tagging_with_retries(self, resource_crn, f_name, tags=None):
        e = None
        resource_model = {'resource_id': resource_crn}
        for retry in range(RETRIES):
            if retry > 0:
                logger.info(f'retry #{retry} to {f_name} {resource_crn}')
            try:
                if f_name == 'attach_tag':
                    tag_results = self.global_tagging_service.attach_tag(
                        resources=[resource_model],
                        tag_names=tags,
                        tag_type='user').get_result()
                    if not tag_results['results'][0]['is_error']:
                        node_tags = [t['name'] for t in self._global_tagging_with_retries(
                            resource_crn, 'list_tags')]

                        if set(tags) - set(node_tags):
                            logger.warning(
                                f'instance {resource_crn} missing attached tags {set(tags) - set(node_tags)}, retrying')
                            continue
                        else:
                            return tag_results
                elif f_name == 'list_tags':
                    return self.global_tagging_service.list_tags(attached_to=resource_crn).get_result()['items']
                else:
                    # detach
                    tag_results = self.global_tagging_service.detach_tag(
                        resources=[resource_model],
                        tag_names=tags,
                        tag_type='user').get_result()
                    if not tag_results['results'][0]['is_error']:
                        node_tags = [t['name'] for t in self._global_tagging_with_retries(
                            resource_crn, 'list_tags')]

                        if set(node_tags) & set(tags):
                            logger.warning(
                                f'instance {resource_crn} still having dettached tags {set(node_tags) & set(tags)}, retrying')
                        else:
                            return tag_results

                logger.error(f"failed to {f_name} tags {tags} for {resource_model}, {tag_results}")
            except Exception as e:
                logger.error(f"Failed to {f_name} {tags} for {resource_model}, {e}")

            if retry == RETRIES / 2:
                # try to reinit client
                self.global_tagging_service = GlobalTaggingV1(IAMAuthenticator(self.iam_api_key))

            time.sleep(5)

        # if exception occured and reinit didn't help, raise exception
        if e:
            raise e
        else:
            return False

    @log_in_out
    def set_node_tags(self, node_id, tags):
        node = self._get_node(node_id)
        with self.lock:
            resource_crn = node['crn']

            # 1. convert tags to gen2 format
            new_tags = [f"{k}:{v}" for k, v in tags.items()]

            # 2. attach new tags
            if not self._global_tagging_with_retries(resource_crn, 'attach_tag', tags=new_tags):
                cli_logger.error(
                    f"failed to tag node {node_id} with tags {new_tags}, raising error")
                raise

            logger.info(f"attached {new_tags}")

            # 3. check that status is been updated and detach old statuses
            if 'ray-node-status' in tags.keys():
                tags_to_detach = ALL_STATUS_TAGS.copy()
                # remove updated status from tags_to_detach
                tags_to_detach.remove(f"ray-node-status:{tags['ray-node-status']}")

                # detach any status tags not appearing in new tags
                if not self._global_tagging_with_retries(resource_crn, 'detach_tag', tags=tags_to_detach):
                    logger.error("failed to detach old status tags")
                    raise

                logger.info(f"detached {tags_to_detach}")
                time.sleep(1)

            return {}

    def _get_instance_data(self, name):
        """
        Returns the instance information
        """
        instances_data = self.ibm_vpc_client.list_instances(name=name).get_result()
        if len(instances_data['instances']) > 0:
            return instances_data['instances'][0]
        return None

    def _create_instance(self, name, base_config):
        """
        Creates a new VM instance

        TODO: consider to use gen2 template file instead of generating dict
        """
        logger.info("Creating new VM instance {}".format(name))

        security_group_identity_model = {'id': base_config['security_group_id']}
        subnet_identity_model = {'id': base_config['subnet_id']}
        primary_network_interface = {
            'name': 'eth0',
            'subnet': subnet_identity_model,
            'security_groups': [security_group_identity_model]
        }

        boot_volume_profile = {
            'capacity': base_config.get('boot_volume_capacity', 100),
            'name': '{}-boot'.format(name),
            'profile': {'name': base_config.get('volume_tier_name', VOLUME_TIER_NAME_DEFAULT)}}

        boot_volume_attachment = {
            'delete_volume_on_instance_delete': True,
            'volume': boot_volume_profile
        }

        key_identity_model = {'id': base_config['key_id']}
        profile_name = base_config.get('instance_profile_name', PROFILE_NAME_DEFAULT)

        instance_prototype = {}
        instance_prototype['name'] = name
        instance_prototype['keys'] = [key_identity_model]
        instance_prototype['profile'] = {'name': profile_name}
        instance_prototype['resource_group'] = {'id': base_config['resource_group_id']}
        instance_prototype['vpc'] = {'id': base_config['vpc_id']}
        instance_prototype['image'] = {'id': base_config['image_id']}

        instance_prototype['zone'] = {'name': self.provider_config['zone_name']}
        instance_prototype['boot_volume_attachment'] = boot_volume_attachment
        instance_prototype['primary_network_interface'] = primary_network_interface

        try:
            resp = self.ibm_vpc_client.create_instance(instance_prototype)
        except ApiException as e:
            if e.code == 400 and 'already exists' in e.message:
                return self._get_instance_data(name)
            elif e.code == 400 and 'over quota' in e.message:
                cli_logger.error("Create VM instance {} failed due to quota limit"
                                 .format(name))
            else:
                cli_logger.error("Create VM instance {} failed with status code {}"
                                 .format(name, str(e.code)))
            raise e

        logger.info("VM instance {} created successfully ".format(name))

        return resp.result

    def _create_floating_ip(self, base_config):
        """
        Creates or attaches floating IP address
        """
        if base_config.get('head_ip'):
            for ip in self.ibm_vpc_client.list_floating_ips().get_result()['floating_ips']:
                if ip['address'] == base_config['head_ip']:
                    return ip

        floating_ip_name = '{}-{}'.format(RAY_RECYCLABLE, uuid4().hex[:4])

        logger.info('Creating floating IP {}'.format(floating_ip_name))
        floating_ip_prototype = {}
        floating_ip_prototype['name'] = floating_ip_name
        floating_ip_prototype['zone'] = {
            'name': self.provider_config['zone_name']}
        floating_ip_prototype['resource_group'] = {
            'id': base_config['resource_group_id']}
        response = self.ibm_vpc_client.create_floating_ip(
            floating_ip_prototype)
        floating_ip_data = response.result

        return floating_ip_data

    def _attach_floating_ip(self, instance, fip_data):
        fip = fip_data['address']
        fip_id = fip_data['id']

        logger.info('Attaching floating IP {} to VM instance {}'.format(
            fip, instance['id']))

        # we need to check if floating ip is not attached already. if not, attach it to instance
        instance_primary_ni = instance['primary_network_interface']

        if instance_primary_ni['primary_ipv4_address'] and instance_primary_ni['id'] == fip_id:
            # floating ip already attached. do nothing
            logger.info('Floating IP {} already attached to eth0'.format(fip))
        else:
            self.ibm_vpc_client.add_instance_network_interface_floating_ip(
                instance['id'], instance['network_interfaces'][0]['id'], fip_id)

    def _stopped_nodes(self, tags):
        # get all nodes tagged for cluster "query":"tags:\"ray-cluster-name:default\" AND tags:\"ray-node-type:head\""
        query = f"tags:\"{TAG_RAY_CLUSTER_NAME}:{self.cluster_name}\""
        query += f" AND tags:\"{TAG_RAY_NODE_KIND}:{tags[TAG_RAY_NODE_KIND]}\""
        query += f" AND tags:\"{TAG_RAY_LAUNCH_CONFIG}:{tags[TAG_RAY_LAUNCH_CONFIG]}\""

        with self.lock:
            try:
                result = self.global_search_service.search(
                    query=query, fields=['*']).get_result()
            except ApiException as e:
                cli_logger.warning(
                    f"failed to query global search service with message: {e.message}, reiniting now")
                self.global_search_service = GlobalSearchV2(
                    IAMAuthenticator(self.iam_api_key))
                result = self.global_search_service.search(
                    query=query, fields=['*']).get_result()

            items = result['items']
            nodes = []

            # filter instances by state (stopped,stopping)
            for node in items:
                instance_id = node['resource_id']
                try:
                    instance = self.ibm_vpc_client.get_instance(
                        instance_id).result
                    state = instance['status']
                    if state in ["stopped", "stopping"]:
                        nodes.append(node)
                except Exception as e:
                    cli_logger.warning(instance_id)
                    if e.message == 'Instance not found':
                        continue
                    raise e
            return nodes

    def _wait_running(self, instance_id):
        # if instance in pendings for more than PENDING_TIMEOUT it is time to delete it and raise
        start = time.time()

        instance = self.ibm_vpc_client.get_instance(instance_id).result

        while instance['status'] != 'running':
            time.sleep(1)
            instance = self.ibm_vpc_client.get_instance(instance_id).result

            if time.time() - start > PENDING_TIMEOUT:
                logger.error(f'pending timeout {PENDING_TIMEOUT} reached, deleting instance {instance_id}')
                self._delete_node(instance_id)
                raise Exception(
                    f'Instance {instance_id} been hanging in pending state too long, deleting')

    def _create_node(self, base_config, tags):
        name_tag = tags[TAG_RAY_NODE_NAME]
        assert (len(name_tag) <=
                (INSTANCE_NAME_MAX_LEN - INSTANCE_NAME_UUID_LEN - 1)) and re.match("^[a-z0-9-:-]*$", name_tag), (
                    name_tag, len(name_tag))

        name = "{name_tag}-{uuid}".format(
            name_tag=name_tag,
            uuid=uuid4().hex[:INSTANCE_NAME_UUID_LEN])

        # create instance in vpc
        instance = self._create_instance(name, base_config)

        # unfortunatelly, currently create and tag is not an atomic operation
        # adding instance to cache to be returned by non_terminated_nodes but with no tags
        self.pending_nodes[instance['id']] = instance

        # currently always creating public ip for head node
        if self._get_node_type(name) == 'head':
            fip_data = self._create_floating_ip(base_config)
            self._attach_floating_ip(instance, fip_data)

        self._wait_running(instance['id'])

        tags[TAG_RAY_CLUSTER_NAME] = self.cluster_name
        tags[TAG_RAY_NODE_NAME] = name

        new_tags = [f"{k}:{v}" for k, v in tags.items()]
        if not self._global_tagging_with_retries(instance['crn'], 'attach_tag', tags=new_tags):
            cli_logger.error(
                f"failed to tag node {instance['id']} with tags {new_tags}, raising error")
            raise

        logger.info(f"attached {new_tags}")

        return {instance['id']: instance}

    @log_in_out
    def create_node(self, base_config, tags, count) -> None:
        with self.lock:

            # here will be implementation of reuse of stopped instances
            stopped_nodes_dict = {}
            futures = []

            # Try to reuse previously stopped nodes with compatible configs
            if self.cache_stopped_nodes:
                stopped_nodes = self._stopped_nodes(tags)
                stopped_nodes_ids = [n['resource_id'] for n in stopped_nodes]
                stopped_nodes_dict = {n['resource_id']: n for n in stopped_nodes}

                if stopped_nodes:
                    cli_logger.print(f"Reusing nodes {stopped_nodes_ids}. "
                                     "To disable reuse, set `cache_stopped_nodes: False` "
                                     "under `provider` in the cluster configuration.")

                for node in stopped_nodes:
                    logger.info(f"Starting instance {node['resource_id']}")
                    self.ibm_vpc_client.create_instance_action(node['resource_id'], 'start')

                time.sleep(1)

                for node_id in stopped_nodes_ids:
                    self.set_node_tags(node_id, tags)

                count -= len(stopped_nodes_ids)

            created_nodes_dict = {}

            with cf.ThreadPoolExecutor(count) as ex:
                for i in range(count):
                    futures.append(
                        ex.submit(self._create_node, base_config, tags))

            for future in cf.as_completed(futures):
                created_node = future.result()
                created_nodes_dict.update(created_node)

            all_created_nodes = stopped_nodes_dict
            all_created_nodes.update(created_nodes_dict)
            return all_created_nodes

    def _delete_node(self, resource_id):
        logger.info(f'in _delete_node with id {resource_id}')
        try:
            floating_ips = []

            try:
                node = self._get_node(resource_id)
                floating_ips = node.get('floating_ips', [])
            except:
                pass

            self.ibm_vpc_client.delete_instance(resource_id)

            for ip in floating_ips:
                # TODO: replace prefix with tags
                if ip['name'].startswith(RAY_RECYCLABLE):self.ibm_vpc_client.delete_floating_ip(ip['id'])
        except ApiException as e:
            if e.code == 404:
                pass
            else:
                raise e

    @log_in_out
    def terminate_nodes(self, node_ids):
        if not node_ids:
            return

        futures = []
        with cf.ThreadPoolExecutor(len(node_ids)) as ex:
            for node_id in node_ids:
                logger.info("NodeProvider: {}: Terminating node".format(node_id))
                futures.append(ex.submit(self.terminate_node, node_id))

        for future in cf.as_completed(futures):
            future.result()

    @log_in_out
    def terminate_node(self, node_id):
        """
        Deletes the VM instance and the associated volume
        """
        logger.info("Deleting VM instance {}".format(node_id))

        try:
            if self.cache_stopped_nodes:
                cli_logger.print(f"Stopping instance {node_id}. To terminate instead, set `cache_stopped_nodes: False` "
                                 "under `provider` in the cluster configuration")

                self.ibm_vpc_client.create_instance_action(node_id, 'stop')
            else:
                cli_logger.print(f"Terminating instance {node_id}")
                self._delete_node(node_id)

            with self.lock:
                self.deleted_nodes.append(node_id)

                # and remove it from all caches
                for key in self.prev_nodes.keys():
                    self.prev_nodes[key].discard(node_id)

                self.cached_nodes.pop(node_id, None)
                # self.pending_nodes.pop(node_id, None)
        except ApiException as e:
            if e.code == 404:
                pass
            else:
                raise e

    def _get_node(self, node_id):
        """Refresh and get info for this node, updating the cache."""
        self.non_terminated_nodes({})  # Side effect: updates cache

        with self.lock:
            if node_id in self.cached_nodes:
                return self.cached_nodes[node_id]

            try:
                node = self.ibm_vpc_client.get_instance(node_id).get_result()
            except Exception as e:
                logger.error(f'failed to get instance with id {node_id}')
                raise e
            try:
                node_tags = self._global_tagging_with_retries(node['crn'], 'list_tags')
            except Exception as e:
                logger.error(f'Error listing tags for node {node}: {e}')
                raise e

            node['tags'] = [t['name'] for t in node_tags]
            return node

    def _get_cached_node(self, node_id):
        """Return node info from cache if possible, otherwise fetches it."""
        if node_id in self.cached_nodes:
            return self.cached_nodes[node_id]

        return self._get_node(node_id)

    @staticmethod
    def bootstrap_config(cluster_config):
        return cluster_config
