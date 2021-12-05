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

import os
import click
import tempfile
import inquirer
import yaml
import os.path as path

from ibm_cloud_sdk_core import ApiException
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_platform_services import GlobalSearchV2, GlobalTaggingV1, ResourceManagerV2

from ibm_vpc import VpcV1

from inquirer import errors
import re

RAY_PUBLIC_GATEWAY_NAME = 'ray-cluster-public-gateway'
RAY_VPC_NAME = 'ray-cluster-vpc'


def get_option_from_list(msg, choices, default=None, choice_key='name', do_nothing=None):
    if(len(choices) == 0 and do_nothing==None):
        error_msg = f"There no option for {msg}"
        print(error_msg)
        raise Exception(error_msg)

    if(len(choices) == 1 and not do_nothing):
        return(choices[0])

    choices_keys = [choice[choice_key] for choice in choices]
    if do_nothing:
        choices_keys.insert(0, do_nothing)

    questions = [
            inquirer.List('answer',
                message=msg,
                choices=choices_keys,
                default=default,
            ),]
    answers = inquirer.prompt(questions)

    # now find the object by name in the list
    if answers['answer'] == do_nothing:
        return do_nothing
    else:
        return next((x for x in choices if x[choice_key] == answers['answer']), None)

def validate_not_empty(answers, current):
    if not current:
        raise errors.ValidationError('', reason=f"Key name can't be empty")
    return True

def validate_exists(answers, current):
    if not current or not os.path.exists(os.path.abspath(os.path.expanduser(current))):
        raise errors.ValidationError('', reason=f"File {current} doesn't exist")
    return True

def register_ssh_key(ibm_vpc_client, resource_group_id):
    questions = [
      inquirer.Text('keyname', message='Please specify a name for the new key', validate=validate_not_empty)
    ]
    answers = inquirer.prompt(questions)
    keyname = answers['keyname']

    EXISTING_CONTENTS = 'Paste existing public key contents'
    EXISTING_PATH = 'Provide path to existing public key'
    GENERATE_NEW = 'Generate new public key'

    questions = [
            inquirer.List('answer',
                message="Please choose",
                choices=[EXISTING_PATH, EXISTING_CONTENTS, GENERATE_NEW]
            )]

    answers = inquirer.prompt(questions)
    ssh_key_data = ""
    ssh_key_path = None
    if answers["answer"] == EXISTING_CONTENTS:
        print("Registering from file contents")
        ssh_key_data = input("[\033[33m?\033[0m] Please paste the contents of your public ssh key. It should start with ssh-rsa: ")
    elif answers["answer"] == EXISTING_PATH:
        print("Register in vpc existing key from path")
        questions = [
          inquirer.Text("public_key_path", message='Please paste path to your \033[92mpublic\033[0m ssh key', validate=validate_exists)
        ]
        answers = inquirer.prompt(questions)

        with open(answers["public_key_path"], 'r') as file:
            ssh_key_data = file.read()
    else:
        filename = f"id.rsa.{keyname}"
        os.system(f'ssh-keygen -b 2048 -t rsa -f {filename} -q -N ""')
        print(f"\n\n\033[92mSSH key pair been generated\n")
        print(f"private key: {os.path.abspath(filename)}")
        print(f"public key {os.path.abspath(filename)}.pub\033[0m")
        with open(f"{filename}.pub", 'r') as file:
            ssh_key_data = file.read()
        ssh_key_path = os.path.abspath(filename)

    response = ibm_vpc_client.create_key(public_key=ssh_key_data, name=keyname, resource_group={"id": resource_group_id}, type='rsa')

    print(f"\033[92mnew SSH key {keyname} been registered in vpc\033[0m")

    result = response.get_result()
    return result['name'], result['id'], ssh_key_path

def find_name_id(objects, msg, obj_id=None, obj_name=None, default=None, do_nothing=None):
    if obj_id:
        # just validating that obj exists
        obj_name = next((obj['name'] for obj in objects if obj['id'] == obj_id), None)
        if not obj_name:
            raise Exception(f'Object with specified id {obj_id} not found')
    if obj_name:
        obj_id = next((obj['id'] for obj in objects if obj['name'] == obj_name), None)

    if not obj_id and not obj_name:
        obj = get_option_from_list(msg, objects, default=default, do_nothing=do_nothing)
        if do_nothing and obj == do_nothing:
            return None, None

        obj_id = obj['id']
        obj_name = obj['name']

    return obj_name, obj_id

def convert_to_ray(data):
    result = {'provider': {}, 'node_config': {}}

    result['provider']['region'] = data['region']
    result['provider']['zone_name'] = data['zone']
    result['provider']['endpoint'] = data['endpoint']
    result['provider']['iam_api_key'] = data['iam_api_key'] #TODO:

    result['node_config']['vpc_id'] = data['vpc_id']
    result['node_config']['resource_group_id'] = data['resource_group_id']
    result['node_config']['security_group_id'] = data['sec_group_id']
    result['node_config']['subnet_id'] = data['subnet_id']
    result['node_config']['key_id'] = data['ssh_key_id']
    result['node_config']['image_id'] = data['image_id']
    result['node_config']['instance_profile_name'] = data['instance_profile_name']
    result['node_config']['volume_tier_name'] = data['volume_profile_name']
    if data.get('head_ip'):
        result['node_config']['head_ip'] = data['head_ip']

    result['ssh_key_path'] = data['ssh_key_path']

    return result

def convert_to_lithops(data):
    result = {'ibm_vpc': {}}

    result['ibm_vpc']['endpoint'] = data['endpoint']
    result['ibm_vpc']['vpc_id'] = data['vpc_id']
    result['ibm_vpc']['resource_group_id'] = data['resource_group_id']
    result['ibm_vpc']['security_group_id'] = data['sec_group_id']
    result['ibm_vpc']['subnet_id'] = data['subnet_id']
    result['ibm_vpc']['key_id'] = data['ssh_key_id']
    result['ibm_vpc']['image_id'] = data['image_id']
    result['ibm_vpc']['zone_name'] = data['zone']

    return result

def print_to_file(format, output_file, result, input_file):
    if format:
        print(f"converting results to {format} format")

    if not output_file:
        output_file = tempfile.mkstemp(suffix = '.yaml')[1]

    if format == 'ray':
        result = convert_to_ray(result)

        defaults_config_path = input_file or path.abspath(path.join(__file__ ,"../../etc/gen2-connector/defaults.yaml"))
        template_config, _, node_template, _ = get_template_config(input_file)

        with open(defaults_config_path) as f: #TODO: find path
            config = yaml.safe_load(f)
            config['provider'].update(result['provider'])

            default_cluster_name = template_config.get('cluster_name', 'default')
            default_min_workers = node_template.get('min_workers', '0')
            default_max_workers = node_template.get('max_workers', '0')


            question = [
              inquirer.Text('name', message="Cluster name, either leave default or type a new one", default=default_cluster_name),
              inquirer.Text('min_workers', message="Minimum number of worker nodes", default=default_min_workers, validate=lambda _, x: re.match('^[+]?[0-9]+$', x)),
              inquirer.Text('max_workers', message="Maximum number of worker nodes", default=default_max_workers, validate=lambda answers, x: re.match('^[+]?[0-9]+$', x) and int(x) >= int(answers['min_workers']))
            ]

            answers = inquirer.prompt(question)
            config['cluster_name'] = answers['name']
            config['auth']['ssh_private_key'] = result['ssh_key_path']
            config['max_workers'] = int(answers['max_workers'])

            if config.get('available_node_types'):
                for available_node_type in config['available_node_types']:
                    config['available_node_types'][available_node_type]['node_config'].update(result['node_config'])

                    if not result['node_config'].get('head_ip'):
                        config['available_node_types'][available_node_type]['node_config'].pop('head_ip', None)

                    config['available_node_types'][available_node_type]['min_workers'] = int(answers['min_workers'])
                    config['available_node_types'][available_node_type]['max_workers'] = int(answers['max_workers'])
            else:
                config['available_node_types'] = {'ray_head_default': {'node_config': result['node_config']}}
                config['available_node_types']['ray_head_default']['min_workers'] = int(answers['min_workers'])
                config['available_node_types']['ray_head_default']['max_workers'] = int(answers['max_workers'])

            with open(output_file, 'w') as outfile:
                yaml.dump(config,  outfile, default_flow_style=False)
    elif format == 'lithops':
        result = convert_to_lithops(result)

        with open(output_file, 'w') as outfile:
            yaml.dump(result,  outfile, default_flow_style=False)
    else:
        with open(output_file, 'w') as outfile:
            yaml.dump(result,  outfile, default_flow_style=False)

    print("\n\n=================================================")
    print(f"\033[92mCluster config file: {output_file}\033[0m")
    print("=================================================")

# currently supported only for ray
def get_template_config(input_file):
    template_config = {}
    provider_template = {}
    node_template = {}
    node_config = {}

    if input_file:
        with open(input_file) as f:
             template_config = yaml.safe_load(f)
             provider_template = template_config.get('provider', {})
             node_configs = tuple(template_config.get('available_node_types', {}).values())

             if node_configs:
                 node_template = node_configs[0]
                 node_config = node_configs[0].get('node_config', {})

    return template_config, provider_template, node_template, node_config

def find_default(template_dict, objects, name=None, id=None):
    val = None
    if name:
        key = 'name'
        val = template_dict.get(name)
    elif id:
        key='id'
        val = template_dict.get(id)

    if val:
        obj = next((obj for obj in objects if obj[key] == val), None)
        if obj:
            return obj['name']

REQUIRED_RULES = {'outbound_tcp_all': 'selected security group is missing rule permitting outbound TCP access\n', 'outbound_udp_all': 'selected security group is missing rule permitting outbound UDP access\n', 'inbound_tcp_sg': 'selected security group is missing rule permiting inbound tcp traffic inside selected security group\n', 'inbound_tcp_22': 'selected security group is missing rule permiting inbound traffic to tcp port 22 required for ssh\n', 'inbound_tcp_6379': 'selected security group is missing rule permiting inbound traffic to tcp port 6379 required for Redis\n', 'inbound_tcp_8265': 'selected security group is missing rule permiting inbound traffic to tcp port 8265 required to access Ray Dashboard\n'}

def validate_security_group(ibm_vpc_client, sg_id):
    required_rules = REQUIRED_RULES.copy()

    sg = ibm_vpc_client.get_security_group(sg_id).get_result()

    for rule in sg['rules']:
        # check inboud and outbound rules
        if rule['direction'] == 'outbound' and rule['remote'] == {'cidr_block': '0.0.0.0/0'}:
            if rule['protocol'] == 'all':
                # outbound is fine!
                required_rules.pop('outbound_tcp_all', None)
                required_rules.pop('outbound_udp_all', None)
            elif rule['protocol'] == 'tcp':
                required_rules.pop('outbound_tcp_all', None)
            elif rule['protocol'] == 'udp':
                required_rules.pop('outbound_udp_all', None)
        elif rule['direction'] == 'inbound':
            if rule['remote'] == {'cidr_block': '0.0.0.0/0'}:
                # we interested only in all or tcp protocols
                if rule['protocol'] == 'all':
                    # there a rule permitting all traffic
                    required_rules.pop('inbound_tcp_sg', None)
                    required_rules.pop('inbound_tcp_22', None)
                    required_rules.pop('inbound_tcp_6379', None)
                    required_rules.pop('inbound_tcp_8265', None)

                elif rule['protocol'] == 'tcp':
                    if rule['port_min'] == 1 and rule['port_max'] == 65535:
                        # all ports are open
                        required_rules.pop('inbound_tcp_sg', None)
                        required_rules.pop('inbound_tcp_22', None)
                        required_rules.pop('inbound_tcp_6379', None)
                        required_rules.pop('inbound_tcp_8265', None)
                    else:
                        port_min = rule['port_min']
                        port_max = rule['port_max']
                        if port_min <= 22 and port_max >= 22:
                            required_rules.pop('inbound_tcp_22', None)
                        elif port_min <= 6379 and port_max >= 6379:
                            required_rules.pop('inbound_tcp_6379', None)
                        elif port_min <= 8265 and port_max >= 8265:
                            required_rules.pop('inbound_tcp_8265', None)

            elif rule['remote'].get('id') == sg['id']:
                # validate that inboud trafic inside group available
                if rule['protocol'] == 'all' or rule['protocol'] == 'tcp':
                    required_rules.pop('inbound_tcp_sg', None)

    return required_rules

def build_security_group_rule_prototype_model(missing_rule, sg_id=None):
    direction, protocol, port = missing_rule.split('_')
    remote = {"cidr_block": "0.0.0.0/0"}

    try:
        port = int(port)
        port_min = port
        port_max = port
    except:
        port_min = 1
        port_max = 65535

        # only valid if security group already exists
        if port == 'sg':
            if not sg_id:
                return None
            remote = {'id': sg_id}

    return {
        'direction': direction,
        'ip_version': 'ipv4',
        'protocol': protocol,
        'remote': remote,
        'port_min': port_min,
        'port_max': port_max
        }

def create_security_group(ibm_vpc_client, vpc_id, rg_id):
    rules = []
    for rule in REQUIRED_RULES.keys():
        security_group_rule_prototype_model = build_security_group_rule_prototype_model(rule)
        if security_group_rule_prototype_model:
            rules.append(security_group_rule_prototype_model)

    q = [
          inquirer.Text('name', message="Please, type a name for the new security group", validate=validate_not_empty),
          inquirer.List('answer', message='Create new security group with all required rules', choices=['yes', 'no'], default='yes')
          ]

    answers = inquirer.prompt(q)
    if answers['answer'] == 'yes':
        vpc_identity_model = {'id': vpc_id}
        sg = ibm_vpc_client.create_security_group(vpc_identity_model, name=answers['name'], resource_group={"id": rg_id}, rules=rules).get_result()
        sg_id = sg['id']

        # add rule to open tcp traffic inside security group
        security_group_rule_prototype_model = build_security_group_rule_prototype_model('inbound_tcp_sg', sg_id=sg_id)
        res = ibm_vpc_client.create_security_group_rule(sg_id, security_group_rule_prototype_model).get_result()
        return sg_id, sg['name']
    else:
        return None, None

def add_rules_to_security_group(ibm_vpc_client, sg_id, sg_name, missing_rules):
    add_rule_msgs = {
            'outbound_tcp_all': f'Add rule to open all outbound TCP ports in selected security group {sg_name}',
            'outbound_udp_all': f'Add rule to open all outbound UDP ports in selected security group {sg_name}',
            'inbound_tcp_sg': f'Add rule to open inbound tcp traffic inside selected security group {sg_name}',
            'inbound_tcp_22': f'Add rule to open inbound tcp port 22 required for SSH in selected security group {sg_name}',
            'inbound_tcp_6379': f'Add rule to open inbound tcp port 6379 required for Redis in selected security group {sg_name}',
            'inbound_tcp_8265': f'Add rule to open inbound tcp port 8265 required to access Ray Dashboard in selected security group {sg_name}'}

    for missing_rule in missing_rules.keys():
        q = [
                inquirer.List('answer',
                message=add_rule_msgs[missing_rule],
                choices=['yes', 'no'],
                default='yes')
            ]

        answers = inquirer.prompt(q)
        if answers['answer'] == 'yes':
            security_group_rule_prototype_model = build_security_group_rule_prototype_model(missing_rule, sg_id=sg_id)
            res = ibm_vpc_client.create_security_group_rule(sg_id, security_group_rule_prototype_model).get_result()
        else:
            return False
    return True

def _create_vpc(ibm_vpc_client, resource_group, vpc_default_name):
    
    q = [
          inquirer.Text('name', message="Please, type a name for the new VPC", validate=validate_not_empty, default=vpc_default_name),
          inquirer.List('answer', message='Create new VPC and configure required rules in default security group', choices=['yes', 'no'], default='yes')
        ]

    answers = inquirer.prompt(q)
    if answers['answer'] == 'yes':        
        vpc_obj = ibm_vpc_client.create_vpc(address_prefix_management='auto', classic_access=False, 
            name=answers['name'], resource_group=resource_group).get_result()

        return vpc_obj
    else:
        return None

def _select_vpc(ibm_vpc_client, resource_service_client, vpc_id, node_config, region):

    vpc_name = None
    zone_obj = None
    sg_id = None

    def select_zone(vpc_id):
        # find availability zone
        zones_objects = ibm_vpc_client.list_region_zones(region).get_result()['zones']
        if vpc_id:
            # filter out zones that given vpc has no subnets in
            all_subnet_objects = ibm_vpc_client.list_subnets().get_result()['subnets']
            zones = [s_obj['zone']['name'] for s_obj in all_subnet_objects if s_obj['vpc']['id'] == vpc_id]
            zones_objects = [z for z in zones_objects if z['name'] in zones]

        try:
            zone_obj = get_option_from_list("Choose availability zone", zones_objects, default = default)
        except:
            raise Exception("Failed to list zones for selected vpc {vpc_id}, please check whether vpc missing subnet")

        return zone_obj

    
    def select_resource_group():
        # find resource group
        endpoint = None
        res_group_objects = resource_service_client.list_resource_groups().get_result()['resources']
    
        default = find_default(node_config, res_group_objects, name='resource_group_id')
        res_group_obj = get_option_from_list("Choose region", res_group_objects, default=default)
        return res_group_obj['id']

    while True:
        CREATE_NEW = 'Create new VPC'

        vpc_objects = ibm_vpc_client.list_vpcs().get_result()['vpcs']
        default = find_default(node_config, vpc_objects, id='vpc_id')
        vpc_name, vpc_id = find_name_id(vpc_objects, "Select VPC", obj_id=vpc_id, do_nothing=CREATE_NEW, default=default)

        zone_obj = select_zone(vpc_id)

        if not vpc_name:
            resource_group_id = select_resource_group()
            resource_group = {'id': resource_group_id}

            # find next default vpc name
            vpc_default_name = RAY_VPC_NAME
            c = 1
            vpc_names = [vpc_obj['name'] for vpc_obj in vpc_objects]
            while vpc_default_name in vpc_names:
                vpc_default_name = f'{RAY_VPC_NAME}-{c}' 
                c += 1
            
            vpc_obj = _create_vpc(ibm_vpc_client, resource_group, vpc_default_name)
            if not vpc_obj:
                continue
            else:      
                vpc_name = vpc_obj['name']
                vpc_id = vpc_obj['id']

                print(f"\n\n\033[92mVPC {vpc_name} been created\033[0m")

                # create and attach public gateway
                gateway_prototype = {}
                gateway_prototype['vpc'] = {'id': vpc_id}
                gateway_prototype['zone'] = {'name': zone_obj['name']}
                gateway_prototype['name'] = f"{vpc_name}-gw"
                gateway_prototype['resource_group'] = resource_group
                gateway_data = ibm_vpc_client.create_public_gateway(**gateway_prototype).get_result()
                gateway_id = gateway_data['id']

                print(f"\033[92mVPC public gateway {gateway_prototype['name']} been created\033[0m")

                # create subnet
                subnet_name = '{}-subnet'.format(vpc_name)
                subnet_data = None

                subnets_info = ibm_vpc_client.list_subnets().result
            
                # find cidr
                ipv4_cidr_block = None
                res = ibm_vpc_client.list_vpc_address_prefixes(vpc_id).result
                address_prefixes = res['address_prefixes']
            
                for address_prefix in address_prefixes:
                    if address_prefix['zone']['name'] == zone_obj['name']:
                        ipv4_cidr_block = address_prefix['cidr']
                        break
                
                subnet_prototype = {}
                subnet_prototype['zone'] = {'name': zone_obj['name']}
                subnet_prototype['ip_version'] = 'ipv4'
                subnet_prototype['name'] = subnet_name
                subnet_prototype['resource_group'] = resource_group
                subnet_prototype['vpc'] = {'id': vpc_id}
                subnet_prototype['ipv4_cidr_block'] = ipv4_cidr_block

                subnet_data = ibm_vpc_client.create_subnet(subnet_prototype).result
                subnet_id = subnet_data['id']

                # Attach public gateway to the subnet
                ibm_vpc_client.set_subnet_public_gateway(subnet_id, {'id': gateway_id})

                print(f"\033[92mVPC subnet {subnet_prototype['name']} been created and attached to gateway\033[0m")

                # Update security group to have all required rules
                sg_id = vpc_obj['default_security_group']['id']

                # update sg name
                sg_name = '{}-sg'.format(vpc_name)
                ibm_vpc_client.update_security_group(sg_id, security_group_patch={'name': sg_name})

                # add rule to open tcp traffic inside security group
                sg_rule_prototype = build_security_group_rule_prototype_model('inbound_tcp_sg', sg_id=sg_id)
                res = ibm_vpc_client.create_security_group_rule(sg_id, sg_rule_prototype).get_result()

                # add all other required rules
                for rule in REQUIRED_RULES.keys():
                    sg_rule_prototype = build_security_group_rule_prototype_model(rule)
                    if sg_rule_prototype:
                        res = ibm_vpc_client.create_security_group_rule(sg_id, sg_rule_prototype).get_result()

                print(f"\033[92mSecurity group {sg_name} been updated with required rules\033[0m\n")

        else:
            break

    vpc_obj = ibm_vpc_client.get_vpc(id=vpc_id).result
    return vpc_obj, zone_obj, sg_id

@click.command()
@click.option('--output-file', '-o', help='Output filename to save configurations')
@click.option('--input-file', '-i', help=f'Template for new configuration, default: {path.abspath(path.join(__file__ ,"../../etc/gen2-connector/defaults.yaml"))}')
@click.option('--iam-api-key', required=True, help='IAM_API_KEY')
@click.option('--region', help='region')
@click.option('--vpc-id', help='vpc id')
@click.option('--sec-group-id', help='security group id')
@click.option('--ssh-key-id', help='ssh key id')
@click.option('--image-id', help='image id')
@click.option('--instance-profile-name', help='instance profile name')
@click.option('--volume-profile-name', default='general-purpose', help='volume profile name')
@click.option('--head-ip', help='head node floating ip')
@click.option('--format', type=click.Choice(['lithops', 'ray']), help='if not specified will print plain text')
def builder(output_file, input_file, iam_api_key, region, vpc_id, sec_group_id, ssh_key_id, image_id, instance_profile_name, volume_profile_name, head_ip, format):
    print(f"\n\033[92mWelcome to vpc config export helper\033[0m\n")

    template_config, provider_template, node_template, node_config = get_template_config(input_file)

    authenticator = IAMAuthenticator(iam_api_key)
    ibm_vpc_client = VpcV1('2021-01-19', authenticator=authenticator)
    resource_service_client = ResourceManagerV2(authenticator=authenticator)

    result = {}

    # find region and endpoint
    endpoint = None
    regions_objects = ibm_vpc_client.list_regions().get_result()['regions']
    if not region:
        default = find_default(provider_template, regions_objects, name='region')
        region_obj = get_option_from_list("Choose region", regions_objects, default = default)
        region = region_obj['name']
        endpoint = region_obj['endpoint']
    else:
        # just need to find endpoint
        region_obj = next((obj for obj in regions_objects if obj['name'] == region), None)
        endpoint = region_obj['endpoint']

    ibm_vpc_client.set_service_url(endpoint  + '/v1')

    result['region'] = region
    result['endpoint'] = endpoint

    vpc_obj, zone_obj, sec_group_id = _select_vpc(ibm_vpc_client, resource_service_client, vpc_id, node_config, region)
    if not vpc_obj:
        raise Exception(f'Failed to select VPC')

    zone_name = zone_obj['name']
    result['zone'] = zone_name

    vpc_id = vpc_obj['id']
    vpc_name = vpc_obj['name']

    result['vpc_name'] = vpc_name
    result['vpc_id'] = vpc_id
    
    resource_group_id = vpc_obj['resource_group']['id']

    result['resource_group_id'] = resource_group_id
    result['resource_group_name'] = vpc_obj['resource_group']['name']

    ssh_key_objects = ibm_vpc_client.list_keys().get_result()['keys']
    CREATE_NEW_SSH_KEY = "Register new SSH key in IBM VPC"

    default = find_default(node_config, ssh_key_objects, id='key_id')
    ssh_key_name, ssh_key_id = find_name_id(ssh_key_objects, 'Choose ssh key', obj_id=ssh_key_id, do_nothing=CREATE_NEW_SSH_KEY, default=default)

    ssh_key_path = None
    if not ssh_key_name:
        ssh_key_name, ssh_key_id, ssh_key_path  = register_ssh_key(ibm_vpc_client, result['resource_group_id'])

    if not ssh_key_path:
        questions = [
          inquirer.Text("private_key_path", message=f'Please paste path to \033[92mprivate\033[0m ssh key associated with selected public key {ssh_key_name}', validate=validate_exists, default="~/.ssh/id_rsa")
        ]
        answers = inquirer.prompt(questions)
        ssh_key_path = os.path.abspath(os.path.expanduser(answers["private_key_path"]))

    result['ssh_key_name'] = ssh_key_name
    result['ssh_key_id'] = ssh_key_id
    result['ssh_key_path'] = ssh_key_path

    while True:

        CREATE_NEW_SECURITY_GROUP = 'Create new security group'

        sec_group_objects = ibm_vpc_client.list_security_groups(resource_group_id=resource_group_id, vpc_id=vpc_id).get_result()['security_groups']
        default = find_default(node_config, sec_group_objects, id='security_group_id')
        sec_group_name, sec_group_id = find_name_id(sec_group_objects, "Choose security group", obj_id=sec_group_id, do_nothing=CREATE_NEW_SECURITY_GROUP, default=default)

        if not sec_group_name:
            sec_group_id, sec_group_name = create_security_group(ibm_vpc_client, vpc_id, vpc_obj['resource_group']['id'])
            if not sec_group_id:
                continue

        errors = validate_security_group(ibm_vpc_client, sec_group_id)
        if errors:
            for val in errors.values():
                print(f"\033[91m{val}\033[0m")

            SELECT = 'Select again security group'
            UPDATE_SELECTED = 'Interactively add required rules to selected security group'

            questions = [
                inquirer.List('answer',
                    message='Selected security group is missing required rules, see error above, please choose',
                    choices=[SELECT, UPDATE_SELECTED, CREATE_NEW_SECURITY_GROUP],
                    default=default,
                ),]

            answers = inquirer.prompt(questions)
            sg_done = False

            if answers['answer'] == SELECT:
                sec_group_id = None
                continue
            elif answers['answer'] == UPDATE_SELECTED:
                # add rules to selected security group
                sg_done = add_rules_to_security_group(ibm_vpc_client, sec_group_id, sec_group_name, errors)
            else:
                # create new security group with all rules
                sec_group_id, sec_group_name = create_security_group(ibm_vpc_client, vpc_id, vpc_obj['resource_group']['id'])
                if sec_group_id:
                    sg_done == True

            if not sg_done:
                sec_group_id = None
                continue

            # just in case, validate again the updated/created security group
            errors = validate_security_group(ibm_vpc_client, sec_group_id)
            if not errors:
                break
            else:
                print(f'Something failed during security group rules update/create, please update the required rules manually using ibmcli or web ui and try again')
                exit(1)
        else:
            break

    result['sec_group_name'] = sec_group_name
    result['sec_group_id'] = sec_group_id

    floating_ips = ibm_vpc_client.list_floating_ips().get_result()['floating_ips']
    if head_ip:
        for ip in floating_ips:
            if ip['address'] == head_ip:
                if ip.get('target'):
                    raise Exception(f"Specified head ip {head_ip} occupied, please choose another or let ray create a new one")
                else:
                    result['head_ip'] = head_ip
                    break
    else:
        free_floating_ips = [x for x in floating_ips if not x.get('target')]
        if free_floating_ips:
            ALLOCATE_NEW_FLOATING_IP = 'Allocate new floating ip'
            head_ip_obj = get_option_from_list("Choose head ip", free_floating_ips, choice_key='address', do_nothing=ALLOCATE_NEW_FLOATING_IP)
            if head_ip_obj and (head_ip_obj != ALLOCATE_NEW_FLOATING_IP):
                result['head_ip'] = head_ip_obj['address']

    all_subnet_objects = ibm_vpc_client.list_subnets().get_result()['subnets']

    #filter only subnets from selected availability zone
    subnet_objects = [s_obj for s_obj in all_subnet_objects if s_obj['zone']['name'] == zone_name and s_obj['vpc']['id'] == vpc_id]

    if not subnet_objects:
        raise f'Failed to find subnet for vpc {vpc_name} in zone {zone_name}'

    # taking first one found
    subnet_name = subnet_objects[0]['name']
    subnet_id = subnet_objects[0]['id']
    result['subnet_name'] = subnet_name
    result['subnet_id'] = subnet_id

    image_objects = ibm_vpc_client.list_images().get_result()['images']
    default = find_default(node_config, image_objects, id='image_id') or 'ibm-ubuntu-20-04-minimal-amd64-2'

    image_name, image_id = find_name_id(image_objects, 'Please choose \033[92mUbuntu\033[0m 20.04 VM image, currently only Ubuntu supported', obj_id=image_id, default=default)
    result['image_name'] = image_name
    result['image_id'] = image_id

    instance_profile_objects = ibm_vpc_client.list_instance_profiles().get_result()['profiles']
    if instance_profile_name:
        # just validate
        instance_profile_name = next((obj['name'] for obj in instance_profile_objects if obj['name'] == instance_profile_name), None)
        if not instance_profile_name:
            raise Exception(f"specified instance_profile_name {instance_profile_name} not found")
    else:
        default = find_default(node_config, instance_profile_objects, name='instance_profile_name')
        obj = get_option_from_list('Carefully choose instance profile, please refer to https://cloud.ibm.com/docs/vpc?topic=vpc-profiles', instance_profile_objects, default=default)
        instance_profile_name = obj['name']

    result['instance_profile_name'] = instance_profile_name

    result['volume_profile_name'] = volume_profile_name
    result['iam_api_key'] = iam_api_key

    print(f"vpc name: {vpc_name} id: {vpc_id}\nzone: {zone_obj['name']}\nendpoint: {endpoint}\nregion: {region}\nresource group name: {result['resource_group_name']} id: {result['resource_group_id']}\nsecurity group name: {sec_group_name} id: {sec_group_id}\nsubnet name: {subnet_name} id: {subnet_id}\nssh key name: {ssh_key_name} id {ssh_key_id}\nimage name: {image_name} id: {image_id}\n")

    print_to_file(format, output_file, result, input_file)


if __name__ == '__main__':
    builder()
