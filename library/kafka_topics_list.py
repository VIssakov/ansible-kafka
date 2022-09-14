#!env python

DOCUMENTATION = '''
---
module: kafka_topics_list
short_description: Get topics from kafka
description:
   - Get all topic from kafka cluster
options:
    kafka_servers:
        description:
            - List of kafka servers
        required: true
'''

EXAMPLES = '''
vars:
  kafka_servers:
    - broker1
    - broker2
    - broker3

- name: get kafka topics
  kafka_topics_list:
    kafka_servers: "{{ kafka_servers }}"
  register: kafka_topics_list_result
'''

RETURN = '''
topics:
    description: List of kafka topics
    returned: success
    type: list
'''

ANSIBLE_METADATA = {
    'metadata_version': '1.1',
    'supported_by': 'community',
    'status': ['preview']
}

try:
    import kafka
    HAS_KAFKA = True
except ImportError:
    HAS_KAFKA = False

from ansible.module_utils.basic import AnsibleModule
from subprocess import call, check_output

def get_kafka_topics(broker_list):
    consumer = kafka.KafkaConsumer(group_id='ansible', bootstrap_servers=broker_list)
    return consumer.topics()

def main():
    module = AnsibleModule(
        argument_spec=dict(
            kafka_servers=dict(type='list', required=True, default=None)
        ),
        supports_check_mode=False
    )

    if not HAS_KAFKA:
        module.fail_json(msg='python module kafka is required for this module')

    broker_list = module.params['kafka_servers']
    topics_list = get_kafka_topics(broker_list)

    result = dict(
        changed=False,
        topics=topics_list
    )

    if topics_list:
        result['changed'] = True

    module.exit_json(**result)


if __name__ == '__main__':
    main()