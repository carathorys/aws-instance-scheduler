######################################################################################################################
#  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Apache License Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://www.apache.org/licenses/                                                                               #
#                                                                                                                    #
#  or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################

import copy
import re

import schedulers
import re
import copy

from boto_retry import get_client_with_retries
from configuration.instance_schedule import InstanceSchedule
from configuration.running_period import RunningPeriod
from configuration.scheduler_config_builder import SchedulerConfigBuilder
from configuration.setbuilders.weekday_setbuilder import WeekdaySetBuilder

RESTRICTED_ECS_TAG_VALUE_SET_CHARACTERS = r"[^a-zA-Z0-9\s_\.:+/=\\@-]"

ERR_STARTING_INSTANCE = "Error starting ecs {} {} ({})"
ERR_STOPPING_INSTANCE = "Error stopping ecs {} {}, ({})"
ERR_DELETING_SNAPSHOT = "Error deleting snapshot {}"

INF_ADD_TAGS = "Adding {} tags {} to instance {}"
INF_DELETE_SNAPSHOT = "Deleted previous snapshot {}"
INF_FETCHED = "Number of fetched ecs {} is {}, number of schedulable  resources is {}"
INF_FETCHING_RESOURCES = "Fetching ecs {} for account {} in region {}"
INF_REMOVE_KEYS = "Removing {} key(s) {} from instance {}"
INF_STOPPED_RESOURCE = "Stopped ecs {} \"{}\""

DEBUG_READ_REPLICA = "Can not schedule ecs instance \"{}\" because it is a read replica of instance {}"
DEBUG_READ_REPLICA_SOURCE = "Can not schedule ecs instance \"{}\" because it is the source for read copy instance(s) {}"
DEBUG_SKIPPING_INSTANCE = "Skipping ecs {} {} because it is not in a start or stop-able state ({})"
DEBUG_WITHOUT_SCHEDULE = "Skipping ecs {} {} without schedule"
DEBUG_SELECTED = "Selected ecs instance {} in state ({}) for schedule {}"
DEBUG_NO_SCHEDULE_TAG = "Instance {} has no schedule tag named {}"

WARN_TAGGING_STARTED = "Error setting start or stop tags to started instance {}, ({})"
WARN_TAGGING_STOPPED = "Error setting start or stop tags to stopped instance {}, ({})"
WARN_RDS_TAG_VALUE = "Tag value \"{}\" for tag \"{}\" changed to \"{}\" because it did contain characters that are not allowed " \
                     "in ECS tag values. The value can only contain only the set of Unicode letters, digits, " \
                     "white-space, '_', '.', '/', '=', '+', '-'"

MAINTENANCE_SCHEDULE_NAME = "ECS preferred Maintenance Window Schedule"
MAINTENANCE_PERIOD_NAME = "ECS preferred Maintenance Window Period"


class EcsService:
    ECS_STATE_AVAILABLE = "available"
    ECS_STATE_STOPPED = "stopped"

    ECS_SCHEDULABLE_STATES = {ECS_STATE_AVAILABLE, ECS_STATE_STOPPED}

    def __init__(self):
        self.service_name = "ecs"
        self.allow_resize = False
        self._instance_tags = None

        self._context = None
        self._session = None
        self._region = None
        self._account = None
        self._logger = None
        self._tagname = None
        self._stack_name = None
        self._config = None

    def _init_scheduler(self, args):
        """
        Initializes common parameters
        :param args: action parameters
        :return:
        """
        self._account = args.get(schedulers.PARAM_ACCOUNT)
        self._context = args.get(schedulers.PARAM_CONTEXT)
        self._logger = args.get(schedulers.PARAM_LOGGER)
        self._region = args.get(schedulers.PARAM_REGION)
        self._stack_name = args.get(schedulers.PARAM_STACK)
        self._session = args.get(schedulers.PARAM_SESSION)
        self._tagname = args.get(schedulers.PARAM_CONFIG).tag_name
        self._config = args.get(schedulers.PARAM_CONFIG)
        self._instance_tags = None

    def get_schedulable_resources(self, fn_is_schedulable, fn_list_name, fn_describe_name, kwargs):

        self._init_scheduler(kwargs)

        client = get_client_with_retries("ecs",
                                         [fn_list_name, fn_describe_name],
                                         context=self._context,
                                         session=self._session,
                                         region=self._region)

        list_arguments = {"maxResults": 100}

        resource_name = fn_describe_name.split("_")[-1]
        resource_name = resource_name[0].upper() + resource_name[1:]
        resources = []
        number_of_resources = 0
        self._logger.info(INF_FETCHING_RESOURCES, resource_name, self._account, self._region)
        fn_list = getattr(client, fn_list_name + "_with_retries")
        fn_desc = getattr(client, fn_describe_name + "_with_retries")

        while True:
            self._logger.debug("Making {} call with parameters {}", fn_list_name, list_arguments)
            ecs_clusters = fn_list(**list_arguments)

            for resource in ecs_clusters["clusterArns"]:
                describe_arguments = {"clusters": [resource], "include": ["ATTACHMENTS",
                                                                          "SETTINGS",
                                                                          "STATISTICS",
                                                                          "TAGS"]}
                self._logger.debug("Making {} call with parameters {}", fn_describe_name, describe_arguments)
                ecs_resp = fn_desc(**describe_arguments)

                number_of_resources += 1

                if fn_is_schedulable(ecs_resp):
                    self._logger.debug("ECS Cluster data: {}", ecs_resp['clusters'][0])
                    resource_data = self._select_resource_data(ecs_resource=ecs_resp['clusters'][0],
                                                               is_cluster=True)
                    self._logger.debug("Resource data: {}", resource_data)

                    schedule_name = resource_data[schedulers.INST_SCHEDULE]
                    if schedule_name not in [None, ""]:
                        self._logger.debug(DEBUG_SELECTED, resource_data[schedulers.INST_ID],
                                           resource_data[schedulers.INST_CURRENT_STATE],
                                           schedule_name)
                        resources.append(resource_data)
                    else:
                        self._logger.debug(DEBUG_WITHOUT_SCHEDULE, resource_name[:-1],
                                           resource_data[schedulers.INST_ID])
            if "Marker" in ecs_clusters:
                list_arguments["Marker"] = ecs_clusters["Marker"]
            else:
                break
        self._logger.info(INF_FETCHED, resource_name, number_of_resources, len(resources))
        return resources

    def get_schedulable_ecs_services(self, kwargs):
        pass

        def is_schedulable(service):
            return True

        return self.get_schedulable_resources(fn_is_schedulable=is_schedulable,
                                              fn_list_name="list_services",
                                              fn_describe_name="describe_services",
                                              kwargs=kwargs)

    def get_schedulable_ecs_clusters(self, kwargs):
        def is_schedulable(cluster):
            return True

        return self.get_schedulable_resources(fn_is_schedulable=is_schedulable,
                                              fn_list_name="list_clusters",
                                              fn_describe_name="describe_clusters",
                                              kwargs=kwargs)

    def get_schedulable_instances(self, kwargs):
        # instances = self.get_schedulable_ecs_services(kwargs)
        # if self._config.schedule_clusters:
        instances = self.get_schedulable_ecs_clusters(kwargs)
        return instances

    def _select_resource_data(self, ecs_resource, is_cluster):

        arn_for_tags = ecs_resource["clusterArn"]
        tags = {tag["key"]: tag["value"]
                for tag in ecs_resource['tags']}

        methods = ["list_services",
                   "describe_services"]

        client = get_client_with_retries("ecs", methods, context=self._context, session=self._session,
                                         region=self._region)
        list_of_services = client.list_services_with_retries(cluster=ecs_resource['clusterArn'])
        service_details = client.describe_services_with_retries(cluster=ecs_resource['clusterArn'],
                                                                services=list_of_services['serviceArns'])
        running_tasks = 0
        pending_tasks = 0
        desired_tasks = 0
        saved_desired_tasks = 0
        services = {}
        for service in service_details['services']:
            running_tasks += service['runningCount']
            pending_tasks += service['pendingCount']
            desired_tasks += service['desiredCount']
            services[service['serviceArn']] = {'currentDesiredCount': service['desiredCount'],
                                               'savedDesiredCount': int(tags[service['serviceArn']])
                                               }
            saved_desired_tasks += services[service['serviceArn']]['savedDesiredCount']

        is_running = running_tasks >= desired_tasks > 0
        is_starting = running_tasks < desired_tasks
        is_terminating = is_running and desired_tasks == 0
        is_terminated = not is_running and not is_terminating and desired_tasks == 0 and saved_desired_tasks == 0

        instance_data = {
            schedulers.INST_ID: ecs_resource["clusterName"],
            schedulers.INST_ARN: ecs_resource["clusterArn"],
            schedulers.INST_ALLOW_RESIZE: self.allow_resize,
            schedulers.INST_HIBERNATE: False,
            schedulers.INST_IS_RUNNING: is_running and not is_starting,
            schedulers.INST_IS_TERMINATED: is_terminated and saved_desired_tasks < 1,
            schedulers.INST_CURRENT_STATE: InstanceSchedule.STATE_RUNNING if is_running else InstanceSchedule.STATE_STOPPED,
            schedulers.INST_TAGS: tags,
            schedulers.INST_NAME: tags.get("Name", ""),
            schedulers.INST_SCHEDULE: tags.get(self._tagname, None),
            schedulers.INST_MAINTENANCE_WINDOW: None,
            schedulers.INST_INSTANCE_TYPE: 'cluster',
            'services': services,
            'is_running': is_running,
            'is_starting': is_starting,
            'is_terminated': is_terminated,
            'is_terminating': is_terminating
        }
        return instance_data

    def resize_instance(self, kwargs):
        pass

    def stop_instances(self, kwargs):

        self._init_scheduler(kwargs)

        methods = ["list_services",
                   "describe_services",
                   "update_service",
                   "tag_resource",
                   "untag_resource"]

        client = get_client_with_retries("ecs", methods, context=self._context, session=self._session,
                                         region=self._region)

        stopped_instances = kwargs["stopped_instances"]
        for ecs_resource in stopped_instances:
            try:
                list_of_services = client.list_services_with_retries(cluster=ecs_resource.id)
                service_details = client.describe_services_with_retries(cluster=ecs_resource.id,
                                                                        services=list_of_services['serviceArns'],
                                                                        include=['TAGS'])

                for service in service_details['services']:
                    client.tag_resource_with_retries(resourceArn=service['clusterArn'], tags=[{
                        'key': service['serviceArn'],
                        'value': str(service['desiredCount'])
                    }])
                    client.update_service_with_retries(cluster=ecs_resource.id, service=service['serviceName'],
                                                       desiredCount=0, forceNewDeployment=True)
                    self._logger.info(INF_STOPPED_RESOURCE, "service", service['serviceName'])

                yield ecs_resource.id, InstanceSchedule.STATE_STOPPED
            except Exception as ex:
                self._logger.error(ERR_STOPPING_INSTANCE, "cluster",
                                   ecs_resource.instance_str, str(ex))

    # noinspection PyMethodMayBeStatic
    def start_instances(self, kwargs):
        self._init_scheduler(kwargs)

        methods = ["list_services",
                   "describe_services",
                   "update_service",
                   "tag_resource",
                   "untag_resource"]

        client = get_client_with_retries("ecs", methods, context=self._context, session=self._session,
                                         region=self._region)

        self._logger.debug("Starting instances: {}", kwargs["started_instances"])
        started_instances = kwargs["started_instances"]
        for ecs_resource in started_instances:
            try:
                list_of_services = client.list_services_with_retries(cluster=ecs_resource.id)
                service_details = client.describe_services_with_retries(cluster=ecs_resource.id,
                                                                        services=list_of_services['serviceArns'])
                tags = ecs_resource.tags
                for service in service_details['services']:
                    if tags[service['serviceArn']] is not None:
                        client.update_service_with_retries(cluster=ecs_resource.id, service=service['serviceName'],
                                                           desiredCount=int(tags[service['serviceArn']]),
                                                           forceNewDeployment=True)
                        client.untag_resource_with_retries(resourceArn=service['clusterArn'],
                                                           tagKeys=[service['serviceArn']])

                yield ecs_resource.id, InstanceSchedule.STATE_RUNNING
            except Exception as ex:
                self._logger.error(ERR_STARTING_INSTANCE, "cluster" if ecs_resource.is_cluster else "instance",
                                   ecs_resource.instance_str, str(ex))
                return
