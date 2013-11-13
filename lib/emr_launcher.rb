# Copyright (c) 2013 Altiscale, inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

require 'aws-sdk'
require 'set'
EMR_REGION = 'us-west-2'
# Launches an emr
class EMRLauncher
  include Logging
  include Command
  EMR_USER = 'hadoop'
  EMR_STATUS_SLEEP = 15
  def initialize(cluster_name, config)
    @cluster_name = cluster_name
    @config = config
    @description = "Emr launcher with #{cluster_name} and #{config.to_s}"
  end

  def launch_emr
    emr = AWS::EMR.new region: EMR_REGION
    job_flow = emr.job_flows.create @cluster_name, @config
    done_states = Set.new %w(WAITING FAILED TERMINATED COMPLETED)
    until done_states.include? job_flow.state
      logger.debug "job_flow #{job_flow.job_flow_id} #{job_flow.state}"
      sleep EMR_STATUS_SLEEP
    end
    fail "Could not start job_flow #{job_flow.job_flow_id}" if job_flow.state == 'FAILED'
    job_flow
  end

  def tag_instances(jobflow_id, user)
    ec2 = AWS::EC2.new region: EMR_REGION
    client = ec2.client
    instances = client.describe_tags(
        filters: [{ name: 'resource-type', values: ['instance'] },
                  { name: 'key', values: ['aws:elasticmapreduce:job-flow-id'] },
                  { name: 'value', values: ["#{jobflow_id}"] }
        ]
      )

    instances = instances[:tag_set]
    logger.debug "instances #{instances.to_s}"
    instance_list = []
    instances.each { |instance| instance_list << instance[:resource_id] }
    logger.debug "instances #{instance_list}"
    client.create_tags(resources: instance_list,
                       tags: [{ key: 'Customer', value: 'Engineering' },
                              { key: 'User', value: "#{user}@altiscale.com" }])
    logger.info "Successfully launched emr instance: #{jobflow_id}"
  end

  def populate_result(job_flow)
    result = {}
    result['platform_name'] = 'emr'
    result['user'] = EMR_USER
    result['host'] = job_flow.master_public_dns_name
    result['jobflow_id'] = job_flow.job_flow_id
    result['node_type'] = job_flow.master_instance_type
    result['hadoop_slaves'] = job_flow.instance_count
    logger.debug "populated result: #{result}"
    result
  end

  def run(prior_result)
    job_flow = launch_emr
    tag_instances job_flow.job_flow_id, prior_result[:current_user]
    populate_result job_flow
  end
end

# Terminates an emr cluster
class EMRTerminator
  include Logging
  include Command
  def initialize
    @description = 'Terminate emr'
  end

  def run(prior_result)
    terminate prior_result['jobflow_id']
  end

  def terminate(jobflow_id)
    emr = AWS::EMR.new region: EMR_REGION
    job_flow = emr.job_flows[jobflow_id]
    job_flow.terminate if job_flow.exists?
  end
end
