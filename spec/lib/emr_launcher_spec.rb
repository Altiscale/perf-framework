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

require 'spec_helper'

describe EMRLauncher, '#launch_emr' do
  cluster_name = 'fake_cluster'
  config = { name: 'emr', alive: true, instance_count: 2 }
  it 'is invoked with the correct config' do
    job_flow = double(AWS::EMR::JobFlow)
    job_flow.stub(:state).and_return('WAITING')
    emr = double(AWS::EMR)
    emr.stub(:job_flows) do |job_flows|
      job_flows.stub(:create).with(cluster_name, config).and_return(job_flow)
    end
    AWS::EMR.stub(:new).with(region: 'us-west-2').and_return(emr)
    launcher = EMRLauncher.new cluster_name, config
    expect(launcher.launch_emr).to eq(job_flow)
  end
end

describe EMRLauncher, '#tag_instances' do
  cluster_name = 'fake_cluster'
  config = { name: 'emr', alive: true, instance_count: 2 }
  jobflow_id = 'my_jobflow_id'
  instances = { tag_set: [{ resource_id: 'i1' }, { resource_id: 'i2' }] }
  instance_list = %w(i1 i2)
  current_user = 'me'
  it 'is invoked with the correct config' do
    ec2 = double(AWS::EC2)
    client = double(AWS::EC2::Client)
    client.stub(:describe_tags).with(
      filters: [{ name: 'resource-type', values: ['instance'] },
                { name: 'key', values: ['aws:elasticmapreduce:job-flow-id'] },
                { name: 'value', values: ["#{jobflow_id}"] }
      ]
    ).and_return(instances)
    client.should_receive(:create_tags).with(resources: instance_list,
                                             tags: [{ key: 'Customer', value: 'Engineering' },
                                                    { key: 'User', value: "#{current_user}@altiscale.com" }])
    ec2.stub(:client).and_return(client)
    AWS::EC2.stub(:new).with(region: 'us-west-2').and_return(ec2)
    launcher = EMRLauncher.new cluster_name, config
    launcher.tag_instances jobflow_id, current_user
  end
end

describe EMRTerminator, '#run' do
  it 'terminates if job exists' do
    jobflow_id = 'job1'
    prior_results = { 'jobflow_id' => jobflow_id }
    job_flow = double(AWS::EMR::JobFlow)
    job_flow.stub(:exists?).and_return(true)
    job_flow.should_receive(:terminate)
    emr = double(AWS::EMR)
    emr.stub(:job_flows) do |job_flows|
      job_flows.stub(:[]).with(jobflow_id).and_return(job_flow)
    end
    AWS::EMR.stub(:new).with(region: 'us-west-2').and_return(emr)
    EMRTerminator.new.run prior_results
  end
end
