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

# Decorators that wrap the benchmark to do additional work before and after
class RemoteDistCP
  include Logging
  HADOOP_FINISHED_STATE = 'FINISHED'
  JOB_STATUS_SLEEP_INTERNVAL = 15
  attr_reader :description
  def initialize(ssh_command, hdfs_jhist_dir, s3_log_dir)
    @ssh_command = ssh_command
    @hdfs_jhist_dir = hdfs_jhist_dir
    @s3_log_dir = s3_log_dir
    @description = "Copy  #{@hdfs_jhist_dir} => #{@s3_log_dir}"
  end

  def run(prior_result={})
    # somehow get the application_id here
    until(job_finished?(prior_result[:application_num])) do
      sleep JOB_STATUS_SLEEP_INTERNVAL
    end 
    command = "hadoop distcp #{@hdfs_jhist_dir} #{@s3_log_dir}"
    status = @ssh_command.execute command
    status
  end
  
  def job_finished?(application_num)
    return true if application_num.nil?
    # make an ssh rest call
    rest_call = "curl --get \'http://localhost:9026/ws/v1/cluster/apps/#{application_num}\'"
    @ssh_command.execute rest_call
    state =  @ssh_command.validator.json['state'] 
    logger.debug "state = #{state}"
    state == HADOOP_FINISHED_STATE
  end
end

