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
# This class will copy the hadoop jar, clean up the output, and run an MR job

require 'logging'
require 'validators'
require 'writers'
class MRBenchmark  
  include Logging
  #TODO: Exposed everything for testability. Need to find smarter way
  attr_writer :writer, :validator, :uniquify
  attr_reader :description
  def initialize (benchmark_config, platform_config, scp_uploader, ssh_command)
    @benchmark_config = benchmark_config
    @platform_config = platform_config
    @benchmark = @benchmark_config["benchmark"]
    @platform = @platform_config["platform"]
    @scp_uploader = scp_uploader
    @ssh_command = ssh_command
    @description = "mr job: #{@benchmark} on #{@platform}"
  end
  
  # build up the set of commands accoring to the parameters provided

  #
  def run label=nil
    cleanup_command =  @benchmark_config["platformspec"][@platform]["cleanup_command"]
    local_jar = @benchmark_config["platformspec"][@platform]["local_jar"]
    hadoop_jar = @benchmark_config["platformspec"][@platform]["hadoop_jar"]
    main_class = @benchmark_config["platformspec"][@platform]["main_class"]
    run_options = @benchmark_config["run_options"]
    input = @benchmark_config["platformspec"][@platform]["input"]
    output = @benchmark_config["platformspec"][@platform]["output"]
    output = "#{output}/#{Time.now.to_i}" if @uniquify
    #initalize the *Commands class
    #scp the hadoop jar
    logger.debug "uploading #{local_jar} to #{hadoop_jar}"
    @scp_uploader.upload local_jar, hadoop_jar unless local_jar.nil?
    logger.debug "Done copying jar"
    #hdfs cleanup
    begin
      @ssh_command.execute "hadoop fs #{cleanup_command} #{output}" unless cleanup_command.nil?
      logger.debug "Cleaned hdfs"
    rescue => e
      logger.warn "Exception while cleaning: #{e.backtrace}"  
    end
    #run hadoop command
    hadoop_command = "hadoop jar #{hadoop_jar} #{main_class} #{run_options} #{input} #{output}"
   
    job_status = @ssh_command.execute hadoop_command
    result = populate_output output, label 
    result.merge! job_status
    # Additional output code
     
    @writer.write result unless @writer.nil?
    result[:exit_code]
  end

  def populate_output output, label
    result = {}
    result[:label] = label
    result[:benchmark] = @benchmark_config["benchmark"]
    result[:platform] = @platform_config["platform"]
    result[:run_options] =  @benchmark_config["run_options"]
    result[:input] = @benchmark_config["platformspec"][@platform]["input"] 
    result[:output] = output
    result[:hadoop_jar] = @benchmark_config["platformspec"][@platform]["hadoop_jar"].split("/")[-1]
    result[:num_nodes] = @platform_config["hadoop_slaves"]
    result[:jobflow_id] = @platform_config["jobflow_id"]
    result[:job_num] = @validator.job_num  
    result
  end
end
