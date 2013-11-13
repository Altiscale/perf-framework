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

require 'logging'
require 'parsers'
require 'writers'
require 'decorators'
# This class will copy the hadoop jar, clean up the output, and run an MR job
class MRBenchmark
  include Logging
  include Command

  attr_writer :writer, :parser
  def initialize(benchmark_config)
    @benchmark_config = benchmark_config
    @benchmark = @benchmark_config['benchmark']
    @description = "mr job: #{@benchmark}"
  end

  def uniquify?(uniquify)
    @uniquify = uniquify
    self
  end

  def run(prior_result)
    @platform = prior_result['platform']
    logger.debug "platform #{@platform}"
    logger.debug "prior_result #{prior_result.to_s}"
    ssh = SSHRun.new prior_result['host'], prior_result['user'], prior_result['ssh_key']
    cleanup_command =  @benchmark_config['platformspec'][@platform]['cleanup_command']
    hadoop_jar = @benchmark_config['platformspec'][@platform]['hadoop_jar']
    main_class = @benchmark_config['platformspec'][@platform]['main_class']
    run_options = @benchmark_config['platformspec'][@platform]['run_options']
    input = @benchmark_config['platformspec'][@platform]['input']
    output = @benchmark_config['platformspec'][@platform]['output']
    output = "#{output}/#{Time.now.to_i}" if @uniquify
    # hdfs cleanup
    begin
      ssh.execute "hadoop fs #{cleanup_command} #{output}" unless cleanup_command.nil?
      logger.debug 'Cleaned hdfs'
    rescue => e
      logger.warn "Exception while cleaning: #{e.backtrace}"
    end
    # run hadoop command
    hadoop_command = "hadoop jar #{hadoop_jar} #{main_class} #{run_options} #{input} #{output}"
    job_status = ssh.execute hadoop_command
    logger.debug "job_status #{job_status}"
    result = populate_output output, prior_result
    result.merge! job_status
    @writer.write result unless @writer.nil?
    result
  end

  def default_label(prior_result)
    "#{@benchmark_config["benchmark"]}"\
    "_#{prior_result["platform"]}_#{prior_result["node_type"]}"\
    "_#{prior_result["hadoop_slaves"]}"
  end

  def populate_output(output, prior_result)
    result = {}
    result[:label] = prior_result[:label].nil? ? default_label(prior_result) : prior_result[:label]
    result[:benchmark] = @benchmark_config['benchmark']
    result[:platform] = @platform
    result[:run_options] =  @benchmark_config['run_options']
    result[:input] = @benchmark_config['platformspec'][@platform]['input']
    result[:output] = output
    result[:hadoop_jar] = @benchmark_config['platformspec'][@platform]['hadoop_jar'].split('/')[-1]
    result[:node_type] = prior_result['node_type']
    result[:num_nodes] = prior_result['hadoop_slaves']
    result[:jobflow_id] = prior_result['jobflow_id']
    result[:job_num] = @parser.job_num
    result[:application_num] = @parser.application_num
    result
  end
end
