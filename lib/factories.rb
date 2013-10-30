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

# Creates the benchmark and associated classes
require 'json'
require 'validators'
require 'mr_benchmark'
require 'writers'
require 'logging'

class Factory 
  include Logging
  attr_reader :benchmark_config, :platform_config
  
  def initialize(benchmark_config, platform_config, output_file, log_level)
    @benchmark_config = benchmark_config
    @platform_config = platform_config
    logger.level = log_level
    @writer = CSVWriter.new output_file
  end
  
  def create_benchmark   
  end
  
  def create_ssh_commands validator
    host = @platform_config["host_name"]
    user = @platform_config["user_name"]
    ssh_key = @platform_config["ssh_private_key"]
    scp_uploader = SCPUploader.new(host, user, ssh_key)
    ssh_command = SSHRun.new(host, user, ssh_key, validator)
    return scp_uploader, ssh_command
  end
end

class EMR_Factory < Factory

  def create_benchmark
    validator = MRValidator.new 
    scp_uploader, ssh_command = create_ssh_commands validator
    benchmark = MRBenchmark.new(@benchmark_config, @platform_config, scp_uploader, ssh_command)
    benchmark.writer = @writer
    benchmark.validator = validator
    benchmark  
  end
end

class ALTI_Factory < Factory

  def create_benchmark
    validator = MRValidator.new 
    scp_uploader, ssh_command = create_ssh_commands validator
    benchmark = MRBenchmark.new(@benchmark_config, @platform_config, scp_uploader, ssh_command)
    benchmark.writer = @writer
    benchmark.validator = validator
    benchmark  
  end
end

class FactoryLoader
  include Logging
  def load_factory benchmark_path, platform_path, output_file, log_level
    benchmark_config = JSON.parse(File.read(benchmark_path))
    platform_config = JSON.parse(File.read(platform_path))
    benchmark_name = benchmark_config["benchmark"]
    platform_name = platform_config["platform"]
    # logger.debug "initialized #{benchmark_name} #{platform_name}"
    factory_class = platform_name.upcase + '_Factory'
    factory_class = self.class.const_get(factory_class)
    factory_class.new benchmark_config, platform_config, output_file, log_level
  end
end