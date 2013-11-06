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
require 'decorators'

class MRFactory
  include Logging
  attr_reader :benchmark_config, :platform_config
  def initialize(benchmark_config, platform_config, output_file)
    @benchmark_config = benchmark_config
    @platform_config = platform_config
    @writer = CSVWriter.new output_file
  end

  def create_benchmark
    validator = MRValidator.new
    host = @platform_config['host_name']
    user = @platform_config['user_name']
    ssh_key = @platform_config['ssh_private_key']
    
    ssh_factory = SSHFactory.new(host, user, ssh_key, validator)
    benchmark = MRBenchmark.new(@benchmark_config, 
                                @platform_config, 
                                ssh_factory.scp, 
                                ssh_factory.ssh)
    benchmark.writer = @writer
    benchmark.validator = validator
    benchmark
  end
end

class SSHFactory
  attr_reader :ssh, :scp
  def initialize(host, user, ssh_key, validator=nil)
    @scp = SCPUploader.new(host, user, ssh_key)
    @ssh = SSHRun.new(host, user, ssh_key, validator)
  end
end

class CommandChain
  include Logging
  def initialize (*commands)
    @commands = *commands
  end

  def add(cmd)
    @commands << cmd
    self
  end

  def run result
    @commands.each do |cmd| 
      logger.info "executing #{cmd.description}" 
      result = cmd.run result
    end
    result
  end  
  
  def commands
    @commands.clone
  end
end

class BenchmarkMaker
  include Logging
  
  def uniquify?(uniquify)
    @uniquify = uniquify
    self
  end
  
  def with_copier(from, to)
    @hdfs_from = from
    @s3_to = to
    self
  end
  
  def load_factory benchmark_path, platform_path, output_file, log_level
    logger.level = log_level
    benchmark_config = JSON.parse(File.read(benchmark_path))
    platform_config = JSON.parse(File.read(platform_path))
    benchmark_name = benchmark_config["benchmark"]
    platform_name = platform_config["platform"]
    
    benchmark = MRFactory.new(benchmark_config, 
                              platform_config, 
                              output_file)
                              .create_benchmark
    benchmark.uniquify = @uniquify                          
    chain = CommandChain.new(benchmark) 
    host = platform_config['host_name']
    user = platform_config['user_name']
    ssh_key = platform_config['ssh_private_key']                          
    ssh_factory = SSHFactory.new(host, user, ssh_key, JSONParser.new)
    #May factor out to a factory later?
    chain.add( RemoteDistCP.new( ssh_factory.ssh, 
                                      @hdfs_from, 
                                      @s3_to)) unless @hdfs_from.nil? or @s3_to.nil?
    chain                                 
  end
end