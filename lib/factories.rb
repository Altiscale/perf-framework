#!/usr/bin/env ruby
require 'json'
require 'validators'
require 'mr_benchmark'
require 'writers'

class Factory 
  include Logging
  attr_reader :benchmark_config, :platform_config
  
  def initialize(benchmark_config, platform_config, output_file, log_level)
    @benchmark_config = benchmark_config
    @platform_config = platform_config
    logger.level = log_level
    @writer = CSV_Writer.new output_file
  end
  
  def create_benchmark   
  end
  
  def create_ssh_commands validator
    host = @platform_config["hostName"]
    user = @platform_config["userName"]
    ssh_key = @platform_config["sshPrivateKey"]
    scpUploader = ScpUploader.new(host, user, ssh_key)
    sshCommand = SshRun.new(host, user, ssh_key, validator)
    return scpUploader, sshCommand
  end
end

class EMR_Factory < Factory

  def create_benchmark
    validator = MR_Validator.new 
    benchmark = MR_Benchmark.new(@benchmark_config, @platform_config)
    benchmark.writer = @writer
    benchmark.validator = validator
    scpUploader, sshCommand = create_ssh_commands validator
    benchmark.scpUploader = scpUploader
    benchmark.sshCommand = sshCommand
    benchmark  
  end
end

class ALTI_Factory < Factory

  def create_benchmark
    validator = MR_Validator.new 
    benchmark = MR_Benchmark.new(@benchmark_config, @platform_config)
    benchmark.writer = @writer
    benchmark.validator = validator
    scpUploader, sshCommand = create_ssh_commands validator
    benchmark.scpUploader = scpUploader
    benchmark.sshCommand = sshCommand
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