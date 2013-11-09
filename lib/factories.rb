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
require 'parsers'
require 'mr_benchmark'
require 'writers'
require 'logging'
require 'decorators'

# Creates the mr_benchmark
class MRFactory
  include Logging
  attr_reader :benchmark_config, :platform_config
  def initialize(benchmark_config, platform_config, output_file)
    @benchmark_config = benchmark_config
    @platform_config = platform_config
    @writer = CSVWriter.new output_file
  end

  def create_benchmark
    parser = MRValidator.new
    host = @platform_config['host_name']
    user = @platform_config['user_name']
    ssh_key = @platform_config['ssh_private_key']

    ssh_factory = SSHFactory.new(host, user, ssh_key, parser)
    benchmark = MRBenchmark.new(@benchmark_config,
                                @platform_config,
                                ssh_factory.ssh)
    benchmark.writer = @writer
    benchmark.parser = parser
    benchmark
  end
end

# Creates an ssh client
class SSHFactory
  attr_reader :ssh, :scp
  def initialize(host, user, ssh_key, parser = nil)
    @scp = SCPUploader.new(host, user, ssh_key)
    @ssh = SSHRun.new(host, user, ssh_key, parser)
  end
end

# Creates the benchmark
class BenchmarkMaker
  include Logging

  def uniquify?(uniquify)
    @uniquify = uniquify
    self
  end

  def transfers(transfers_array, ssh_factory)
    transfer_list = []
    transfers_array.each do |transfer|
      force = !transfer['force'].nil? && transfer['force'] == 'true'
      transfer_list << RemoteDistCP.new(ssh_factory.ssh, transfer['from'], transfer['to'], force) if transfer['scp'].nil?
      transfer_list << RemoteSCP.new(ssh_factory.scp, transfer['from'], transfer['to']) unless transfer['scp'].nil?
    end unless transfers_array.nil?
    logger.debug "adding #{transfer_list.to_s}"
    transfer_list
  end

  def load_factory(benchmark_path, platform_path, output_file, log_level)
    logger.level = log_level
    benchmark_config = JSON.parse(File.read(benchmark_path))
    platform_config = JSON.parse(File.read(platform_path))
    benchmark = MRFactory.new(benchmark_config,
                              platform_config,
                              output_file)
                              .create_benchmark
    benchmark.uniquify = @uniquify
    host = platform_config['host_name']
    user = platform_config['user_name']
    ssh_key = platform_config['ssh_private_key']
    ssh_factory = SSHFactory.new(host, user, ssh_key)

    platform = platform_config['platform']
    logger.debug "platform #{platform}"
    pre_transfers = benchmark_config['platformspec'][platform]['pre_transfers']
    post_transfers = benchmark_config['platformspec'][platform]['post_transfers']
    chain = CommandChain.new
    chain.add transfers pre_transfers, ssh_factory
    chain.add benchmark
    chain.add transfers post_transfers, ssh_factory
    chain
  end
end
