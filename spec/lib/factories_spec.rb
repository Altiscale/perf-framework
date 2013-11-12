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
require 'tempfile'
describe BenchmarkMaker, '#load_factory' do
  output_file = 'results.csv'
  log_level = 'debug'
  it 'creates a factory chain with one element' do
    benchmark_path = 'resources/bare-wikilogs-config.json'
    platform_path = 'resources/emr-config.json'
    benchmark_config = JSON.parse(File.read(benchmark_path))
    platform_config = JSON.parse(File.read(platform_path))
    factory_chain = [MRFactory.new(benchmark_config, platform_config, output_file)]
    factory_chain_from_loader = BenchmarkMaker.new
                                              .uniquify?(true)
                                              .load_factory(benchmark_path,
                                                            platform_path,
                                                            output_file,
                                                            log_level)

    mr_benchmark = factory_chain_from_loader.commands[0]
    expect(factory_chain_from_loader).to be_kind_of(CommandChain)
    expect(mr_benchmark).to be_kind_of(MRBenchmark)
    expect(factory_chain_from_loader.commands.size).to eq(factory_chain.size)
    expect(mr_benchmark.instance_variable_get(:@benchmark_config)).to eq(benchmark_config)
    expect(mr_benchmark.instance_variable_get(:@platform_config)).to eq(platform_config)
  end

  it 'creates a factory chain with more than one elements' do
    benchmark_path = 'resources/wikilogs-config.json'
    platform_path = 'resources/emr-config.json'
    benchmark_config = JSON.parse(File.read(benchmark_path))
    platform_config = JSON.parse(File.read(platform_path))
    factory_chain = [RemoteDistCP.new(nil, 's3://dp-138-perf/jobjars/WikiStats_lzo.jar', '/jobjars/WikiStats.jar'),
                     RemoteDistCP.new(nil, 's3://wikilogs-5gb', '/wikilogs-5gb'),
                     MRFactory.new(benchmark_config, platform_config, output_file),
                     RemoteDistCP.new(nil, '/tmp/hadoop-yarn/staging/history', 's3://dp-138-perf/jhist')]
    factory_chain_from_loader = BenchmarkMaker.new
                                               .uniquify?(true)
                                               .load_factory(benchmark_path,
                                                             platform_path,
                                                             output_file,
                                                             log_level)
    mr_benchmark = factory_chain_from_loader.commands[2]
    copier = factory_chain_from_loader.commands[1]
    expect(factory_chain_from_loader).to be_kind_of(CommandChain)
    expect(factory_chain_from_loader.commands.size).to eq(factory_chain.size)
    expect(mr_benchmark).to be_kind_of(MRBenchmark)
    expect(copier).to be_kind_of(RemoteDistCP)
  end
end

describe 'Factory classes' do
  context 'constructed from mock configuration hashes' do
    platform = '<platform_name>'
    benchmark = 'fake'
    benchmark_config = {}
    benchmark_config['benchmark'] = benchmark
    benchmark_config['platformspec'] = {}
    benchmark_config['platformspec'][platform] = {}
    benchmark_config['platformspec'][platform]['input'] = 'someInput'
    benchmark_config['platformspec'][platform]['output'] = 'someOutput'
    benchmark_config['run_option'] = 'runSomething'
    benchmark_config['platformspec'][platform]['hadoop_jar'] = '/path/to/jar'

    platform_config = {}
    platform_config['platform'] = platform
    platform_config['hadoop_slaves'] = 13
    platform_config['jobflow_id'] = 'j-2342'
    platform_config['host_name'] = 'my.fake.host'
    platform_config['user_name'] = 'fake_user'
    platform_config['ssh_private_key'] = 'fake_key'
    output_file = 'results.csv'
    describe MRFactory, '#create_benchmark' do
      it 'creates a benchmark with the provided configuration' do
        emr_factory = MRFactory.new benchmark_config, platform_config, output_file
        benchmark = emr_factory.create_benchmark
        expect(benchmark.instance_variable_get(:@parser)).to be_kind_of(MRValidator)
        expect(benchmark.instance_variable_get(:@writer)).to be_kind_of(CSVWriter)
        expect(benchmark.instance_variable_get(:@ssh_command)).to be_kind_of(SSHRun)
      end
    end
  end
end
