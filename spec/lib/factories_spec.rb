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

describe BenchmarkMaker, "#load_factory" do
  benchmark_path = "resources/wikilogs-config.json"
  platform_path = "resources/emr-config.json"
  output_file = "results.csv"
  log_level = 'debug'
  benchmark_config = JSON.parse(File.read(benchmark_path))
  platform_config = JSON.parse(File.read(platform_path))
  
  it "creates a factory chain with one element" do
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
  
  it "creates a factory chain with two elements" do
    factory_chain = [MRFactory.new(benchmark_config, platform_config, output_file),
                     RemoteDistCP.new(nil, '/from', '/to')]
    factory_chain_from_loader = BenchmarkMaker.new
                                               .with_copier('/from', '/to')
                                               .uniquify?(true)
                                               .load_factory(benchmark_path, 
                                                             platform_path, 
                                                             output_file, 
                                                             log_level)
                                                
    mr_benchmark = factory_chain_from_loader.commands[0]
    copier = factory_chain_from_loader.commands[1]                                                            
    expect(factory_chain_from_loader).to be_kind_of(CommandChain)
    expect(factory_chain_from_loader.commands.size).to eq(factory_chain.size)
    expect(mr_benchmark).to be_kind_of(MRBenchmark)
    expect(copier).to be_kind_of(RemoteDistCP)
  end
end

describe "Factory classes" do
  context "constructed from mock configuration hashes" do
    platform = "emr"
    benchmark = "fake"
    benchmark_config = {}
    benchmark_config["benchmark"] = benchmark
    benchmark_config["platformspec"] = {}
    benchmark_config["platformspec"][platform] = {}
    benchmark_config["platformspec"][platform]["input"] = "someInput"
    benchmark_config["platformspec"][platform]["output"] = "someOutput"
    benchmark_config["run_option"] = "runSomething"
    benchmark_config["platformspec"][platform]["hadoop_jar"] = "/path/to/jar"

    platform_config = {}
    platform_config["platform"] = platform
    platform_config["hadoop_slaves"] = 13
    platform_config["jobflow_id"] = "j-2342"
    platform_config["host_name"] = "my.fake.host"
    platform_config["user_name"] = "fake_user"
    platform_config["ssh_private_key"] = "fake_key"
    machine = platform_config["host_name"].split(".")[0]

    output = "#{benchmark_config["platformspec"][platform]["output"]}/#{machine}"
    label = "myNewJob"
    output_file = "results.csv"
    log_level = 'debug'
    describe MRFactory, "#create_benchmark" do
      it "creates a benchmark with the provided configuration" do
        emr_factory = MRFactory.new benchmark_config, platform_config, output_file
        benchmark = emr_factory.create_benchmark
        expect(benchmark.instance_variable_get(:@validator)).to be_kind_of(MRValidator)
        expect(benchmark.instance_variable_get(:@writer)).to be_kind_of(CSVWriter)
        expect(benchmark.instance_variable_get(:@scp_uploader)).to be_kind_of(SCPUploader)
        expect(benchmark.instance_variable_get(:@ssh_command)).to be_kind_of(SSHRun)
      end
    end
  end
end
