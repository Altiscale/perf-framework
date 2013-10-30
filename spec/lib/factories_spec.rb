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
describe FactoryLoader, "#load_factory" do
  it "parses benchmark and platform json files" do
    benchmark_path = "resources/wikilogs-config.json"
    platform_path = "resources/emr-config.json"
    output_file = "results.csv"
    log_level = 'debug'
    puts "lg: #{log_level}"
    result_factory = EMR_Factory.new JSON.parse(File.read(benchmark_path)), JSON.parse(File.read(platform_path)), output_file, log_level
    factory_from_loader = FactoryLoader.new().load_factory benchmark_path, platform_path, output_file, log_level
    expect(factory_from_loader).to be_kind_of(EMR_Factory)
    expect(factory_from_loader.benchmark_config).to eq(result_factory.benchmark_config)
    expect(factory_from_loader.platform_config).to eq(result_factory.platform_config)
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
    describe EMR_Factory, "#create_benchmark" do
      it "creates a benchmark with the provided configuration" do
        emr_factory = EMR_Factory.new benchmark_config, platform_config, output_file, log_level
        benchmark = emr_factory.create_benchmark
        expect(benchmark.validator).to be_kind_of(MRValidator)
        expect(benchmark.writer).to be_kind_of(CSVWriter)
        expect(benchmark.scp_uploader).to be_kind_of(SCPUploader)
        expect(benchmark.ssh_command).to be_kind_of(SSHRun)
      end
    end

    describe ALTI_Factory, "#create_benchmark" do
      it "creates a benchmark with the provided configuration" do
        alti_factory = ALTI_Factory.new benchmark_config, platform_config, output_file, log_level
        benchmark = alti_factory.create_benchmark
        expect(benchmark.validator).to be_kind_of(MRValidator)
        expect(benchmark.writer).to be_kind_of(CSVWriter)
        expect(benchmark.scp_uploader).to be_kind_of(SCPUploader)
        expect(benchmark.ssh_command).to be_kind_of(SSHRun)
      end
    end
  end
end
