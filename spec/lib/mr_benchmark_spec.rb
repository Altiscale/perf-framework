# Copyright (c) 2013 Altiscale, inc.
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
    # http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License
require 'spec_helper'
describe MRBenchmark do
  context 'constructed from mock configuration hashes' do
    platform = 'emr'
    benchmark = 'fake'
    benchmark_config = {}
    benchmark_config['benchmark'] = benchmark
    benchmark_config['platformspec'] = {}
    benchmark_config['platformspec'][platform] = {}
    benchmark_config['platformspec'][platform]['input'] = 'someInput'
    benchmark_config['platformspec'][platform]['output'] = 'someOutput'
    benchmark_config['run_option'] = 'runSomething'
    benchmark_config['platformspec'][platform]['hadoop_jar'] = '/path/to/runjob.jar'
    platform_config = {}
    platform_config['platform'] = platform
    platform_config['hadoop_slaves'] = 13
    platform_config['jobflow_id'] = 'j-2342'
    platform_config['host_name'] = 'my.fake.host'
    platform_config['node_type'] = 'm2_very_large'
    label = 'myNewJob'
    output = benchmark_config['platformspec'][platform]['output']
    describe MRBenchmark, '#populate_output' do
      it 'returns a populated hash' do
        mock_parser = double(MRValidator)
        mock_parser.stub(:job_num).and_return('job_122')
        mock_parser.stub(:application_num).and_return('application_num')
        result = {}
        result[:label] = label
        result[:benchmark] = benchmark_config['benchmark']
        result[:platform] = platform_config['platform']
        result[:run_options] =  benchmark_config['run_options']
        result[:input] = benchmark_config['platformspec'][platform]['input']
        result[:output] = benchmark_config['platformspec'][platform]['output']
        result[:hadoop_jar] = benchmark_config['platformspec'][platform]['hadoop_jar'].split('/')[-1]
        result[:num_nodes] = platform_config['hadoop_slaves']
        result[:node_type] = platform_config['node_type']
        result[:jobflow_id] = platform_config['jobflow_id']
        result[:job_num] = mock_parser.job_num
        result[:application_num] = mock_parser.application_num
        benchmark = MRBenchmark.new benchmark_config, platform_config, double(SSHRun)
        benchmark.parser = mock_parser
        expect(benchmark.populate_output(output, label: label)).to eq(result)
      end
    end

    describe MRBenchmark, '#run' do
      it 'cleans output directory in hdfs' do
        benchmark_config['platformspec'][platform]['cleanup_command'] = 'cleanup command'
        cleanup_command = benchmark_config['platformspec'][platform]['cleanup_command']
        mock_parser = double(MRValidator).as_null_object
        mock_ssh = double(SSHRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        hdfs_cleanup = "hadoop fs #{cleanup_command} #{output}"
        mock_ssh.should_receive(:execute).with(hdfs_cleanup)
        benchmark = MRBenchmark.new benchmark_config, platform_config, mock_ssh
        benchmark.parser = mock_parser
        benchmark.run
      end

      it 'swallows exception during cleaning of output directory in hdfs' do
        benchmark_config['platformspec'][platform]['cleanup_command'] = 'cleanup command'
        cleanup_command = benchmark_config['platformspec'][platform]['cleanup_command']
        mock_parser = double(MRValidator).as_null_object
        mock_ssh = double(SSHRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        hdfs_cleanup = "hadoop fs #{cleanup_command} #{output}"
        mock_ssh.stub(:execute).with(hdfs_cleanup) do
          fail 'I will be swallowed'
        end
        benchmark = MRBenchmark.new benchmark_config, platform_config, mock_ssh
        benchmark.parser = mock_parser
        benchmark.run
      end

      it 'runs a hadoop job' do
        hadoop_jar = benchmark_config['platformspec'][platform]['hadoop_jar']
        main_class = benchmark_config['platformspec'][platform]['main_class']
        run_options = benchmark_config['run_options']
        input = benchmark_config['platformspec'][platform]['input']

        mock_parser = double(MRValidator).as_null_object
        mock_ssh = double(SSHRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end

        hadoop_command = "hadoop jar #{hadoop_jar} #{main_class} #{run_options} #{input} #{output}"
        job_status = { exit_code: 0 }
        mock_ssh.stub(:execute).with(hadoop_command) do
          job_status
        end
        mock_ssh.should_receive(:execute).with(hadoop_command)
        benchmark = MRBenchmark.new benchmark_config, platform_config, mock_ssh
        benchmark.parser = mock_parser
        result = {}
        result[:label] =  "#{benchmark_config["benchmark"]}"\
                          "_#{platform_config["platform"]}_#{platform_config["node_type"]}"\
                          "_#{platform_config["hadoop_slaves"]}"
        result[:benchmark] = benchmark_config['benchmark']
        result[:platform] = platform_config['platform']
        result[:run_options] =  benchmark_config['run_options']
        result[:input] = benchmark_config['platformspec'][platform]['input']
        result[:output] = benchmark_config['platformspec'][platform]['output']
        result[:hadoop_jar] = benchmark_config['platformspec'][platform]['hadoop_jar'].split('/')[-1]
        result[:num_nodes] = platform_config['hadoop_slaves']
        result[:node_type] = platform_config['node_type']
        result[:jobflow_id] = platform_config['jobflow_id']
        result[:job_num] = mock_parser.job_num
        result[:application_num] = mock_parser.application_num
        result[:exit_code] = 0
        expect(benchmark.run {}).to eq(result)
      end
    end
  end
end
