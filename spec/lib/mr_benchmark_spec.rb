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

describe MRBenchmark do
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
    platform_config["job_flow_id"] = "j-2342"
    platform_config["host_name"] = "my.fake.host"
    machine = platform_config["host_name"].split(".")[0]
    
    output = "#{benchmark_config["platformspec"][platform]["output"]}/#{machine}"
    label = "myNewJob"
    
    describe MRBenchmark, "#populate_output" do
      it "returns a populated hash" do
        mock_validator = double(MRValidator)
        mock_validator.stub(:job_num).and_return(["job_122"])
       
        result = {}
        result[:label] = label
        result[:benchmark] = benchmark_config["benchmark"]
        result[:platform] = platform_config["platform"]
        result[:run_options] =  benchmark_config["run_options"]
        result[:input] = benchmark_config["platformspec"][platform]["input"]
        result[:output] = output
        result[:hadoop_jar] = benchmark_config["platformspec"][platform]["hadoop_jar"].split("/")[-1]
        result[:num_nodes] = platform_config["hadoop_slaves"]
        result[:job_flow_id] = platform_config["job_flow_id"]
        result[:job_num] = mock_validator.job_num
        benchmark = MRBenchmark.new benchmark_config, platform_config, double(SCPUploader), double(SSHRun)
        benchmark.validator = mock_validator
        expect(benchmark.populate_output(output, label)).to eq(result)
      end
    end
    
    describe MRBenchmark, "#run" do
      it "copies hadoop jar if available" do
        benchmark_config["platformspec"][platform]["local_jar"] = "someJar"
        mock_validator = double(MRValidator)
        mock_validator.stub(:job_num).and_return(["job_122"])
        mock_ssh = double(SSHRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        mock_scp = double(SCPUploader)
        mock_scp.should_receive(:upload).with(benchmark_config["platformspec"][platform]["local_jar"],\
        benchmark_config["platformspec"][platform]["hadoop_jar"])
        benchmark = MRBenchmark.new benchmark_config, platform_config, mock_scp, mock_ssh
        benchmark.validator = mock_validator
        benchmark.run
      end
      
      it "does not copy hadoop jar if not available" do
        benchmark_config["platformspec"][platform]["local_jar"] = nil
        mock_validator = double(MRValidator).as_null_object
        mock_ssh = double(SSHRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        mock_scp = double(SCPUploader)
        benchmark = MRBenchmark.new benchmark_config, platform_config, mock_scp, mock_ssh
        benchmark.validator = mock_validator
        benchmark.run
      end
      
      it "cleans output directory in hdfs" do
        benchmark_config["platformspec"][platform]["cleanup_command"] = "cleanup command"
        cleanup_command = benchmark_config["platformspec"][platform]["cleanup_command"]
        mock_validator = double(MRValidator).as_null_object
        mock_ssh = double(SSHRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        hdfs_cleanup = "hadoop fs #{cleanup_command} #{output}"
        mock_ssh.should_receive(:execute).with(hdfs_cleanup)
        mock_scp = double(SCPUploader).as_null_object
        benchmark = MRBenchmark.new benchmark_config, platform_config, mock_scp, mock_ssh
        benchmark.validator = mock_validator
        benchmark.run
      end
      
      it "swallows exception during cleaning of output directory in hdfs" do
        benchmark_config["platformspec"][platform]["cleanup_command"] = "cleanup command"
        cleanup_command = benchmark_config["platformspec"][platform]["cleanup_command"]
        mock_validator = double(MRValidator).as_null_object
        mock_ssh = double(SSHRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        hdfs_cleanup = "hadoop fs #{cleanup_command} #{output}"
        mock_ssh.stub(:execute).with(hdfs_cleanup) do
          raise "I will be swallowed"
        end
        mock_scp = double(SCPUploader).as_null_object
        benchmark = MRBenchmark.new benchmark_config, platform_config, mock_scp, mock_ssh
        benchmark.validator = mock_validator
        benchmark.run
      end
      
      it "runs a hadoop job" do
        hadoop_jar = benchmark_config["platformspec"][platform]["hadoop_jar"]
        main_class = benchmark_config["platformspec"][platform]["main_class"]
        run_options = benchmark_config["run_options"]
        input = benchmark_config["platformspec"][platform]["input"] 
  
        mock_validator = double(MRValidator).as_null_object
        mock_ssh = double(SSHRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        
        hadoop_command = "hadoop jar #{hadoop_jar} #{main_class} #{run_options} #{input} #{output}"
        job_status = {:exit_code => 0 }
        mock_ssh.stub(:execute).with(hadoop_command) do
          job_status
        end
        mock_ssh.should_receive(:execute).with(hadoop_command)
        mock_scp = double(SCPUploader).as_null_object
        benchmark = MRBenchmark.new benchmark_config, platform_config, mock_scp, mock_ssh
        benchmark.validator = mock_validator
        expect(benchmark.run label).to eq(0)
      end
    end
  end
end