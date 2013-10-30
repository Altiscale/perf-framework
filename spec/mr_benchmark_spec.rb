require 'spec_helper'

describe MR_Benchmark do
  context "constructed from mock configuration hashes" do
    platform = "emr"
    benchmark = "fake"
    benchmark_config = {}
    benchmark_config["benchmark"] = benchmark
    benchmark_config["platformspec"] = {}
    benchmark_config["platformspec"][platform] = {}
    benchmark_config["platformspec"][platform]["input"] = "someInput"
    benchmark_config["platformspec"][platform]["output"] = "someOutput"
    benchmark_config["runOption"] = "runSomething"
    benchmark_config["platformspec"][platform]["hadoopJar"] = "/path/to/jar"
    
    platform_config = {}
    platform_config["platform"] = platform
    platform_config["hadoopSlaves"] = 13
    platform_config["jobflowId"] = "j-2342"
    platform_config["hostName"] = "my.fake.host"
    machine = platform_config["hostName"].split(".")[0]
    
    output = "#{benchmark_config["platformspec"][platform]["output"]}/#{machine}"
    label = "myNewJob"
    
    describe MR_Benchmark, "#populate_output" do
      it "returns a populated hash" do
        mock_validator = double(MR_Validator)
        mock_validator.stub(:jobNum).and_return(["job_122"])
        benchmark = MR_Benchmark.new benchmark_config, platform_config
        benchmark.validator = mock_validator
        result = {}
        result[:label] = label
        result[:benchmark] = benchmark_config["benchmark"]
        result[:platform] = platform_config["platform"]
        result[:runOptions] =  benchmark_config["runOptions"]
        result[:input] = benchmark_config["platformspec"][platform]["input"]
        result[:output] = output
        result[:hadoopJar] = benchmark_config["platformspec"][platform]["hadoopJar"].split("/")[-1]
        result[:numNodes] = platform_config["hadoopSlaves"]
        result[:jobflowId] = platform_config["jobflowId"]
        result[:jobNum] = mock_validator.jobNum

        expect(benchmark.populate_output(output, label)).to eq(result)
      end
    end
    
    describe MR_Benchmark, "#run" do
      it "copies hadoop jar if available" do
        benchmark_config["platformspec"][platform]["localJar"] = "someJar"
        mock_validator = double(MR_Validator)
        mock_validator.stub(:jobNum).and_return(["job_122"])
        benchmark = MR_Benchmark.new benchmark_config, platform_config
        benchmark.validator = mock_validator
        mock_ssh = double(SshRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        benchmark.sshCommand = mock_ssh
        mock_scp = double(ScpUploader)
        mock_scp.should_receive(:upload).with(benchmark_config["platformspec"][platform]["localJar"],\
        benchmark_config["platformspec"][platform]["hadoopJar"])
        benchmark.scpUploader = mock_scp
        benchmark.run
      end
      
      it "does not copy hadoop jar if not available" do
        benchmark_config["platformspec"][platform]["localJar"] = nil
        mock_validator = double(MR_Validator).as_null_object

        benchmark = MR_Benchmark.new benchmark_config, platform_config
        benchmark.validator = mock_validator
        mock_ssh = double(SshRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        benchmark.sshCommand = mock_ssh
        mock_scp = double(ScpUploader)
        benchmark.scpUploader = mock_scp
        benchmark.run
      end
      
      it "cleans output directory in hdfs" do
        benchmark_config["platformspec"][platform]["cleanupCommand"] = "cleanup command"
        cleanupCommand = benchmark_config["platformspec"][platform]["cleanupCommand"]
        mock_validator = mock(MR_Validator).as_null_object
        benchmark = MR_Benchmark.new benchmark_config, platform_config
        benchmark.validator = mock_validator
        mock_ssh = double(SshRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        hdfs_cleanup = "hadoop fs #{cleanupCommand} #{output}"
        mock_ssh.should_receive(:execute).with(hdfs_cleanup)
        benchmark.sshCommand = mock_ssh
        mock_scp = double(ScpUploader)
        benchmark.scpUploader = mock_scp
        benchmark.run
      end
      
      it "swallows exception during cleaning of output directory in hdfs" do
        benchmark_config["platformspec"][platform]["cleanupCommand"] = "cleanup command"
        cleanupCommand = benchmark_config["platformspec"][platform]["cleanupCommand"]
        mock_validator = mock(MR_Validator).as_null_object
        benchmark = MR_Benchmark.new benchmark_config, platform_config
        benchmark.validator = mock_validator
        mock_ssh = double(SshRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        hdfs_cleanup = "hadoop fs #{cleanupCommand} #{output}"
        mock_ssh.stub(:execute).with(hdfs_cleanup) do
          raise "I will be swallowed"
        end
        benchmark.sshCommand = mock_ssh
        mock_scp = double(ScpUploader)
        benchmark.scpUploader = mock_scp
        benchmark.run
      end
      
      it "runs a hadoop job" do
        hadoopJar = benchmark_config["platformspec"][platform]["hadoopJar"]
        mainClass = benchmark_config["platformspec"][platform]["mainClass"]
        runOptions = benchmark_config["runOptions"]
        input = benchmark_config["platformspec"][platform]["input"] 
  
        mock_validator = mock(MR_Validator).as_null_object
        benchmark = MR_Benchmark.new benchmark_config, platform_config
        benchmark.validator = mock_validator
        mock_ssh = double(SshRun)
        mock_ssh.stub(:execute).with(an_instance_of(String)) do
          {}
        end
        
        hadoop_command = "hadoop jar #{hadoopJar} #{mainClass} #{runOptions} #{input} #{output}"
        job_status = {:exit_code => 0 }
        mock_ssh.stub(:execute).with(hadoop_command) do
          job_status
        end
        mock_ssh.should_receive(:execute).with(hadoop_command)
        benchmark.sshCommand = mock_ssh
        mock_scp = double(ScpUploader)
        benchmark.scpUploader = mock_scp
        expect(benchmark.run label).to include(job_status)
      end
    end
  end
end
