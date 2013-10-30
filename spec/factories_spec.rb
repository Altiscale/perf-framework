require 'spec_helper'

describe FactoryLoader, "#load_factory" do
  it "parses benchmark and platform json files" do
    benchmark_path = "resources/wikilogs-config.json"
    platform_path = "resources/emr-config.json"
    output_file = "results.csv"
    log_level = "some log level"
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
    benchmark_config["runOption"] = "runSomething"
    benchmark_config["platformspec"][platform]["hadoopJar"] = "/path/to/jar"

    platform_config = {}
    platform_config["platform"] = platform
    platform_config["hadoopSlaves"] = 13
    platform_config["jobflowId"] = "j-2342"
    platform_config["hostName"] = "my.fake.host"
    platform_config["userName"] = "fake_user"
    platform_config["sshPrivateKey"] = "fake_key"
    machine = platform_config["hostName"].split(".")[0]

    output = "#{benchmark_config["platformspec"][platform]["output"]}/#{machine}"
    label = "myNewJob"
    output_file = "results.csv"
    log_level = "some log level"
    describe EMR_Factory, "#create_benchmark" do
      it "creates a benchmark with the provided configuration" do
        emr_factory = EMR_Factory.new benchmark_config, platform_config, output_file, log_level
        benchmark = emr_factory.create_benchmark
        expect(benchmark.validator).to be_kind_of(MR_Validator)
        expect(benchmark.writer).to be_kind_of(CSV_Writer)
        expect(benchmark.scpUploader).to be_kind_of(ScpUploader)
        expect(benchmark.sshCommand).to be_kind_of(SshRun)
      end
    end

    describe ALTI_Factory, "#create_benchmark" do
      it "creates a benchmark with the provided configuration" do
        alti_factory = ALTI_Factory.new benchmark_config, platform_config, output_file, log_level
        benchmark = alti_factory.create_benchmark
        expect(benchmark.validator).to be_kind_of(MR_Validator)
        expect(benchmark.writer).to be_kind_of(CSV_Writer)
        expect(benchmark.scpUploader).to be_kind_of(ScpUploader)
        expect(benchmark.sshCommand).to be_kind_of(SshRun)
      end
    end
  end
end
