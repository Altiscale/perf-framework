#!/usr/bin/env ruby
# This creates an emr cluster, runs a benchmark job, and terminates the cluster
# This is throwaway code(read: full of hacks) that would be replaced by an emr launcher in perf-framework

require 'optparse'
require 'ostruct'
require 'rubygems'
require 'json'
require 'pp'
require 'logger'
require 'tempfile'
require 'erb'
LOG_LEVELS = %w(
  debug
  info
  warn
  error
  fatal
)
ONE_HUNDRED_MB = 100 * 1024 * 1024
EMR_STATUS_CHECK_INTERVAL = 20
def parseOptions(logger)
  settings = OpenStruct.new
  options = OptionParser.new do |opts|
    settings.log_level = 'info'
    settings.on_demand = false
    settings.preserve = false
    settings.uniquifier = false
    settings.output_file = "results.csv"
    settings.log_uri = "s3://dp-138-perf"
    
    opts.on('-e', '--emr-erb EMR_ERB',
            'emr configuration') do |emr_erb|
      settings.emr_erb = emr_erb
    end
    
    opts.on('-f', '--from-dir HDFS or S3 Directory',
            'HDFS or S3 Directory path to copy from') do |from_dir|
      settings.from_dir = from_dir
    end
    
    opts.on('-x', '--benchmark-runner PATH',
            'path to benchmark runner') do |perf_framework_dir|
      settings.perf_framework_dir = perf_framework_dir
    end
    
    opts.on('-t', '--to-dir HDFS or S3 Directory',
            'HDFS or S3 Directory to copy to') do |to_dir|
      settings.to_dir = to_dir
    end
    
    opts.on('-b', '--benchmark-path BENCHMARK_JSON_PATH',
            'path to benchmark.json') do |benchmark_path|
      settings.benchmark_path = benchmark_path
    end
    
    opts.on('-c', '--job_flow_name NAME',
            'Name of the emr environment to deploy.') do |job_flow_name|
      settings.job_flow_name = job_flow_name
    end

    opts.on('-k', '--ssh-key KEY',
            'SSH key for target cluster.') do |ssh_key|
      settings.ssh_key = ssh_key
    end
    
    opts.on('-s', '--instance-size INSTANCE_SIZE',
            'Size of the aws instance(m1.small, m1.medium, etc.)') do |instance_size|
      settings.instance_size = instance_size
    end
    
    opts.on('-n', '--num-instances NUM_INSTANCES',
            'Number of instances(separate from the master).') do |num_instances|
      settings.num_instances = num_instances
    end
    
    opts.on('-m', '--bid-price BID_PRICE',
            'Spot instance bid price.') do |bid_price|
      settings.bid_price = bid_price
    end
    
    opts.on('--job-label LABEL',
            'Label of the job on the results file.') do |job_label|
      settings.job_label = job_label
    end
    
    opts.on('--log-uri LOG_URI',
            'The s3 log uri for storing emr logs.') do |log_uri|
      settings.log_uri = log_uri
    end
    
    opts.on('--uniquify',
            'uniquify output') do |uniquify|
      settings.uniquify = true
    end
    
    opts.on('--on-demand',
            'The flag to allocate on demand instances.') do |on_demand|
      settings.on_demand = true
    end
    
    opts.on('-o', '--output-file',
            'Path to the output file.') do |output_file|
      settings.output_file = output_file
    end
    
    opts.on('-p', '--preserve',
            'The flag to preserve instances.') do |preserve|
      settings.preserve = true
    end
    
    opts.on('-l', '--log-level LEVEL',
            "Log level: #{LOG_LEVELS.join(', ')}") do |log_level|
      settings.log_level = log_level
    end
    
     opts.on('--times NUMBER',
            "Number of times to run") do |times|
      settings.times = times
    end
  
    opts.on('-h', '--help', 'Show this help.') do
      logger.info options.to_s
      exit
    end
  end

  # Parse options and make sure we have our required ones.
  begin
    options.parse!
    mandatory = [:job_flow_name, :instance_size, :num_instances, :ssh_key]
    missing = []
    mandatory.each do |setting|
      if !settings.marshal_dump.has_key?(setting)
        missing << setting.to_s.gsub('_', '-')
      end
    end
    if !missing.empty?
      logger.warn "Missing required options: #{missing.join(', ')}"
      logger.warn options.to_s
      exit!
    end
    if !LOG_LEVELS.include?(settings.log_level)
      logger.warn "Invalid log_level (#{settings.log_level}).  Valid values: #{LOG_LEVELS.join(', ')}"
      exit!
    end
  rescue OptionParser::InvalidOption, OptionParser::MissingArgument
    logger.error $!.to_s
    logger.error options.to_s
    exit!
  end
  settings
end

def allocate_emr_instance(settings, logger)
  
   allocate_emr = "emr --create --name #{settings.job_flow_name} --alive --instance-group MASTER --instance-type #{settings.instance_size} "\
   "--instance-count 1 --bid-price #{settings.bid_price} --instance-group CORE --instance-type #{settings.instance_size} --instance-count #{settings.num_instances} "\
   "--bid-price #{settings.bid_price} --bootstrap-action s3://elasticmapreduce/bootstrap-actions/install-ganglia "\
   "--bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop --args \"-m,mapred.reduce.tasks.speculative.execution=false\""

   allocate_emr = "emr --create --name #{settings.job_flow_name} --alive --instance-group MASTER --instance-type #{settings.instance_size} "\
   "--instance-count 1 --instance-group CORE --instance-type #{settings.instance_size} --instance-count #{settings.num_instances} "\
   "--bootstrap-action s3://elasticmapreduce/bootstrap-actions/install-ganglia "\
   "--bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop --args \"-m,mapred.reduce.tasks.speculative.execution=false\"" unless !settings.on_demand
  allocate_emr = "#{allocate_emr} --log-uri #{settings.log_uri}"
  allocate_emr = "#{allocate_emr} --ami-version 3.0.0"
  allocate_emr = "#{allocate_emr} > /tmp/emr_jobflow_id"
  logger.info "allocating emr: #{allocate_emr}"
  logAndExit(logger, "Allocate emr failed: #{allocate_emr}") unless system("#{allocate_emr}")
  jobflow_id = `cat /tmp/emr_jobflow_id | awk '{print $4}'`
  jobflow_id = jobflow_id.gsub("\n",'')
  begin
    status_j = `emr --describe -j #{jobflow_id}`
    status_j = JSON.parse status_j
    status = status_j["JobFlows"][0]["ExecutionStatusDetail"]["State"]
    logger.debug "emr #{jobflow_id} status #{status}"
    sleep EMR_STATUS_CHECK_INTERVAL
  end while !((status.include? "WAITING") || (status.include? "FAILED"))
  logAndExit(logger, "Failed to allocate emr: #{jobflow_id}") unless status.include? "WAITING"
  logger.info "Successfully allocated emr #{status}"
  machine_dns = status_j["JobFlows"][0]["Instances"]["MasterPublicDnsName"]
  ret = [jobflow_id, machine_dns]
  ret
end

def set_cluster_visible(settings, logger, jobflow_id)
  set_visible_command = "emr --set-visible-to-all-users true -j #{jobflow_id}"
  logAndExit(logger, "Failed to set visible: #{jobflow_id}") unless system("#{set_visible_command}")
end

def tag_cluster(settings, logger, jobflow_id)
  instance_cmd = "ec2-describe-tags --filter \"resource-type=instance\" --filter \"key=aws:elasticmapreduce:job-flow-id\" --filter \"value=#{jobflow_id}\" --region us-west-2 | awk '{print $3}'"
  logger.debug "cmd: #{instance_cmd}"
  instances = `#{instance_cmd}`
  instance_array = instances.split(/\n/).reject(&:empty?)
  logger.debug "instances #{instance_array}"
  user = `echo $(whoami)`
  user = user.gsub("\n",'')
  instance_array.each do |instance|
    tagging_cmd = "ec2-create-tags #{instance} --tag \"User=#{user}@altiscale.com\" --tag \"Customer=Engineering\" --region us-west-2"
    logger.debug "tagging #{tagging_cmd}"
    logAndExit(logger, "Failed to tag: #{instance}") unless system(tagging_cmd)
  end   
end

def logAndExit(logger, message)
  logger.fatal(message)
  raise "Fatal error: #{message}"
end 

def create_emr_conf(settings, logger, machine_dns)
  @machine_dns = machine_dns
  @private_key = settings.ssh_key
  @instance_size = settings.instance_size
  @instance_count = settings.num_instances
  
  emr_json = ERB.new(File.read("#{settings.emr_erb}")).result(binding)
  emr_config = Tempfile.new('emr_config')
  File.open("#{emr_config.path}","w") do |f|
    f.write(JSON.pretty_generate(JSON.parse(emr_json)))
  end
  emr_config.path
end

def terminate_emr(settings, logger, jobflow_id)
  
  terminate_cmd = "emr --terminate -j #{jobflow_id}"
  logger.info "about to terminate emr: #{jobflow_id}"
  logAndExit(logger, "Failed to terminate #{terminate_cmd}") unless system(terminate_cmd)
  logger.debug "successfully terminated #{terminate_cmd}"
end

logger = Logger.new(STDOUT, shift_size = ONE_HUNDRED_MB)
log_map = Hash[LOG_LEVELS.map.with_index.to_a]
settings = parseOptions(logger)
logger.level = log_map[settings.log_level]
logger.info "Starting emr allocation"
status = allocate_emr_instance(settings, logger)
jobflow_id = status[0]
machine_dns = status[1]
begin
  logger.debug "job flow #{jobflow_id} machine #{machine_dns}"
  set_cluster_visible(settings, logger, jobflow_id)
  tag_cluster(settings, logger, jobflow_id)
  #run a job
  # for now call benchmark
  emr_conf_path = create_emr_conf(settings, logger, machine_dns)
  run_job_cmd = "ruby -I lib #{settings.perf_framework_dir}/bin/benchmark_runner.rb "\
  "-b #{settings.benchmark_path} -p #{emr_conf_path} -l #{settings.log_level} "\
  "-o #{settings.output_file} -j #{settings.job_label}"
  run_job_cmd = "#{run_job_cmd} -u" if settings.uniquify
  run_job_cmd = "#{run_job_cmd} -f #{settings.from_dir} -t #{settings.to_dir}/#{jobflow_id}" unless settings.from_dir.nil? or settings.to_dir.nil?
  num = 0
  job_ran_successfully = false
  logger.info "will run #{run_job_cmd} #{settings.times} times"
  settings.times.to_i.times do
    num += 1
    logger.info "#{num}) executing #{run_job_cmd}"
    job_ran_successfully &= system run_job_cmd
  end
  #collect data
  # call shell scripts
  #clean it up
ensure
  terminate_emr(settings, logger, jobflow_id) unless settings.preserve
  exit 1 if !job_ran_successfully.nil? && !job_ran_successfully
end