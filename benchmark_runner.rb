#!/usr/bin/env ruby
# This script runs the benchmark. It requires the platform and benchmark json files

require 'perf_benchmark'
require 'logger'
require 'optparse'
require 'ostruct'
LOG_LEVELS = [
  'debug',
  'info',
  'warn',
  'error',
  'fatal'
]
ONE_HUNDRED_MB = 100 * 1024 * 1024
def parseOptions(logger)
  settings = OpenStruct.new
  options = OptionParser.new do |opts|
    settings.log_level = 'info'
    settings.on_demand = false
    settings.preserve = false
    settings.run_job = false
    settings.output = "results.csv"
    opts.on('-b', '--benchmark_path BENCHMARK_JSON_PATH',
            'name of the benchmark to run') do |benchmark_path|
      settings.benchmark_path = benchmark_path
    end
    
    opts.on('-p', '--platform_name PLATFORM_JSON_PATH',
            'name of the platform to run the benchmark on') do |platform_path|
      settings.platform_path = platform_path
    end

    opts.on('-l', '--log-level LEVEL',
            "Log level: #{LOG_LEVELS.join(', ')}") do |log_level|
      settings.log_level = log_level
    end
    
    opts.on('-o', '--output FILE',
            "output file to write the result") do |output|
      settings.output = output
    end
    
     opts.on('-j', '--job_label LABEL_NAME',
            "A label for the job") do |label|
      settings.label = label
    end
  
    opts.on('-h', '--help', 'Show this help.') do
      logger.info options.to_s
      exit
    end
  end

  # Parse options and make sure we have our required ones.
  begin
    options.parse!
    mandatory = [:benchmark_path, :platform_path]
    missing = []
    mandatory.each do |setting|
      if !settings.marshal_dump.has_key?(setting)
        missing << setting.to_s.gsub('_', '-')
      end
    end
    if !missing.empty?
      logger.warn "Missing required options: #{missing.join(', ')}"
      logger.warn options
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

logger = Logger.new(STDOUT, shift_size = ONE_HUNDRED_MB)
log_map = Hash[LOG_LEVELS.map.with_index.to_a]
settings = parseOptions(logger)
logger.level = log_map[settings.log_level]

# benchmark = MRBenchmark.new(settings.benchmark_name, settings.platform_name, logger.level)
benchmark_factory = FactoryLoader.new().load_factory(settings.benchmark_path, settings.platform_path, settings.output, logger.level)
benchmark = benchmark_factory.create_benchmark
status = benchmark.run settings.label
logger.debug "status #{status.to_s}"
exit status[:exit_code]

