#!/usr/bin/env ruby

# Copyright (c) 2013 Altiscale, inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License

# This script runs the benchmark. It requires the platform
# and benchmark json files

require 'perf_benchmark'
require 'logger'
require 'optparse'
require 'ostruct'

LOG_LEVELS = %w(
  debug
  info
  warn
  error
  fatal
)
logger = Logger.new(STDOUT)
log_map = Hash[LOG_LEVELS.map.with_index.to_a]
settings = OpenStruct.new
options = OptionParser.new do |opts|
  settings.log_level = 'info'
  settings.uniquify = false
  settings.output = 'results.csv'
  opts.on('-b',
          '--benchmark_path BENCHMARK_JSON_PATH',
          'path to benchmark.json') do |benchmark_path|
    settings.benchmark_path = benchmark_path
  end

  opts.on('-p',
          '--platform_name PLATFORM_JSON_PATH',
          'path to platform.json') do |platform_path|
    settings.platform_path = platform_path
  end

  opts.on('-l',
          '--log-level LEVEL',
          "Log level: #{LOG_LEVELS.join(', ')}") do |log_level|
    settings.log_level = log_level
  end

  opts.on('-o',
          '--output FILE',
          'output file to write the result') do |output|
    settings.output = output
  end
  
  opts.on('-u',
          '--uniquify',
          'uniquify output') do |uniquify|
    settings.uniquify = true
  end

  opts.on('-f',
          '--from-dir DIRECTORY_PATH',
          'HDFS or S3 directory') do |from_dir|
    settings.from_dir = from_dir
  end

  opts.on('-t',
          '--to-dir DIRECTORY_PATH',
          'HDFS or S3 directory') do |to_dir|
    settings.to_dir = to_dir
  end

  opts.on('-j',
          '--job_label LABEL_NAME',
          'A label for the job') do |label|
    settings.label = label
  end

  opts.on('-h',
          '--help',
          'Show this help.') do
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
    unless settings.marshal_dump.key?(setting)
      missing << setting.to_s.gsub('_', '-')
    end
  end
  unless missing.empty?
    logger.warn "Missing required options: #{missing.join(', ')}"
    logger.warn options
    exit!
  end
  unless LOG_LEVELS.include?(settings.log_level)
    logger.warn "Invalid log_level (#{settings.log_level}). \
    Valid values: #{LOG_LEVELS.join(', ')}"
    exit!
  end
rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  logger.error $ERROR_INFO.to_s
  logger.error options.to_s
  exit!
end

logger.level = log_map[settings.log_level]
benchmark_maker = BenchmarkMaker.new
                                .with_copier(settings.from_dir, settings.to_dir)
                                .uniquify?(settings.uniquify)            
benchmark = benchmark_maker.load_factory(settings.benchmark_path,
                                         settings.platform_path,
                                         settings.output,
                                         settings.log_level)
status = benchmark.run settings.label
logger.debug "status #{status}"
exit status
