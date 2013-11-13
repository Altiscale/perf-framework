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

require 'logger'
require 'optparse'
require 'ostruct'
require 'json'
require 'aws-sdk'
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

  opts.on('-c',
          '--emr_config EMR_CONFIGURATION_JSON',
          'path to emr_config.json') do |emr_config|
    settings.emr_config = emr_config
  end

  opts.on('-l',
          '--log-level LEVEL',
          "Log level: #{LOG_LEVELS.join(', ')}") do |log_level|
    settings.log_level = log_level
  end

  opts.on('-h',
          '--help',
          'Show this help.') do
    puts options.to_s
    exit
  end
end

# Parse options and make sure we have our required ones.
begin
  options.parse!
  mandatory = [:emr_config]
  missing = []
  mandatory.each do |setting|
    missing << setting.to_s.gsub('_', '-') unless settings.marshal_dump.key?(setting)
  end
  unless missing.empty?
    puts "Missing required options: #{missing.join(', ')}"
    puts options.to_s
    exit 1
  end
  unless LOG_LEVELS.include?(settings.log_level)
    puts "Invalid log_level (#{settings.log_level}). \
    Valid values: #{LOG_LEVELS.join(', ')}"
    exit 1
  end
rescue OptionParser::InvalidOption, OptionParser::MissingArgument
  puts $ERROR_INFO.to_s
  puts options.to_s
  exit 1
end

logger.level = log_map[settings.log_level]
config = JSON.parse(File.read(settings.emr_config), symbolize_names: true)
emr = AWS::EMR.new({ region: 'us-west-2' })
job_flow = emr.job_flows.create('abin', config)
until job_flow.state == 'WAITING' || job_flow.state == 'FAILED'
  logger.debug "job_flow #{job_flow.job_flow_id} #{job_flow.state}"
  sleep 15
end
fail "Could not start job_flow #{job_flow.job_flow_id}" unless job_flow.state == 'WAITING'
# Job has started, now tag the instances
ec2 = AWS::EC2.new({ region: 'us-west-2' })
client = ec2.client
instances = client.describe_tags({ 
    filters: [{ name:"resource-type", values: ["instance"]},
      { name: "key", values: ["aws:elasticmapreduce:job-flow-id"] },
      { name: "value", values: ["#{job_flow.job_flow_id}"] }
    ]
  })

user = `echo $(whoami)`
user = user.gsub("\n", '')
instances = instances[:tag_set]
logger.debug "instances #{instances.to_s}"
instance_list = []
instances.each {|instance| instance_list << instance[:resource_id]}
logger.debug "instances #{instance_list}"
client.create_tags({  resources: instance_list, 
                      tags: [ { key: "Customer", value: "Engineering"}, 
                              { key: "User", value: "#{user}@altiscale.com" }] })
# Now invoke perf
