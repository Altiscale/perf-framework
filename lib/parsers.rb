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

require 'logging'
require 'json'
module Parser
  include Logging
  def validate data
    logger.debug "validating #{data}"
    return true
  end
  
  def parse data
    logger.debug "parsing #{data}"
  end
end

class MRValidator
  include Parser
  attr_accessor :job_num, :application_num, :failure_reason
  def initialize (
    job_run_pattern=/Running job: (job_\w*$)/,
    application_pattern=/Submitted application\s*(application_[\d_]+) to ResourceManager/,
    failure_pattern=/Job\sjob_\d+_\d+\sfailed with state FAILED due to:\s*(.*$)/)
    
    @job_run_pattern = job_run_pattern
    @application_pattern = application_pattern
    @failure_pattern = failure_pattern
  end

  def validate output
    @job_num = @job_run_pattern.match(output)[1] unless @job_run_pattern.match(output).nil?
    @application_num = @application_pattern.match(output)[1] unless @application_pattern.match(output).nil?
    unless @failure_pattern.match(output).nil?
      @failure_reason = @failure_pattern.match(output)[1]
      logger.warn "Failed validation: #{@failure_reason}" 
      return false
    end
    return true
  end
end

# Adapter for the JSONParser
class JSONParser 
  include Parser
  attr_reader :json 
  
  def parse data
    logger.debug "parsing: #{data}"
    begin
      @json = JSON.parse data
      @json = @json['app']
      logger.debug "json #{@json.to_s}"
    rescue JSON::ParserError
    end  
  end  
end