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

class MRValidator
  include Logging
  attr_accessor :job_num, :bytes_written, :failure_reason
  def initialize (
    job_run_pattern=/Running job: (job_\w*$)/,
    failure_pattern=/Job\sjob_\d+_\d+\sfailed with state FAILED due to:\s*(.*$)/)
    
    @job_run_pattern = job_run_pattern
    @failure_pattern = failure_pattern
  end

  def validate output
    @job_num = @job_run_pattern.match(output)[1] unless @job_run_pattern.match(output).nil?
    unless @failure_pattern.match(output).nil?
      @failure_reason = @failure_pattern.match(output)[1]
      logger.warn "Failed validation: #{@failure_reason}" 
      return false
    end
    return true
  end
end