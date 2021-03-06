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

require 'logger'

# Include this module to use logger
module Logging
  # This is the magical bit that gets mixed into your classes
  def logger
    Logging.logger
  end

  # Global, memoized, lazy initialized instance of a logger
  def self.logger
    @logger ||= CustomLogger.new(STDOUT)
  end
end

# Wrapper for setting the log_level
class CustomLogger < Logger
  LOG_LEVELS = %w(
    debug
    info
    warn
    error
    fatal
  )
  def level=(log_level)
    puts "Setting log level to #{log_level}"
    fail "Invalid log_level (#{log_level}).  "\
    "Valid values: #{LOG_LEVELS.join(', ')}" unless LOG_LEVELS.include?(log_level)
    log_map = Hash[LOG_LEVELS.map.with_index.to_a]
    super(log_map[log_level])
  end
end
