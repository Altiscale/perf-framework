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

# Decorators that wrap the benchmark to do additional work before and after
module Decorator
  attr_reader :description
end
# SSH and Distcp
class RemoteDistCP
  include Logging
  include Decorator
  HADOOP_FINISHED_STATE = 'FINISHED'
  JOB_STATUS_SLEEP_INTERNVAL = 15
  def initialize(ssh_command, from_dir, to_dir, force = false)
    @ssh_command = ssh_command
    @from_dir = from_dir
    @to_dir = to_dir
    @force = force
    @description = "Copy  #{@from_dir} => #{@to_dir} (force: #{@force})"
  end

  def run(prior_result = {})
    # somehow get the application_id here
    sleep JOB_STATUS_SLEEP_INTERNVAL  until job_finished?(prior_result[:application_num])
    ok_to_copy = @force
    @ssh_command.execute "hadoop fs -ls #{@to_dir}" do |data|
      ok_to_copy ||= dest_not_found(data)
    end
    command = "hadoop distcp #{@from_dir} #{@to_dir}"
    status = { exit_code: 0 }
    logger.info "Aborting copy to #{@to_dir}" unless ok_to_copy
    status = @ssh_command.execute command if ok_to_copy
    status
  end

  def job_finished?(application_num)
    return true if application_num.nil?
    # make an ssh rest call
    rest_call = "curl --get \'http://localhost:9026/ws/v1/cluster/apps/#{application_num}\'"
    state = nil
    @ssh_command.execute rest_call do |data|
      begin
        json = JSON.parse data
        state = json['app']['state']
        logger.debug "json #{json.to_s}"
      rescue JSON::ParserError => e
        logger.debug "parse error #{e}"
      end
    end
    logger.debug "state = #{state}"
    state == HADOOP_FINISHED_STATE
  end

  def dest_not_found(data)
    !/ls: \`#{Regexp.escape(@to_dir)}': No such file or directory/.match(data).nil?
  end
end

# Does an Scp
class RemoteSCP
  include Decorator
  include Logging
  def initialize(scp, from_dir, to_dir)
    @scp = scp
    @from_dir = from_dir
    @to_dir = to_dir
    @description = "scp  #{@from_dir} => #{@to_dir})"
  end

  def run(prior_result = {})
    @scp.upload @from_dir, @to_dir
  end
end

# A Command wrapper that executes a list of commands
class CommandChain
  include Logging
  def initialize(*commands)
    @commands = *commands
  end

  def add(*commands)
    commands = commands.last if commands.last.is_a?(Array)
    commands.each  do |cmd|
      @commands << cmd unless cmd.nil?
    end
    self
  end

  def run(result)
    @commands.each do |cmd|
      logger.info "executing #{cmd.description}"
      show_wait_spinner { result = cmd.run result }
    end
    result
  end

  def commands
    @commands.clone
  end

  def show_wait_spinner(fps = 10)
    chars = %w[| / - \\]
    delay = 1.0 / fps
    iter = 0
    spinner = Thread.new do
      while iter # Keep spinning until told otherwise
        print chars[(iter += 1) % chars.length]
        sleep delay
        print "\b"
      end
    end
    yield.tap do     # After yielding to the block, save the return value
      iter = false   # Tell the thread to exit, cleaning up after itself
      spinner.join   # and wait for it to do so.
    end              # Use the block's return value as the method's
  end
end
