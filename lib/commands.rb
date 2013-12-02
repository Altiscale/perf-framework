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

# This contains all the commands to setup a job in mrBenchmark
require 'net/ssh'
require 'net/scp'
require 'logging'

# Uplaods to SCP
class SCPUploader
  include Logging
  def initialize(host, user, ssh_key)
    @host = host
    @user = user
    raise "Missing ssh_key: #{ssh_key}" unless ssh_key.nil? || File.exist?(ssh_key)
    @ssh_key = ssh_key
  end

  def upload(local_file, remote_file)
    Net::SCP.start(
      @host,
      @user,
      keys: [@ssh_key],
      paranoid: FALSE,
      user_known_hosts_file: '/dev/null',
      global_known_hosts_file: '/dev/null') do |session|
      logger.info "Uploading #{local_file} to #{remote_file}"
      session.upload! local_file, remote_file
    end
  end
end

# Runs ssh commands
class SSHRun
  include Logging
  attr_reader :parser
  def initialize(host, user, ssh_key, parser = nil)
    @host = host
    @user = user
    raise "Missing ssh_key: #{ssh_key}" unless !ssh_key.nil? && File.exist?(ssh_key)
    @ssh_key = ssh_key
    @parser = parser
  end

  def execute(command, &validator)
    status = {}
    Net::SSH.start(
      @host,
      @user,
      keys: [@ssh_key],
      paranoid: FALSE,
      user_known_hosts_file: '/dev/null',
      global_known_hosts_file: '/dev/null') do |session|
      logger.info "Logged into #{@host} to run #{command}"
      start_time = Time.now.to_i
      status = execsh command, session, command, &validator
      end_time = Time.now.to_i
      status[:start_time] = start_time
      status[:end_time] = end_time
      status[:duration] = end_time - start_time
    end
    status
  end

  def log_and_exit(message)
    logger.fatal(message)
    fail "Fatal error: #{message}"
  end

  def execsh(comment, session, command, &validator)
    result = {}
    session.open_channel do |channel|
      channel.exec(command) do |ch, success|
        log_and_exit "could not execute command #{command}" unless success

        channel.on_data do |c, data|
          logger.debug "#{data}"
          validator.call(data) unless validator.nil?
        end

        channel.on_extended_data do |c, type, data|
        # If parser is not null then we must validate, otherwise, no point validating output
          log_and_exit "could not execute command #{command}" unless @parser.nil? || @parser.validate(data)
          logger.debug "#{data}"
          validator.call(data) unless validator.nil?
        end

        channel.on_close do |c|
          logger.info "done executing: #{command}"
        end

        channel.on_request('exit-status') do |c, data|
          result[:exit_code] = data.read_long
        end

        channel.on_request('exit-signal') do |c, data|
          result[:exit_signal] = data.read_long
        end
      end
    end

    session.loop
    result
  end
end
