#!/usr/bin/env ruby
# This contains all the commands to setup a job in mrBenchmark
require 'net/ssh'
require 'net/scp'

require 'utils'
class ScpUploader
  def initialize host, user, ssh_key
    @host = host
    @user = user
    @ssh_key = ssh_key
  end

  def upload localFile, remoteFile
    Net::SCP.start(
    @host,
    @user,
    :keys => [@ssh_key],
    :paranoid => FALSE,
    :user_known_hosts_file => "/dev/null",
    :global_known_hosts_file => "/dev/null" ) do |session|
      session.upload! localFile, remoteFile
    end
  end
end

class SshRun
  include Utils
  def initialize host, user, ssh_key, validator=nil
    @host = host
    @user = user
    @ssh_key = ssh_key
    @validator = validator
  end

  def execute command
    status = {}
    Net::SSH.start(
    @host,
    @user,
    :keys => [@ssh_key],
    :paranoid => FALSE,
    :user_known_hosts_file => "/dev/null",
    :global_known_hosts_file => "/dev/null" ) do |session|
      logger.debug "Logged into #{@host} to run #{command}"
      start_time = Time.now.to_i
      status = execsh command, session, command
      end_time = Time.now.to_i
      status[:start_time] = start_time
      status[:end_time] = end_time
      status[:duration] = end_time - start_time
    end
    status
  end

  def execsh comment, session, command
    result = {}
    session.open_channel do |channel|
      channel.exec(command) do |ch, success|
        logAndExit "could not execute command #{command}" unless success

        channel.on_data do |c, data|
          logger.debug data
        end

        channel.on_extended_data do |c, type, data|
          #If validator is not null then we must validate, otherwise, no point validating output
          logAndExit "could not execute command #{command}" unless @validator.nil? || @validator.validate(data)
          logger.debug data
        end

        channel.on_close do |c|
          logger.info "done executing: #{command}"
        end

        channel.on_request("exit-status") do |c,data|
          result[:exit_code] = data.read_long
        end

        channel.on_request("exit-signal") do |c, data|
          result[:exit_signal] = data.read_long
        end
      end
    end

    session.loop
    result
  end
end