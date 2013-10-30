#!/usr/bin/env ruby
require 'logger'

require 'logging'
module Utils
  extend Logging
  def self.included(base)
    base.send :include, Logging
  end

  def logAndExit message
    logger.fatal(message)
    raise "Fatal error: #{message}"
  end

  def exec comment, command
    logAndExit("#{comment} failed: #{command}") unless system(command)
    logger.info "#{comment} [OK]"
  end
end