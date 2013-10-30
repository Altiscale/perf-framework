#!/usr/bin/env ruby
require 'logging'

class MR_Validator
  include Logging
  attr_accessor :jobNum, :bytesWritten, :failureReason
  def initialize (
    jobRunPattern=/Running job: (job_\w*$)/,
    failurePattern=/Job\sjob_\d+_\d+\sfailed with state FAILED due to:\s*(.*$)/)
    @jobRunPattern = jobRunPattern
    @failurePattern = failurePattern
  end

  def validate output
    @jobNum = @jobRunPattern.match(output)[1] unless @jobRunPattern.match(output).nil?
    unless @failurePattern.match(output).nil?
      @failureReason = @failurePattern.match(output)[1]
      logger.warn "Failed validation: #{@failureReason}" 
      return false
    end
    return true
  end
end