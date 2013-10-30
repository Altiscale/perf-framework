#!/usr/bin/env ruby
# This class will copy the hadoop jar, clean up the output, and run an MR job

require 'logging'
require 'validators'
require 'writers'
class MR_Benchmark  
  include Logging
  #TODO: Exposed everything for testability. Need to find smarter way
  attr_accessor :writer, :validator, :scpUploader, :sshCommand
  def initialize (benchmark_config, platform_config)
    @benchmark_config = benchmark_config
    @platform_config = platform_config
    @benchmark = @benchmark_config["benchmark"]
    @platform = @platform_config["platform"]
  end
  
  # build up the set of commands accoring to the parameters provided

  #
  def run label=nil
    cleanupCommand =  @benchmark_config["platformspec"][@platform]["cleanupCommand"]
    localJar = @benchmark_config["platformspec"][@platform]["localJar"]
    hadoopJar = @benchmark_config["platformspec"][@platform]["hadoopJar"]
    mainClass = @benchmark_config["platformspec"][@platform]["mainClass"]
    runOptions = @benchmark_config["runOptions"]
    input = @benchmark_config["platformspec"][@platform]["input"] 
    machine = @platform_config["hostName"].split(".")[0]
    output = "#{@benchmark_config["platformspec"][@platform]["output"]}/#{machine}"
    #initalize the *Commands class
    #scp the hadoop jar
    @scpUploader.upload localJar, hadoopJar unless localJar.nil?
    logger.debug "Done copying jar"
    #hdfs cleanup
    begin
      @sshCommand.execute "hadoop fs #{cleanupCommand} #{output}"
      logger.debug "Cleaned hdfs"
    rescue => e
      logger.warn "Exception while cleaning: #{e}"  
    end
    #run hadoop command
    hadoop_command = "hadoop jar #{hadoopJar} #{mainClass} #{runOptions} #{input} #{output}"
   
    job_status = @sshCommand.execute hadoop_command
    result = populate_output output, label 
    result.merge! job_status
    # Additional output code
     
    @writer.write result unless @writer.nil?
    result
  end

  def populate_output output, label
    result = {}
    result[:label] = label
    result[:benchmark] = @benchmark_config["benchmark"]
    result[:platform] = @platform_config["platform"]
    result[:runOptions] =  @benchmark_config["runOptions"]
    result[:input] = @benchmark_config["platformspec"][@platform]["input"] 
    result[:output] = output
    result[:hadoopJar] = @benchmark_config["platformspec"][@platform]["hadoopJar"].split("/")[-1]
    result[:numNodes] = @platform_config["hadoopSlaves"]
    result[:jobflowId] = @platform_config["jobflowId"]
    result[:jobNum] = @validator.jobNum  
    result
  end
end
