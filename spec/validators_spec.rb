require 'spec_helper'

describe MR_Validator, "#validate" do
  it "returns true for empty input" do
    validator = MR_Validator.new
    validator.validate("").should eq(true)
  end  
  
  it "returns true for unmatched input" do
    validator = MR_Validator.new
    validator.validate("I am not matched").should eq(true)
  end
  
  it "it finds a jobNum" do
    validator = MR_Validator.new
    validator.validate("D, [2013-10-28T11:46:53.412989 #81300] DEBUG -- : 13/10/28 18:46:52 INFO mapreduce.Job: Running job: job_1381874118387_0206").should eq(true)
    validator.jobNum.should eq("job_1381874118387_0206")
  end
  
  it "it returns false for failures" do
    validator = MR_Validator.new
    validator.validate("D, [2013-10-28T12:09:30.601538 #81300] DEBUG -- : 13/10/28 19:09:29 INFO mapreduce.Job: Job job_1381874118387_0206 failed with state FAILED due to: Task failed task_1381874118387_0206_r_000000").should eq(false)
    validator.failureReason.should eq("Task failed task_1381874118387_0206_r_000000")
    
  end
end
