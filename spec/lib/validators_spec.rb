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

require 'spec_helper'

describe MRValidator, "#validate" do
  it "returns true for empty input" do
    validator = MRValidator.new
    validator.validate("").should eq(true)
  end  
  
  it "returns true for unmatched input" do
    validator = MRValidator.new
    validator.validate("I am not matched").should eq(true)
  end
  
  it "finds a job_num" do
    validator = MRValidator.new
    validator.validate("D, [2013-10-28T11:46:53.412989 #81300] DEBUG -- : 13/10/28 18:46:52 INFO mapreduce.Job: Running job: job_1381874118387_0206").should eq(true)
    validator.job_num.should eq("job_1381874118387_0206")
  end
  
  it "finds an application_num" do
    validator = MRValidator.new
    validator.validate("D, [2013-11-05T17:49:34.137428 #10962] DEBUG -- : 13/11/05 17:49:34 INFO impl.YarnClientImpl: Submitted application application_1383672321583_0002 to ResourceManager at /10.217.3.104:9022").should eq(true)
    validator.application_num.should eq("application_1383672321583_0002")
  end
  
  it "returns false for failures" do
    validator = MRValidator.new
    validator.validate("D, [2013-10-28T12:09:30.601538 #81300] DEBUG -- : 13/10/28 19:09:29 INFO mapreduce.Job: Job job_1381874118387_0206 failed with state FAILED due to: Task failed task_1381874118387_0206_r_000000").should eq(false)
    validator.failure_reason.should eq("Task failed task_1381874118387_0206_r_000000")
  end
end

describe JSONParser, '#validate' do
  it "parses a json string" do
    result = {"status" => 'FINISHED'}
    result_from_parser = JSONParser.new.validate result.to_json
    expect(result_from_parser).to eq(result)
  end
end
