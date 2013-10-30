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

require 'csv'
class CSVWriter
  def initialize(file)
    @file = file
  end
  
  #Parse the file
  #If it's empty or does not exist, print a header column
  #Then print the map out
  def write output
    file_exists = File.exists? @file
    CSV.open(@file, "ab") do |csv|
      csv << output.keys unless file_exists
      csv << output.values
    end
  end
end