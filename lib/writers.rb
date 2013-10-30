#!/usr/bin/env ruby
require 'csv'
class CSV_Writer
  def initialize(file)
    @file = file
  end
  
  #Parse the file
  #If it's empty or does not exist, print a header column
  #Then print the map out
  def write output
    file_is_new = File.exists? @file
    CSV.open(@file, "ab") do |csv|
      csv << output.keys unless file_is_new
      csv << output.values
    end
  end
end