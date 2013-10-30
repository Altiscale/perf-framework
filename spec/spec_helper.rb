#
# spec_helper - setup rspec
#

require 'rspec'
require 'perf_benchmark'

RSpec.configure do |config|
  config.color_enabled = true
  config.formatter     = 'documentation'
end
