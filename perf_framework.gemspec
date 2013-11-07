# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'perf_framework/version'

Gem::Specification.new do |spec|
  spec.name          = "perf_framework"
  spec.version       = PerfFramework::VERSION
  spec.authors       = ["Abin Shahab"]
  spec.email         = ["ashahab@altiscale.com"]
  spec.description   = %q{Gem to run a performance benchmark}
  spec.summary       = IO.read(File.join(File.dirname(__FILE__), 'README.md'))
  spec.homepage      = "https://github.com/AltiPub/perf-framework"
  spec.license       = "Proprietary"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.3"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "rake"
  spec.add_runtime_dependency "net-ssh"
  spec.add_runtime_dependency "net-scp"
end
