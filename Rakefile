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
# Rakefile - for perf-framework

# coding: utf-8
require 'bundler/gem_tasks'
require 'rake/clean'
require 'rspec/core/rake_task'
require 'rubocop/rake_task'
require_relative 'lib/perf_framework/version'
# Clobber should also clean up built packages
CLOBBER.include('pkg')

# Disable the push to rubygems.org
Rake::Task[:release].clear

# Use bundler to install deps since rake won't
task :install_deps do
  Bundler.with_clean_env do
    sh 'bundle install --system'
  end
end

task :publish do
  
  sh "gem inabox pkg/perf_framework-#{PerfFramework::VERSION}.gem"
end

RSpec::Core::RakeTask.new(:test) do |t|
  Bundler.with_clean_env do
    sh 'bundle install --system'
  end
end

desc 'Run RuboCop on the lib directory'
RuboCop::RakeTask.new(:lint) do |t|
  t.patterns = ['lib/**/*.rb']
  t.fail_on_error = false
end

Rake::Task[:install].enhance [:install_deps]

task test: [:install_deps, :spec]

task default: [:test]

desc 'Run tests and build if successful'
task all: [:test, :build]