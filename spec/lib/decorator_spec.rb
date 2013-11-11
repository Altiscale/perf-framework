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

describe RemoteDistCP, '#run' do
  from_dir = 'from/my/source'
  to_dir = 'to/my/dest'
  it 'invokes distcp from_dir to to_dir' do
    ssh = double(SSHRun)
    ssh.should_receive(:execute).with(anything)
    distcp = "hadoop distcp #{from_dir} #{to_dir}"
    ssh.should_receive(:execute).with(distcp)
    RemoteDistCP.new(ssh, from_dir, to_dir, true).run
  end

  it 'does not invoke distcp if force is false and destination found' do
    ssh = double(SSHRun)
    command = "hadoop fs -test -d #{to_dir}"
    ssh.stub(:execute).with(command) do
      { exit_code: 0 }
    end
    expect(RemoteDistCP.new(ssh, from_dir, to_dir).run).to eql(nil)
  end
end
