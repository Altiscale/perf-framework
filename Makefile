GEM_SOURCE?=https://gems.service.verticloud.com

all: test gem

.PHONY: all test clean

test:
	rake test

gem:
	gem build *.gemspec
	
release:
	-gem inabox -g $(GEM_SOURCE) -V *.gem

clean:
	rm -f *.gem