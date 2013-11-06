GEM_SOURCE?=https://gems.service.verticloud.com

all: test gem

.PHONY: all test clean

test:
	rake test

gem:
	gem build *.gemspec
	-gem inabox -g $(GEM_SOURCE) -V *.gem

clean:
	rm -f *.gem