GEM_SOURCE?=https://gems.service.verticloud.com

release:
	rake clean all install
	-gem inabox -g $(GEM_SOURCE) -V pkg/*.gem