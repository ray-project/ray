VERSION = 1.2.1

RELEASE_PKG = ./cmd/depth
INSTALL_PKG = $(RELEASE_PKG)


# Remote includes require 'mmake' 
# github.com/tj/mmake
include github.com/KyleBanks/make/go/install
include github.com/KyleBanks/make/go/sanity
include github.com/KyleBanks/make/go/release
include github.com/KyleBanks/make/go/bench
include github.com/KyleBanks/make/git/precommit

# Runs a number of depth commands as examples of what's possible.
example: | install
	depth github.com/KyleBanks/depth/cmd/depth strings ./

	depth -internal strings 

	depth -json github.com/KyleBanks/depth/cmd/depth

	depth -test github.com/KyleBanks/depth/cmd/depth

	depth -test -internal strings

	depth -test -internal -max 3 strings

	depth .

	depth ./cmd/depth
.PHONY: example
