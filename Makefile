FLAGS ?= --optimize

all: cache

cache:
	elm make elm/Cache.elm $(FLAGS) --output tshistory_refinery/refinery_static/cache.js

clean: cleanstuff cleanbuild

cleanstuff:
	rm elm-stuff -rf

cleanbuild:
	rm tshistory_refinery/refinery_static/cache.js -f
