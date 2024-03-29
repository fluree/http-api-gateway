SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

target/http-api-gateway-%.jar: $(SOURCES) $(RESOURCES)
	clojure -T:build uber

uberjar: target/http-api-gateway-%.jar

.PHONY: test
test:
	clojure -X:test

.PHONY: eastwood
eastwood:
	clojure -M:dev:test:eastwood

.PHONY: ci
ci: test eastwood

.PHONY: test-verbose
test-verbose:
	clojure -X:test-verbose

.PHONY: clean
clean:
	rm -rf target
	rm -rf .cpcache
