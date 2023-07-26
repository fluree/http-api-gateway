SOURCES := $(shell find src)
RESOURCES := $(shell find resources)

target/http-api-gateway-%.jar: $(SOURCES) $(RESOURCES)
	clojure -T:build uber

uberjar: target/http-api-gateway-%.jar

docker-build:
	docker buildx build --platform linux/amd64,linux/arm64 -t fluree/http-api-gateway:latest -t fluree/http-api-gateway:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" .

docker-run:
	docker run -p 58090:8090 -v `pwd`/data:/opt/fluree-http-api-gateway/data fluree/http-api-gateway

docker-push:
	docker buildx build --platform linux/amd64,linux/arm64 -t fluree/http-api-gateway:latest -t fluree/http-api-gateway:$(shell git rev-parse HEAD) --build-arg="PROFILE=prod" --push .

.PHONY: test
test:
	clojure -X:test

.PHONY: clean
clean:
	rm -rf target
	rm -rf .cpcache
