FROM --platform=$BUILDPLATFORM clojure:temurin-17-tools-deps-1.11.1.1208-bullseye-slim AS builder

RUN mkdir -p /usr/src/fluree-http-api-gateway
WORKDIR /usr/src/fluree-http-api-gateway

COPY deps.edn ./

RUN clojure -P && clojure -A:build -P

COPY . ./

RUN clojure -T:build uber
RUN ls -la target

FROM eclipse-temurin:17-jre-jammy AS runner

RUN mkdir -p /opt/fluree-http-api-gateway
WORKDIR /opt/fluree-http-api-gateway

COPY --from=builder /usr/src/fluree-http-api-gateway/target/http-api-gateway-*.jar ./fluree-http-api-gateway.jar

EXPOSE 8090
EXPOSE 58090

VOLUME ./data

ENTRYPOINT ["java", "-jar", "fluree-http-api-gateway.jar"]
CMD ["docker"]
