# Fluree 3 HTTP API server

## Usage

Run `clojure -M -m fluree.http-api.system [profile]` where the optional
`profile` arg can be one of `prod` (default) or `dev`. See
`resources/config.edn` for the different parameters these profiles enable.

### Dev

Shortcut for running in dev mode: `clojure -X:run-dev`

### Docker

You can build a Docker image of this component by running:
`docker buildx build -t fluree/http-api-gateway:[version] .`

And then run it with:
`docker run -p 8090:8090 -v /local/dir:/opt/fluree-http-api-gateway/data fluree/http-api-gateway:[version] [profile]`

`profile` defaults to `docker` (see `resources/config.edn` for what that
enables) but can also be set to `dev`.

### Config

`resources/config.edn` defines several env vars you can set to override the
config defaults. They are in all caps and have a `#env` in front of them.

## API

Pull up the root URL of this server in a web browser for Swagger docs of the
API. I.e. if it says it's running on port 8090, browse to http://localhost:8090/

## Tests

`clojure -X:test` runs the test suite.
