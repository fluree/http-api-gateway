# Fluree 3 HTTP API server

## Usage

Run `clojure -M -m fluree.http-api.system [profile]` where the optional
`profile` arg can be one of `prod` (default) or `dev`. See
`resources/config.edn` for the different parameters these profiles enable.

## API

Pull up the root URL of this server in a web browser for Swagger docs of the
API. I.e. if it says it's running on port 8090, browse to http://localhost:8090/
