(ns fluree.http-api.integration.test-system
  (:require [clojure.edn :as edn]
            [clj-http.client :as http]
            [donut.system :as ds]
            [fluree.http-api.system :as sys])
  (:import (java.net ServerSocket)))

(def json-headers
  {"Content-Type" "application/json"
   "Accept"       "application/json"})

(def edn-headers
  {"Content-Type" "application/edn"
   "Accept"       "application/edn"})

(def sparql-headers
  {"Content-Type" "application/sparql-query"
   "Accept"       "application/json"})

(defn find-open-port
  ([] (find-open-port nil))
  ([_] ; so it can be used in swap!
   (let [socket (ServerSocket. 0)]
     (.close socket)
     (.getLocalPort socket))))

(defonce api-port (atom nil))

(defmethod ds/named-system :test
  [_]
  (ds/system :dev {[:env] {:http/server {:port @api-port}
                           :fluree/connection
                           {:method      :memory
                            :parallelism 1
                            :defaults
                            {:context
                             {:id     "@id"
                              :type   "@type"
                              :ex     "http://example.com/"
                              :schema "http://schema.org/"
                              :rdf    "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                              :f      "https://ns.flur.ee/ledger#"}}}}}))

(defn run-test-server
  [run-tests]
  (swap! api-port find-open-port)
  (let [stop-server (sys/run-server {:profile :test})]
    (run-tests)
    (stop-server)))

(defn api-url [endpoint]
  (str "http://localhost:" @api-port "/fluree/" (name endpoint)))

(defn api-post [endpoint req]
  (http/post (api-url endpoint) (assoc req :throw-exceptions false)))

(defn api-get [endpoint req]
  (http/get (api-url endpoint) (assoc req :throw-exceptions false)))

(defn create-rand-ledger
  [name-root]
  (let [ledger-name (str name-root "-" (random-uuid))
        req         (pr-str {:id      ledger-name
                             :context ["" {:foo "http://foobar.com/"}]
                             :graph   [{:id       :foo/create-test
                                        :type     :foo/test
                                        :foo/name "create-endpoint-test"}]})
        headers     {"Content-Type" "application/edn"
                     "Accept"       "application/edn"}
        res         (update (api-post :create {:body req :headers headers})
                            :body edn/read-string)]
    (if (= 201 (:status res))
      (get-in res [:body :alias])
      (throw (ex-info "Error creating random ledger" res)))))

(def auth
  {:id "did:fluree:TfHgFTQQiJMHaK1r1qxVPZ3Ridj9pCozqnh"
   :public "03b160698617e3b4cd621afd96c0591e33824cb9753ab2f1dace567884b4e242b0"
   :private "509553eece84d5a410f1012e8e19e84e938f226aa3ad144e2d12f36df0f51c1e"})
