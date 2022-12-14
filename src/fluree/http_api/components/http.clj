(ns fluree.http-api.components.http
  (:require
    [donut.system :as ds]
    [jsonista.core :as j]
    [ring.adapter.jetty9 :as http]
    [reitit.ring :as ring]
    [reitit.coercion.spec]
    [reitit.swagger :as swagger]
    [reitit.swagger-ui :as swagger-ui]
    [reitit.ring.coercion :as coercion]
    [reitit.ring.middleware.muuntaja :as muuntaja]
    [reitit.ring.middleware.exception :as exception]
    [fluree.http-api.handlers.ledger :as ledger]
    [clojure.spec.alpha :as s]
    [muuntaja.core :as m]
    [muuntaja.format.json :as json-format]
    [muuntaja.format.core :as mf]
    [ring.middleware.cors :refer [wrap-cors]]
    [fluree.db.util.log :as log])
  (:import (java.io InputStream InputStreamReader)))

;; TODO: Flesh this out some more
(s/def ::non-empty-string (s/and string? #(< 0 (count %))))
(s/def ::address ::non-empty-string)
(s/def ::id ::non-empty-string)
(s/def ::t neg-int?)
(s/def ::alias ::non-empty-string)
(s/def ::action (s/or :keywords #{:new :insert}
                      :strings #{"new" "insert"}))
(s/def ::ledger ::non-empty-string)
(s/def ::txn (s/or :single-map map? :collection-of-maps (s/coll-of map?)))

(def server
  #::ds{:start  (fn [{{:keys [handler options]} ::ds/config}]
                  (let [server (http/run-jetty handler options)]
                    (println "Fluree HTTP API server running on port"
                             (:port options))
                    server))
        :stop   (fn [{::ds/keys [instance]}]
                  (http/stop-server instance))
        :config {:handler (ds/local-ref [:handler])
                 :options
                 {:port  (ds/ref [:env :http/server :port])
                  :join? false}}})

(def query-endpoint
  {:summary    "Endpoint for submitting queries"
   :parameters {:body {:ledger string?
                       :query  map?}}
   :responses  {200 {:body sequential?}
                400 {:body string?}
                500 {:body string?}}
   :handler    ledger/query})

(defn wrap-assoc-conn
  [conn handler]
  (fn [req]
    (-> req
        (assoc :fluree/conn conn)
        handler)))

(defn fluree-json-ld-decoder
  [options]
  (let [mapper (json-format/object-mapper! (assoc options
                                             :decode-key-fn false))]
    (reify
      mf/Decode
      (decode [_ data charset]
        (let [decoded (if (.equals "utf-8" ^String charset)
                        (j/read-value data mapper)
                        (j/read-value (InputStreamReader. ^InputStream data
                                                          ^String charset)
                                      mapper))]
          ;; keywordize only the top-level keys
          (reduce-kv (fn [m k v]
                       (assoc m (keyword k) v))
                     {} decoded))))))

(def fluree-json-ld-format
  (mf/map->Format
    {:name "application/json"
     :encoder [json-format/encoder]
     :decoder [fluree-json-ld-decoder]}))

(defn websocket-handler
  [upgrade-request]
  ;; Mostly copy-pasta from
  ;; https://github.com/sunng87/ring-jetty9-adapter/blob/master/examples/rj9a/websocket.clj
  (let [provided-subprotocols (:websocket-subprotocols upgrade-request)
        provided-extensions (:websocket-extensions upgrade-request)]
    {;; provide websocket callbacks
     :on-connect (fn on-connect [_]
                   (tap> [:ws :connect]))
     :on-text (fn on-text [ws text-message]
                (tap> [:ws :msg text-message])
                (http/send! ws (str "echo: " text-message)))
     :on-bytes (fn on-bytes [_ _ _ _]
                 (tap> [:ws :bytes]))
     :on-close (fn on-close [_ status-code reason]
                 (tap> [:ws :close status-code reason]))
     :on-ping (fn on-ping [ws payload]
                (tap> [:ws :ping])
                (http/send! ws payload))
     :on-pong (fn on-pong [_ _]
                (tap> [:ws :pong]))
     :on-error (fn on-error [_ e]
                 (tap> [:ws :error e]))
     :subprotocol (first provided-subprotocols)
     :extensions provided-extensions}))

(defn app
  [conn]
  (ring/ring-handler
    (ring/router
      [["/swagger.json"
        {:get {:no-doc  true
               :swagger {:info {:title "Fluree HTTP API"}}
               :handler (swagger/create-swagger-handler)}}]
       ["/fdb" {:middleware [#(wrap-cors %
                                         :access-control-allow-origin [#".*"]
                                         :access-control-allow-methods [:get :post])
                             (partial wrap-assoc-conn conn)]}
        ["/transact"
         {:post {:summary    "Endpoint for submitting transactions"
                 :parameters {:body (s/keys :opt-un [::action]
                                            :req-un [::ledger ::txn])}
                 :responses  {200 {:body (s/keys :opt-un [::address ::id]
                                                 :req-un [::alias ::t])}
                              400 {:body string?}
                              500 {:body string?}}
                 :handler    ledger/transact}}]
        ["/query"
         {:get  query-endpoint
          :post query-endpoint}]]]
      {:data {:coercion   reitit.coercion.spec/coercion
              :muuntaja   (m/create
                            (assoc-in
                              m/default-options
                              [:formats "application/json"]
                              fluree-json-ld-format))
              :middleware [swagger/swagger-feature
                           muuntaja/format-negotiate-middleware
                           muuntaja/format-response-middleware
                           (exception/create-exception-middleware
                             {::exception/default
                              (partial exception/wrap-log-to-console
                                       exception/default-handler)})
                           muuntaja/format-request-middleware
                           coercion/coerce-response-middleware
                           coercion/coerce-request-middleware]}})

    (ring/routes
      (ring/ring-handler
        (ring/router
          ["/ws" {:get (fn [req]
                         (if (http/ws-upgrade-request? req)
                           (http/ws-upgrade-response websocket-handler)
                           {:status 400
                            :body "Invalid websocket upgrade request"}))}]))
      (swagger-ui/create-swagger-ui-handler
        {:path   "/"
         :config {:validatorUrl     nil
                  :operationsSorter "alpha"}})
      (ring/create-default-handler))))
