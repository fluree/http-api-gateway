(ns fluree.http-api.components.http
  (:require
   [donut.system :as ds]
   [ring.adapter.jetty9 :as http]
   [reitit.ring :as ring]
   [reitit.coercion.malli]
   [reitit.swagger :as swagger]
   [reitit.swagger-ui :as swagger-ui]
   [reitit.ring.coercion :as coercion]
   [reitit.ring.middleware.muuntaja :as muuntaja-mw]
   [reitit.ring.middleware.exception :as exception]
   [fluree.http-api.handlers.ledger :as ledger]
   [muuntaja.core :as muuntaja]
   [muuntaja.format.core :as mfc]
   [muuntaja.format.json :as mfj]
   [ring.middleware.cors :as rmc]
   [fluree.db.json-ld.transact :as ftx]
   [fluree.db.query.fql.syntax :as fql]
   [fluree.db.query.history :as fqh]
   [fluree.db.util.log :as log]
   [fluree.db.util.validation :as v]
   [malli.core :as m]
   [malli.experimental.lite :as l]))

(set! *warn-on-reflection* true)

(def LedgerAlias
  (m/schema [:string {:min 1}]))

(def LedgerAddress
  (m/schema [:string {:min 1}]))

(def Transaction
  (m/schema ::ftx/txn {:registry ftx/registry}))

(def Context
  (m/schema ::v/context {:registry v/registry}))

(def Defaults
  (m/schema [:map ["@context" Context]]))

(def TValue
  (m/schema pos-int?))

(def DID
  (m/schema [:string {:min 1}]))

(def Query
  (m/schema ::fql/analytical-query {:registry fql/registry}))

(def QueryResponse
  (m/schema ::fql/analytical-query-results {:registry fql/registry}))

(def MultiQuery
  (m/schema ::fql/multi-query {:registry fql/registry}))

(def MultiQueryResponse
  (m/schema ::fql/multi-query-results {:registry fql/registry}))

(def HistoryQuery
  (m/schema ::fqh/history-query {:registry fqh/registry}))

(def HistoryQueryResponse
  (m/schema ::fqh/history-query-results {:registry fqh/registry}))

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
   :parameters {:body {"ledger" LedgerAlias
                       "query"  Query}}
   :responses  {200 {:body QueryResponse}
                400 {:body [:or :string :map]}
                500 {:body [:or :string :map]}}
   :handler    ledger/query})

(def multi-query-endpoint
  {:summary    "Endpoint for submitting multi-queries"
   :parameters {:body {"ledger" LedgerAlias
                       "query"  MultiQuery}}
   :responses  {200 {:body MultiQueryResponse}
                400 {:body [:or :string :map]}
                500 {:body [:or :string :map]}}
   :handler    ledger/multi-query})

(def history-endpoint
  {:summary    "Endpoint for submitting history queries"
   :parameters {:body {"ledger" LedgerAlias
                       "query"  HistoryQuery}}
   :responses  {200 {:body HistoryQueryResponse}
                400 {:body [:or :string :map]}
                500 {:body [:or :string :map]}}
   :handler    ledger/history})

(defn wrap-assoc-conn
  [conn handler]
  (fn [req]
    (-> req
        (assoc :fluree/conn conn)
        handler)))

(defn wrap-cors
  [handler]
  (rmc/wrap-cors handler
                 :access-control-allow-origin [#".*"]
                 :access-control-allow-methods [:get :post]))

(defn wrap-set-fuel-header
  [handler]
  (fn [req]
    (let [resp (handler req)
          fuel 1000] ; TODO: get this for real
      (assoc-in resp [:headers "x-fdb-fuel"] (str fuel)))))

(defn sort-middleware-by-weight
  [weighted-middleware]
  (map (fn [[_ mw]] mw) (sort-by first weighted-middleware)))

(defn websocket-handler
  [upgrade-request]
  ;; Mostly copy-pasta from
  ;; https://github.com/sunng87/ring-jetty9-adapter/blob/master/examples/rj9a/websocket.clj
  (let [provided-subprotocols (:websocket-subprotocols upgrade-request)
        provided-extensions   (:websocket-extensions upgrade-request)]
    {;; provide websocket callbacks
     :on-connect  (fn on-connect [_]
                    (tap> [:ws :connect]))
     :on-text     (fn on-text [ws text-message]
                    (tap> [:ws :msg text-message])
                    (http/send! ws (str "echo: " text-message)))
     :on-bytes    (fn on-bytes [_ _ _ _]
                    (tap> [:ws :bytes]))
     :on-close    (fn on-close [_ status-code reason]
                    (tap> [:ws :close status-code reason]))
     :on-ping     (fn on-ping [ws payload]
                    (tap> [:ws :ping])
                    (http/send! ws payload))
     :on-pong     (fn on-pong [_ _]
                    (tap> [:ws :pong]))
     :on-error    (fn on-error [_ e]
                    (tap> [:ws :error e]))
     :subprotocol (first provided-subprotocols)
     :extensions  provided-extensions}))

(def json-format
  (mfc/map->Format
    {:name "application/json"
     :decoder [mfj/decoder {:decode-key-fn false}]
     :encoder [mfj/encoder]}))

(defn muuntaja-instance
  []
  (muuntaja/create
    (assoc-in muuntaja/default-options [:formats "application/json"]
               json-format)))

(defn debug-middleware
  "Put this in anywhere in your middleware chain to get some insight into what's
  happening there. Logs the request and response at DEBUG level, prefixed with
  the name argument."
  [name]
  (fn [handler]
    (fn [req]
      (log/debug name "got request:" req)
      (let [resp (handler req)]
        (log/debug name "got response:" resp)
        resp))))

(defn app
  [{:keys [:fluree/conn :http/middleware :http/routes]}]
  (log/debug "HTTP server running with Fluree connection:" conn
             "- middleware:" middleware "- routes:" routes)
  (let [default-fluree-middleware [[10 wrap-cors]
                                   [10 (partial wrap-assoc-conn conn)]
                                   [100 wrap-set-fuel-header]
                                   [200 coercion/coerce-exceptions-middleware]
                                   [300 coercion/coerce-response-middleware]
                                   [400 coercion/coerce-request-middleware]
                                   ;; Exception middleware should always be last.
                                   ;; Otherwise middleware that comes after it
                                   ;; will be skipped on response if handler code
                                   ;; throws an exception b/c this is what catches
                                   ;; them and turns them into responses.
                                   [1000 (exception/create-exception-middleware
                                           {::exception/default
                                            (partial exception/wrap-log-to-console
                                                     exception/http-response-handler)})]]
        fluree-middleware         (sort-middleware-by-weight
                                    (concat default-fluree-middleware
                                            middleware))]
    (ring/ring-handler
      (ring/router
        [["/swagger.json"
          {:get {:no-doc  true
                 :swagger {:info {:title "Fluree HTTP API"}}
                 :handler (swagger/create-swagger-handler)}}]
         ["/fluree" {:middleware fluree-middleware}
          ["/create"
           {:post {:summary    "Endpoint for creating new ledgers"
                   :parameters {:body {"ledger"   LedgerAlias
                                       "txn"      Transaction
                                       "defaults" (l/optional Defaults)}}
                   :responses  {201 {:body {"alias"   LedgerAlias
                                            "t"       TValue
                                            "address" (l/optional LedgerAddress)
                                            "id"      (l/optional DID)}}
                                409 {:body [:or :string :map]}
                                400 {:body [:or :string :map]}
                                500 {:body [:or :string :map]}}
                   :handler    ledger/create}}]
          ["/transact"
           {:post {:summary    "Endpoint for submitting transactions"
                   :parameters {:body {"ledger" LedgerAlias
                                       "txn"    Transaction
                                       "opts"   (l/optional TransactOpts)}}
                   :responses  {200 {:body {"alias"   LedgerAlias
                                            "t"       TValue
                                            "address" (l/optional LedgerAddress)
                                            "id"      (l/optional DID)}}
                                400 {:body [:or :string :map]}
                                500 {:body [:or :string :map]}}
                   :handler    ledger/transact}}]
          ["/query"
           {:get  query-endpoint
            :post query-endpoint}]
          ["/multi-query"
           {:get  multi-query-endpoint
            :post multi-query-endpoint}]
          ["/history"
           {:get  history-endpoint
            :post history-endpoint}]]]
        {:data {:coercion   reitit.coercion.malli/coercion
                :muuntaja   (muuntaja-instance)
                :middleware [swagger/swagger-feature
                             muuntaja-mw/format-negotiate-middleware
                             muuntaja-mw/format-response-middleware
                             muuntaja-mw/format-request-middleware]}})
      (ring/routes
        (ring/ring-handler
          (ring/router
            (concat
              [["/ws" {:get (fn [req]
                              (if (http/ws-upgrade-request? req)
                                (http/ws-upgrade-response websocket-handler)
                                {:status 400
                                 :body   "Invalid websocket upgrade request"}))}]
               routes])))
        (swagger-ui/create-swagger-ui-handler
          {:path   "/"
           :config {:validatorUrl     nil
                    :operationsSorter "alpha"}})
        (ring/create-default-handler)))))
