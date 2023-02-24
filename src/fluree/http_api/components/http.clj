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
    [ring.middleware.cors :as rmc]
    [fluree.db.util.log :as log]
    [malli.core :as m]
    [malli.experimental.lite :as l]
    [fluree.db.query.history :refer [HistoryQuery]]
    [fluree.db.query.fql.syntax :as fql]))

(set! *warn-on-reflection* true)

;; TODO: Flesh this out some more
(def non-empty-string (m/schema [:string {:min 1}]))
(def address non-empty-string)
(def id non-empty-string)
(def natural-int (m/schema [:int {:min 0}]))
(def t natural-int)
(def ledger-alias non-empty-string)
(def txn (m/schema [:orn
                    [:single-map [:map-of :any :any]]
                    [:collection-of-maps [:sequential map?]]]))
(def context (m/schema [:map-of :any :any]))

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
   :parameters {:body {:ledger ledger-alias
                       :query  (m/schema ::fql/analytical-query
                                         {:registry fql/registry})}}
   :responses  {200 {:body sequential?}
                400 {:body non-empty-string}
                500 {:body non-empty-string}}
   :handler    ledger/query})

(def multi-query-endpoint
  {:summary    "Endpoint for submitting multi-queries"
   :parameters {:body {:ledger ledger-alias
                       :query  (m/schema ::fql/multi-query
                                         {:registry fql/registry})}}
   :responses  {200 {:body map?}
                400 {:body non-empty-string}
                500 {:body non-empty-string}}
   :handler    ledger/multi-query})

(def history-endpoint
  {:summary    "Endpoint for submitting history queries"
   :parameters {:body {:ledger ledger-alias
                       :query  HistoryQuery}}
   :responses  {200 {:body sequential?}
                400 {:body non-empty-string}
                500 {:body non-empty-string}}
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
                   :parameters {:body [:map
                                       [:ledger ledger-alias]
                                       [:txn txn]
                                       [:context {:optional true} context]]}
                   :responses  {201 {:body [:map
                                            [:alias ledger-alias]
                                            [:t t]
                                            [:address {:optional true} address]
                                            [:id {:optional true} id]]}
                                400 {:body non-empty-string}
                                500 {:body non-empty-string}}
                   :handler    ledger/create}}]
          ["/transact"
           {:post {:summary    "Endpoint for submitting transactions"
                   :parameters {:body {:ledger ledger-alias
                                       :txn    txn}}
                   :responses  {200 {:body {:alias   ledger-alias
                                            :t       t
                                            :address (l/optional address)
                                            :id      (l/optional id)}}
                                400 {:body non-empty-string}
                                500 {:body non-empty-string}}
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
                :muuntaja   muuntaja/instance
                :middleware [swagger/swagger-feature
                             muuntaja/format-negotiate-middleware
                             muuntaja/format-response-middleware
                             muuntaja/format-request-middleware]}})
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
