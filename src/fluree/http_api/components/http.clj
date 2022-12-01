(ns fluree.http-api.components.http
  (:require
    [donut.system :as ds]
    [org.httpkit.server :as http]
    [reitit.ring :as ring]
    [reitit.coercion.spec]
    [reitit.swagger :as swagger]
    [reitit.swagger-ui :as swagger-ui]
    [reitit.ring.coercion :as coercion]
    [reitit.ring.middleware.muuntaja :as muuntaja]
    [reitit.ring.middleware.exception :as exception]
    [fluree.http-api.handlers.ledger :as ledger]
    [clojure.spec.alpha :as s]
    [muuntaja.core :as m]))

;; TODO: Flesh this out some more
(s/def ::non-empty-string (s/and string? #(< 0 (count %))))
(s/def ::address ::non-empty-string)
(s/def ::id ::non-empty-string)
(s/def ::t neg-int?)
(s/def ::alias ::non-empty-string)
(s/def ::action #{:new :insert})

(def server
  #::ds{:start  (fn [{{:keys [handler options]} ::ds/config}]
                  (http/run-server handler options))
        :stop   (fn [{::ds/keys [instance]}]
                  (http/server-stop! instance))
        :config {:handler (ds/local-ref [:handler])
                 :options
                 {:port                 (ds/ref [:env :http/server :port])
                  :legacy-return-value? false}}})

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

(defn app
  [conn]
  (ring/ring-handler
    (ring/router
      [["/swagger.json"
        {:get {:no-doc  true
               :swagger {:info {:title "Fluree HTTP API"}}
               :handler (swagger/create-swagger-handler)}}]
       ["/fdb" {:middleware [(partial wrap-assoc-conn conn)]}
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
              :muuntaja   m/instance
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
      (swagger-ui/create-swagger-ui-handler
        {:path   "/"
         :config {:validatorUrl     nil
                  :operationsSorter "alpha"}})
      (ring/create-default-handler))))
