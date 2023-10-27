(ns fluree.http-api.handlers.ledger
  (:require
   [clojure.core.async :refer [<!!]]
   [clojure.string :as str]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.util.core :as util]
   [fluree.db.util.log :as log]
   [fluree.http-api.components.http :as-alias http]
   [fluree.http-api.components.txn-queue :as txn-queue])
  (:import (clojure.lang ExceptionInfo)))

(defn deref!
  "Derefs promise p and throws if the result is an exception, returns it otherwise."
  [p]
  (let [res @p]
    (if (util/exception? res)
      (throw res)
      res)))

(defmacro defhandler
  [name args & body]
  `(defn ~name ~args
     (try
       ~@body
       (catch ExceptionInfo e#
         (if (-> e# ex-data (contains? :response))
           (throw e#)
           (let [msg#   (ex-message e#)
                 {status# :status :as data# :or {status# 500}} (ex-data e#)
                 error# (dissoc data# :status)]
             (throw (ex-info "Error in ledger handler"
                             {:response
                              {:status status#
                               :body   (assoc error# :message msg#)}})))))
       (catch Throwable t#
         (throw (ex-info "Error in ledger handler"
                         {:response {:status 500
                                     :body   {:error (ex-message t#)}}}))))))

(defn header->content-type
  [ct-header]
  (if (re-matches #"application/(?:[^\+]+\+)?json(?:;.+)?" ct-header)
    :json
    (-> ct-header (str/split #"/") last keyword)))

(defn content-type->context-type
  [ct]
  (if (= :json ct) :string :keyword))

(defn query-body->opts
  [query content-type]
  (let [opts          (:opts query)
        content-type* (header->content-type content-type)]
    (assoc opts :context-type (content-type->context-type content-type*))))

(defn opts->context-type
  [opts content-type]
  (let [content-type* (header->content-type content-type)]
    (assoc opts :context-type (content-type->context-type content-type*))))

(defn ledger-summary
  [db]
  (assoc (-> db :ledger (select-keys [:alias :address]))
    :t (-> db :commit :data :t)))

(defhandler create
  [txn-queue
   {:keys          [content-type credential/did]
    {:keys [body]} :parameters}]
  (log/debug "create body:" body)
  (let [opts (cond-> (opts->context-type {} content-type)
               did (assoc :did did))
        _    (log/trace "create opts:" opts)
        db   (<!! (txn-queue/submit txn-queue #::txn-queue{:txn-type :create
                                                           :txn      body
                                                           :opts     opts}))]
    (log/trace "create-with-txn result:" db)
    (when (util/exception? db) (throw db))
    {:status 201
     :body   (ledger-summary db)}))

(defhandler transact
  [txn-queue
   {:keys          [content-type credential/did]
    {:keys [body]} :parameters}]
  (log/trace "transact handler req body:" body)
  (let [opts (cond-> (opts->context-type {} content-type)
               did (assoc :did did))
        _    (log/trace "transact handler opts:" opts)
        db   (<!! (txn-queue/submit txn-queue #::txn-queue{:txn-type :txn
                                                           :txn      body
                                                           :opts     opts}))]
    (log/trace "transact result:" db)
    (when (util/exception? db) (throw db))
    {:status 200
     :body   (ledger-summary db)}))

(defhandler query
  [{:keys [fluree/conn content-type credential/did]
    {:keys [body]} :parameters}]
  (let [query  (or (::http/query body) body)
        format (or (::http/format body) :fql)
        _      (log/debug "query handler received query:" query)
        opts   (when (= :fql format)
                 (cond-> (query-body->opts query content-type)
                   did (assoc :did did)))
        query* (if opts (assoc query :opts opts) query)]
    {:status 200
     :body   (deref! (fluree/query-connection conn query* {:format format}))}))

(defhandler history
  [{:keys [fluree/conn content-type credential/did] {{ledger :from :as query} :body} :parameters}]
  (log/debug "history handler got query:" query)
  (let [ledger* (->> ledger (fluree/load conn) deref!)
        opts    (cond-> (query-body->opts query content-type)
                  did (assoc :did did))
        query*  (-> query
                    (dissoc :from)
                    (assoc :opts opts))]
    (log/debug "history - Querying ledger" ledger "-" query*)
    (let [results (deref! (fluree/history ledger* query*))]
      (log/debug "history - query results:" results)
      {:status 200
       :body   results})))

(defhandler default-context
  [{:keys [fluree/conn] {{:keys [ledger t] :as body} :body} :parameters}]
  (log/debug "default-context handler got request:" body)
  (let [ledger* (->> ledger (fluree/load conn) deref!)
        results (if t
                  (-> ledger* (fluree/default-context-at-t t) deref)
                  (-> ledger* fluree/db fluree/default-context))]
    (log/debug "default-context for ledger" (str ledger ":") results)
    {:status 200
     :body   results}))
