(ns fluree.http-api.handlers.ledger
  (:require
   [clojure.string :as str]
   [fluree.db.json-ld.api :as fluree]
   [fluree.db.util.core :as util]
   [fluree.db.util.log :as log])
  (:import (clojure.lang ExceptionInfo)))

(defn deref!
  "Derefs promise p and throws if the result is an exception, returns it otherwise."
  [p]
  (let [res @p]
    (if (util/exception? res)
      (throw res)
      res)))

(defn error-catching-handler
  [handler]
  (fn [req]
    (try
      (handler req)
      (catch ExceptionInfo e
        (if (-> e ex-data (contains? :response))
          (throw e)
          (let [msg   (ex-message e)
                {:keys [status] :as data :or {status 500}} (ex-data e)
                error (dissoc data :status)]
            (throw (ex-info "Error in ledger handler"
                            {:response
                             {:status status
                              :body   (assoc error :message msg)}})))))
      (catch Throwable t
        (throw (ex-info "Error in ledger handler"
                        {:response {:status 500
                                    :body   {:error (ex-message t)}}}))))))

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
  (let [opts (:opts query)
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

(def create
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type credential/did]
          {{:keys [ledger txn defaultContext opts] :as body} :body} :parameters}]
      (let [ledger-exists? (deref! (fluree/exists? conn ledger))]
        (log/debug "Ledger" ledger "exists?" ledger-exists?)
        (if ledger-exists?
          (let [err-message (str "Ledger " ledger " already exists")]
            (throw (ex-info err-message
                            {:response {:status 409
                                        :body   {:error err-message}}})))
          (let [opts*    (cond-> (opts->context-type opts content-type)
                           defaultContext (assoc :defaultContext defaultContext)
                           did (assoc :did did))
                _       (log/info "Creating ledger" ledger opts*)
                ledger* (deref! (fluree/create conn ledger opts*))
                db      (-> ledger*
                            fluree/db
                            (fluree/stage txn opts*)
                            deref!
                            (->> (fluree/commit! ledger*))
                            deref!)]
            {:status 201
             :body   (ledger-summary db)}))))))

(def transact
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type credential/did]
          {:keys [body]} :parameters}]
      (let [opts    (cond-> (opts->context-type {} content-type)
                      did (assoc :did did))
            db      (-> (fluree/transact! conn body opts)
                        deref!)]
        {:status 200
         :body   (ledger-summary db)}))))

(def query
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type credential/did] {{ledger :from :as query} :body} :parameters}]
      (log/debug "query handler received query:" query)
      (let [db     (->> ledger (fluree/load conn) deref! fluree/db)
            opts (cond-> (query-body->opts query content-type)
                   did (assoc :did did))
            query* (-> query
                       (assoc :opts opts)
                       (dissoc :from))]
        (log/debug "query - Querying ledger" ledger "-" query*)
        {:status 200
         :body   (deref! (fluree/query db query*))}))))

(def history
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type credential/did] {{ledger :from :as query} :body} :parameters}]
      (log/debug "history handler got query:" query)
      (let [ledger* (->> ledger (fluree/load conn) deref!)
            opts (cond-> (query-body->opts query content-type)
                   did (assoc :did did))
            query*  (-> query
                        (dissoc :from)
                        (assoc :opts opts))]
        (log/debug "history - Querying ledger" ledger "-" query*)
        (let [results (deref! (fluree/history ledger* query*))]
          (log/debug "history - query results:" results)
          {:status 200
           :body results})))))

(def default-context
  (error-catching-handler
   (fn [{:keys [fluree/conn] {{:keys [ledger t] :as body} :body} :parameters}]
     (log/debug "default-context handler got request:" body)
     (let [ledger* (->> ledger (fluree/load conn) deref!)]
       (let [results (if t
                       (-> ledger* (fluree/default-context-at-t t) deref)
                       (-> ledger* fluree/db fluree/default-context))]
         (log/debug "default-context for ledger" (str ledger ":") results)
         {:status 200
          :body   results})))))
