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
        (prn t)
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

(defn txn-body->opts
  [{:keys [defaultContext opts] :as _body} content-type]
  (let [content-type* (header->content-type content-type)]
    (cond-> (assoc opts :context-type (content-type->context-type content-type*))
      defaultContext (assoc :defaultContext defaultContext))))

(defn query-body->opts
  [query content-type]
  (let [opts (:opts query)
        content-type* (header->content-type content-type)]
    (assoc opts :context-type (content-type->context-type content-type*))))

(defn ledger-summary
  [db]
  (assoc (-> db :ledger (select-keys [:alias :address]))
         :t (-> db :commit :data :t)))

(def create
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type credential/did]
          {{:keys [ledger txn] :as body} :body} :parameters}]
      (let [ledger-exists? (deref! (fluree/exists? conn ledger))]
        (log/debug "Ledger" ledger "exists?" ledger-exists?)
        (if ledger-exists?
          (let [err-message (str "Ledger " ledger " already exists")]
            (throw (ex-info err-message
                            {:response {:status 409
                                        :body   {:error err-message}}})))
          (let [opts    (cond-> (txn-body->opts body content-type)
                          did (assoc :did did))
                _       (log/info "Creating ledger" ledger opts)
                ledger* (deref! (fluree/create conn ledger opts))
                db      (-> ledger*
                            fluree/db
                            (fluree/stage txn opts)
                            deref!
                            (->> (fluree/commit! ledger*))
                            deref!)]
            {:status 201
             :body   (ledger-summary db)}))))))

(def transact
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type credential/did]
          {{:keys [ledger txn] :as body} :body} :parameters}]
      (println "\nTransacting to" ledger ":" (pr-str txn))
      (let [ledger  (if (deref! (fluree/exists? conn ledger))
                      (do
                        (log/debug "transact - Ledger" ledger
                                   "exists; loading it")
                        (deref! (fluree/load conn ledger)))
                      (throw (ex-info "Ledger does not exist" {:ledger ledger})))

            {:keys [defaultContext] :as opts} (txn-body->opts body content-type)

            opts    (cond-> opts
                      did (assoc :did did))
            db      (fluree/db ledger)
            db      (if defaultContext
                      (do
                        (log/trace "Updating default context to:" defaultContext)
                        (fluree/update-default-context db defaultContext))
                      db)
            db      (-> db
                        (fluree/stage txn opts)
                        deref!
                        (->> (fluree/commit! ledger))
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
