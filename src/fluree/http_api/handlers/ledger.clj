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

(defn txn-body->opts
  [{:keys [defaultContext opts] :as _body} content-type]
  (let [content-type* (header->content-type content-type)]
    (cond-> opts
            true (assoc :context-type (content-type->context-type content-type*))
            defaultContext (assoc :defaultContext defaultContext))))

(defn query-body->opts
  [{:keys [query] :as _body} content-type]
  (let [opts (:opts query)
        content-type* (header->content-type content-type)]
    (assoc opts :context-type (content-type->context-type content-type*))))

(defn ledger-summary
  [db]
  (assoc (-> db :ledger (select-keys [:alias :address]))
         :t (-> db :commit :data :t)))

(def create
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type] {{:keys [ledger txn] :as body} :body} :parameters}]
      (let [ledger-exists? (deref! (fluree/exists? conn ledger))]
        (log/debug "Ledger" ledger "exists?" ledger-exists?)
        (if ledger-exists?
          (let [err-message (str "Ledger " ledger " already exists")]
            (throw (ex-info err-message
                            {:response {:status 409
                                        :body   {:error err-message}}})))
          (do
            (log/info "Creating ledger" ledger)
            (let [opts    (txn-body->opts body content-type)
                  _       (log/debug "create opts:" opts)
                  ledger* (deref! (fluree/create conn ledger opts))
                  db      (-> ledger*
                              fluree/db
                              (fluree/stage txn opts)
                              deref!
                              (->> (fluree/commit! ledger*))
                              deref!)]
              {:status 201
               :body   (ledger-summary db)})))))))

(def transact
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type] {{:keys [ledger txn] :as body} :body} :parameters}]
      (println "\nTransacting to" ledger ":" (pr-str txn))
      (let [ledger  (if (deref! (fluree/exists? conn ledger))
                      (do
                        (log/debug "transact - Ledger" ledger
                                   "exists; loading it")
                        (deref! (fluree/load conn ledger)))
                      (throw (ex-info "Ledger does not exist" {:ledger ledger})))
            opts    (txn-body->opts body content-type)
            ;; TODO: Add a transact! fn to f.d.json-ld.api that stages and commits in one step
            db      (-> ledger
                        fluree/db
                        (fluree/stage txn opts)
                        deref!
                        (->> (fluree/commit! ledger))
                        deref!)]
        {:status 200
         :body   (ledger-summary db)}))))

(def query
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type] {{:keys [ledger query] :as body} :body} :parameters}]
      (log/debug "query handler received body:" body)
      (let [db     (->> ledger (fluree/load conn) deref! fluree/db)
            query* (-> query
                       (->> (reduce-kv (fn [acc k v] (assoc acc (keyword k) v))
                                       {}))
                       (assoc :opts (query-body->opts body content-type)))]
        (log/debug "query - Querying ledger" ledger "-" query*)
        {:status 200
         :body   (deref! (fluree/query db query*))}))))

(def multi-query
  (error-catching-handler
   (fn [{:keys [fluree/conn content-type] {{:keys [ledger query] :as body} :body} :parameters}]
     (let [db     (->> ledger (fluree/load conn) deref! fluree/db)
           query* (-> (reduce-kv (fn [m k v]
                                   (assoc m k (util/keywordize-keys v)))
                                 {} query)
                      (assoc :opts (query-body->opts body content-type)))]
       (log/debug "multi-query - Querying ledger" ledger "-" query)
       {:status 200
        :body   (deref! (fluree/multi-query db query*))}))))

(def history
  (error-catching-handler
    (fn [{:keys [fluree/conn content-type] {{:keys [ledger query] :as body} :body} :parameters}]
      (log/debug "history handler got query:" query)
      (let [ledger* (->> ledger (fluree/load conn) deref!)
            query*  (assoc query :opts (query-body->opts body content-type))]
        (log/debug "history - Querying ledger" ledger "-" query*)
        (let [results (deref! (fluree/history ledger* query*))]
          (log/debug "history - query results:" results)
          {:status 200
           :body results})))))
