(ns fluree.http-api.handlers.ledger
  (:require
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.util.core :as util]
    [fluree.db.util.log :as log])
  (:import (clojure.lang ExceptionInfo)))

(defn keywordize-keys
  "Transforms all top-level map keys to keywords."
  [m]
  (reduce-kv (fn [m* k v]
               (assoc m* (keyword k) v))
             {} m))

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

(defn txn-body->opts
  [{:keys [context txn] :as _body}]
  (let [first-txn (if (map? txn)
                    txn
                    (first txn))]
    (cond-> {}
            (-> first-txn keys first keyword?) (assoc :context-type :keyword)
            (-> first-txn keys first string?) (assoc :context-type :string)
            context (assoc :context context))))

(defn query-body->opts
  [{:keys [query] :as _body}]
  (cond-> {}
          (-> query keys first keyword?) (assoc :context-type :keyword)
          (-> query keys first string?) (assoc :context-type :string)))

(def create
  (error-catching-handler
    (fn [{:keys [fluree/conn] {{:keys [ledger txn] :as body} :body} :parameters}]
      (let [ledger-exists? (deref! (fluree/exists? conn ledger))]
        (log/debug "Ledger" ledger "exists?" ledger-exists?)
        (if ledger-exists?
          (let [err-message (str "Ledger " ledger " already exists")]
            (throw (ex-info err-message
                            {:response {:status 409
                                        :body   {:error err-message}}})))
          (do
            (log/info "Creating ledger" ledger)
            (let [opts    (txn-body->opts body)
                  _       (log/debug "create opts:" opts)
                  ledger* (deref! (fluree/create conn ledger opts))
                  address (:address ledger*)
                  db      (-> ledger*
                              fluree/db
                              (fluree/stage txn opts)
                              deref!
                              (->> (fluree/commit! ledger*))
                              deref!)]
              {:status 201
               :body   (-> db
                           (select-keys [:alias :t])
                           (assoc :address address))})))))))

(def transact
  (error-catching-handler
    (fn [{:keys [fluree/conn] {{:keys [ledger txn] :as body} :body} :parameters}]
      (println "\nTransacting to" ledger ":" (pr-str txn))
      (let [ledger  (if (deref! (fluree/exists? conn ledger))
                      (do
                        (log/debug "transact - Ledger" ledger
                                   "exists; loading it")
                        (deref! (fluree/load conn ledger)))
                      (throw (ex-info "Ledger does not exist" {:ledger ledger})))
            address (:address ledger)
            opts    (txn-body->opts body)
            ;; TODO: Add a transact! fn to f.d.json-ld.api that stages and commits in one step
            db      (-> ledger
                        fluree/db
                        (fluree/stage txn opts)
                        deref!
                        (->> (fluree/commit! ledger))
                        deref!)]
        {:status 200
         :body   (-> db (select-keys [:alias :t]) (assoc :address address))}))))

(def query
  (error-catching-handler
    (fn [{:keys [fluree/conn] {{:keys [ledger query] :as body} :body} :parameters}]
      (let [db     (->> ledger (fluree/load conn) deref! fluree/db)
            query* (-> query
                       (->> (reduce-kv (fn [acc k v] (assoc acc (keyword k) v))
                                       {}))
                       (assoc :opts (query-body->opts body)))]
        (log/debug "query - Querying ledger" ledger "-" query*)
        {:status 200
         :body   (deref! (fluree/query db query*))}))))


(def multi-query
  (error-catching-handler
   (fn [{:keys [fluree/conn] {{:keys [ledger multi-query]} :body} :parameters}]
     (let [db           (->> ledger (fluree/load conn) deref! fluree/db)
           multi-query* (reduce-kv (fn [m k v]
                                     (assoc m k (keywordize-keys v)))
                                   {} multi-query)]
       (log/debug "multi-query - Querying ledger" ledger "-" multi-query)
       {:status 200
        :body   (deref! (fluree/multi-query db (assoc-in multi-query* [:opts :js?] true)))}))))


(def history
  (error-catching-handler
    (fn [{:keys [fluree/conn] {{:keys [ledger query] :as body} :body} :parameters}]
      (let [ledger* (->> ledger (fluree/load conn) deref!)
            query*  (-> query
                        (->> (reduce-kv (fn [acc k v] (assoc acc (keyword k) v))
                                        {}))
                        (assoc :opts (query-body->opts body)))]
        (log/debug "history - Querying ledger" ledger "-" query*)
        {:status 200
         :body   (deref! (fluree/history ledger* query*))}))))
