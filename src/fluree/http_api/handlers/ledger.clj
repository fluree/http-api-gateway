(ns fluree.http-api.handlers.ledger
  (:require
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

(defn ledger-summary
  [{:keys [commit] {:keys [alias address]} :ledger :as db}]
  (log/debug "ledger-summary got:" db)
  {"alias" alias, "address" address, "t" (get-in commit [:data :t])})

(def create
  (error-catching-handler
    (fn [{:keys [fluree/conn] {{:strs [ledger txn] :as body} :body} :parameters}]
      (let [ledger-exists? (deref! (fluree/exists? conn ledger))]
        (log/debug "Ledger" ledger "exists?" ledger-exists?)
        (if ledger-exists?
          (let [err-message (str "Ledger " ledger " already exists")]
            (throw (ex-info err-message
                            {:response {:status 409
                                        :body   {:error err-message}}})))
          (do
            (log/info "Creating ledger" ledger)
            (let [opts    (select-keys body ["@context"])
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
    (fn [{:keys [fluree/conn] {{:strs [ledger txn opts]} :body} :parameters}]
      (println "\nTransacting to" ledger ":" (pr-str txn))
      (let [ledger  (if (deref! (fluree/exists? conn ledger))
                      (do
                        (log/debug "transact - Ledger" ledger
                                   "exists; loading it")
                        (deref! (fluree/load conn ledger)))
                      (throw (ex-info "Ledger does not exist" {:ledger ledger})))
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
    (fn [{:keys [fluree/conn] {{:strs [ledger query]} :body} :parameters}]
      (let [db     (->> ledger (fluree/load conn) deref! fluree/db)]
        (log/debug "query - Querying ledger" ledger "-" query)
        {:status 200
         :body   (deref! (fluree/query db query))}))))

(def multi-query
  (error-catching-handler
   (fn [{:keys [fluree/conn] {{:strs [ledger query]} :body} :parameters}]
     (let [db     (->> ledger (fluree/load conn) deref! fluree/db)]
       (log/debug "multi-query - Querying ledger" ledger "-" query)
       {:status 200
        :body   (deref! (fluree/multi-query db query))}))))

(def history
  (error-catching-handler
    (fn [{:keys [fluree/conn] {{:strs [ledger query]} :body} :parameters}]
      (let [ledger* (->> ledger (fluree/load conn) deref!)]
        (log/debug "history - Querying ledger" ledger "-" query)
        (let [results (deref! (fluree/history ledger* query))]
          (log/debug "history - Query results:" results)
          {:status 200
           :body results})))))
