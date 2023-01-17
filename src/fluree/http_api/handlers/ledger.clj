(ns fluree.http-api.handlers.ledger
  (:require
    [fluree.db.json-ld.api :as fluree]
    [fluree.db.util.core :as util]
    [fluree.db.util.log :as log]))

(defn deref!
  "Derefs promise p and throws if the result is an exception, returns it otherwise."
  [p]
  (let [res @p]
    (if (util/exception? res)
      (throw res)
      res)))

(defn create
  [{:keys [conn name default-context]}]
  (log/info "Creating ledger" name)
  (log/debug "New ledger default context:" default-context)
  (deref! (fluree/create conn name {:context default-context})))

(defn transact
  [{:keys [fluree/conn] {{:keys [action ledger txn defaultContext]} :body} :parameters}]
  (println "\nTransacting to" ledger ":" (pr-str txn))
  (let [[ledger status] (if (deref! (fluree/exists? conn ledger))
                          (do
                            (log/debug "transact - Ledger" ledger "exists; loading it")
                            [(deref! (fluree/load conn ledger)) 200])
                          (if (= :new (keyword action))
                            (do
                              (log/debug "transact - Ledger" ledger "does not exist; creating it")
                              [(create {:conn conn, :name ledger :default-context (or defaultContext (get txn "@context"))}) 201])
                            (throw (ex-info "Ledger does not exist" {:ledger ledger}))))
        address (:address ledger)
        ;; TODO: Add a transact! fn to f.d.json-ld.api that stages and commits in one step
        db      (-> ledger
                    fluree/db
                    (fluree/stage txn {:js? true})
                    deref!
                    fluree/commit!
                    deref!)]
    {:status status
     :body   (-> db
                 (select-keys [:alias :t])
                 (assoc :address address))}))

(defn query
  [{:keys [fluree/conn] {{:keys [ledger query]} :body} :parameters}]
  (let [db     (->> ledger (fluree/load conn) deref! fluree/db)
        query* (reduce-kv (fn [acc k v] (assoc acc (keyword k) v)) {} query)]
    (log/debug "query - Querying ledger" ledger "-" query*)
    {:status 200
     :body   (deref! (fluree/query db (assoc-in query* [:opts :js?] true)))}))
