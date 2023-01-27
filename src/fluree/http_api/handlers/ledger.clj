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
  [{:keys [fluree/conn] {{:keys [ledger txn]} :body} :parameters}]
  (log/info "Creating ledger" ledger)
  (let [ledger* (deref! (fluree/create conn ledger))
        address (:address ledger*)
        db      (-> ledger*
                    fluree/db
                    (fluree/stage txn)
                    deref!
                    (->> (fluree/commit! ledger*))
                    deref!)]
    {:status 201
     :body (-> db (select-keys [:alias :t]) (assoc :address address))}))

(defn transact
  [{:keys [fluree/conn] {{:keys [ledger txn]} :body} :parameters}]
  (println "\nTransacting to" ledger ":" (pr-str txn))
  (let [ledger  (if (deref! (fluree/exists? conn ledger))
                  (do
                    (log/debug "transact - Ledger" ledger "exists; loading it")
                    (deref! (fluree/load conn ledger)))
                  (throw (ex-info "Ledger does not exist" {:ledger ledger})))
        address (:address ledger)
        ;; TODO: Add a transact! fn to f.d.json-ld.api that stages and commits in one step
        db      (-> ledger
                    fluree/db
                    (fluree/stage txn {:context-type :string})
                    deref!
                    (->> (fluree/commit! ledger))
                    deref!)]
    {:status 200
     :body   (-> db (select-keys [:alias :t]) (assoc :address address))}))

(defn query
  [{:keys [fluree/conn] {{:keys [ledger query]} :body} :parameters}]
  (let [db     (->> ledger (fluree/load conn) deref! fluree/db)
        query* (-> query
                   (->> (reduce-kv (fn [acc k v] (assoc acc (keyword k) v)) {}))
                   (assoc-in [:opts :context-type] :string))]
    (log/debug "query - Querying ledger" ledger "-" query*)
    {:status 200
     :body   (deref! (fluree/query db query*))}))

(defn history
  [{:keys [fluree/conn] {{:keys [ledger query]} :body} :parameters}]
  (let [ledger* (->> ledger (fluree/load conn) deref!)
        query*  (-> query
                    (->> (reduce-kv (fn [acc k v] (assoc acc (keyword k) v)) {}))
                    (assoc-in [:opts :context-type] :string))]
    (log/debug "history - Querying ledger" ledger "-" query*)
    {:status 200
     :body   (deref! (fluree/history ledger* query*))}))
