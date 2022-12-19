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
  [{:keys [conn name]}]
  (log/info "Creating ledger" name)
  (deref! (fluree/create conn name)))

(defn transact
  [{:keys [fluree/conn] {{:keys [action ledger txn]} :body} :parameters}]
  (println "\n\nTransacting to" ledger ":" (pr-str txn))
  (let [[ledger status] (if (deref! (fluree/exists? conn ledger))
                          (do
                            (log/debug "transact - Ledger" ledger "exists; loading it")
                            [(deref! (fluree/load conn ledger)) 200])
                          (if (= :new action)
                            (do
                              (log/debug "transact - Ledger" ledger "does not exist; creating it")
                              [(create {:conn conn, :name ledger}) 201])
                            (throw (ex-info "Ledger does not exist" {:ledger ledger}))))
        address (:address ledger)
        ;; TODO: Add a transact! fn to f.d.json-ld.api that stages and commits in one step
        db      (-> ledger
                    fluree/db
                    (fluree/stage txn)
                    fluree/commit!)]
    {:status status
     :body   (-> db
                 (select-keys [:alias :t])
                 (assoc :address address))}))

(defn query
  [{:keys [fluree/conn] {{:keys [ledger query]} :body} :parameters}]
  (let [db (->> ledger (fluree/load conn) deref! fluree/db)]
    (log/debug "query - Querying ledger" ledger "-" query)
    {:status 200
     :body   (deref! (fluree/query db query))}))
