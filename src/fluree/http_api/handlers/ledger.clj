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

(def DB-ATOM (atom {}))

(defn transact
  [{:keys [fluree/conn] {{:keys [action ledger txn defaultContext]} :body} :parameters}]
  (println "\n\nTransacting to" ledger ":" (pr-str txn))
  (let [ledger-name ledger
        in-memory? (get @DB-ATOM ledger)
        [ledger status] (if (or in-memory?
                                (deref! (fluree/exists? conn ledger)))
                          (do
                            (log/debug "transact - Ledger" ledger "exists; loading it")
                            [(or (get @DB-ATOM ledger)
                                 (deref! (fluree/load conn ledger))) 200])
                          (if (= :new (keyword action))
                            (do
                              (log/debug "transact - Ledger" ledger "does not exist; creating it")
                              [(create {:conn conn, :name ledger :default-context (or defaultContext (get txn "@context"))}) 201])
                            (throw (ex-info "Ledger does not exist" {:ledger ledger}))))
        address (:address ledger)
        ;; TODO: Add a transact! fn to f.d.json-ld.api that stages and commits in one step
        db-before (or (get @DB-ATOM ledger)
                      (fluree/db ledger))
        db      (-> db-before
                    (fluree/stage txn {:js? true})
                    deref!
                    fluree/commit!
                    deref!)]
    (swap! DB-ATOM assoc ledger-name db)
    {:status status
     :body   (-> db
                 (select-keys [:alias :t])
                 (assoc :address address))}))

(defn query
  [{:keys [fluree/conn] {{:keys [ledger query]} :body} :parameters}]
  (let [db     (or
                 (get @DB-ATOM ledger)
                 (->> ledger (fluree/load conn) deref! fluree/db))
        query* (reduce-kv (fn [acc k v] (assoc acc (keyword k) v)) {} query)]
    (log/debug "query - Querying ledger" ledger "-" query*)
    {:status 200
     :body   (deref! (fluree/query  db (assoc-in query* [:opts :js?] true)))}))
