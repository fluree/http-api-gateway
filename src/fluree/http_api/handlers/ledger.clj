(ns fluree.http-api.handlers.ledger
  (:require
    [fluree.db.conn.proto :as conn-proto]
    [fluree.db.json-ld.api :as fdb]
    [fluree.db.util.core :as util]))

(defn deref!
  "Derefs promise p and throws if the result is an exception, returns it otherwise."
  [p]
  (let [res @p]
    (if (util/exception? res)
      (throw res)
      res)))

(defn create
  [{:keys [conn name]}]
  (println "Creating ledger" name)
  (deref! (fdb/create conn name)))

(defn transact
  [{:keys [fluree/conn] {{:keys [action ledger txn]} :body} :parameters}]
  (println "Transacting to" ledger ":" (pr-str txn))
  ;; TODO: Add a transact! fn to f.d.json-ld.api that stages and commits in one step
  (let [[ledger status]  (if-let [res (deref! (fdb/load-if-exists conn ledger))]
                           [res 200]
                           (if (= :new action)
                             (do
                               (println "Creating new ledger:" ledger)
                               [(create {:conn conn, :name ledger}) 201])
                             (throw (ex-info "Ledger does not exist" {:ledger ledger}))))
        address (:address ledger)
        db      (fdb/db ledger)
        db'     (deref! (fdb/stage db txn))
        db''    (deref! (fdb/commit! db'))]
    {:status status
     :body   (-> db''
                 (select-keys [:alias :t])
                 (assoc :address address))}))

(defn query
  [{:keys [fluree/conn] {{:keys [ledger query]} :body} :parameters}]
  (println "Querying" ledger ":" (pr-str query))
  (let [ledger (deref! (fdb/load conn ledger))
        db     (fdb/db ledger)]
    {:status 200
     :body   (deref! (fdb/query db query))}))
