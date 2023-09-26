(ns fluree.http-api.components.txn-queue
  (:require [clojure.core.async :as async :refer [<!! >!! alt!! put!]]
            [donut.system :as ds]
            [fluree.db.json-ld.api :as fluree]
            [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

;; In theory you could parallelize transaction processing for different ledgers
;; but this component is typically used for dev environments where you don't
;; want to deal with raft / a whole cluster of servers. And typically you don't
;; care about performance / parallelization too much there, and often aren't
;; working with more than one ledger at a time anyway. So this implements a
;; simple global queue to ensure that only one transaction is processed at a
;; time for correctness.

(defn process-job
  [conn
   {:keys [::result-chan] {:keys [::txn ::opts ::txn-type]} ::req}]
  (log/trace "Processing" txn-type "job:" txn)
  (let [result (try
                 (if (= :create txn-type)
                   @(fluree/create-with-txn conn txn opts)
                   @(fluree/transact! conn txn opts))
                 (catch Exception e e))]
    (>!! result-chan result)))

(defn start!
  [conn {:keys [queue-len] :as cfg}]
  (log/info "Starting transaction processor with config:" cfg)
  (let [queue (async/chan queue-len)]
    (async/thread
     (loop []
       (let [job (<!! queue)]
         (when-not (nil? job)
           (process-job conn job)
           (recur)))))
    queue))

(defn stop!
  [queue]
  (log/info "Stopping transaction processor")
  (async/close! queue))

(def processor
  #::ds{:start  (fn [{{:keys [:fluree/txn-queue-len :fluree/connection]}
                      ::ds/config}]
                  (start! connection {:queue-len txn-queue-len}))
        :stop   (fn [{:keys [::ds/instance]}]
                  (stop! instance))
        :config {:fluree/txn-queue-len (ds/ref [:env :fluree/txn-queue :length])
                 :fluree/connection    (ds/ref [:fluree :conn])}})

(defn submit
  "Submits a transaction to the queue for processing and returns immediately
  with a channel that will contain the result (or an exception) once it's
  processed."
  [queue txn]
  (let [result-ch (async/chan 1)]
    (put! queue {::req txn ::result-chan result-ch}
          (fn [_] (log/trace "Transaction queued:" txn)))
    result-ch))
