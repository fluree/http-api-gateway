(ns fluree.http-api.components.fluree
  (:require [donut.system :as ds]
            [fluree.db.json-ld.api :as db]
            [fluree.db.conn.proto :as conn-proto]))

(def conn
  #::ds{:start  (fn [{{:keys [options]} ::ds/config}]
                  @(db/connect options))
        :stop   (fn [{::ds/keys [instance]}]
                  ;; TODO: Add a close-connection fn to f.d.json-ld.api
                  (when instance (conn-proto/-close instance)))
        :config {:options
                 {:method      (ds/ref [:env :fluree/connection :method])
                  :parallelism (ds/ref [:env :fluree/connection :parallelism])
                  :storage-path (ds/ref [:env :fluree/connection :storage-path])}}})