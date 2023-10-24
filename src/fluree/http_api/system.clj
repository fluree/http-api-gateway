(ns fluree.http-api.system
  (:require [aero.core :as aero]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [donut.system :as ds]
            [fluree.db.util.log :as log]
            [fluree.http-api.components.fluree :as fluree]
            [fluree.http-api.components.http :as http]
            [fluree.http-api.components.txn-queue :as txn-queue]
            [jsonista.core :as json])
  (:gen-class)
  (:import (java.io PushbackReader)))

(defmethod aero/reader 'include-json-or-edn
  [_opts _tag value]
  (when value
    (cond
      (str/ends-with? value ".edn")
      (with-open [r (io/reader value)]
        (log/debug "Reading EDN file at" value)
        (-> r (PushbackReader.) edn/read))

      (str/ends-with? value ".json")
      (with-open [r (io/reader value)]
        (log/debug "Reading JSON file at" value)
        (json/read-value r)))))

(defn env-config [& [profile]]
  (aero/read-config (io/resource "config.edn")
                    (when profile {:profile profile})))

(def base-system
  {::ds/defs
   {:env
    {:http/server       {}
     :fluree/connection {}
     :fluree/txn-queue  {}}
    :fluree
    {:conn      fluree/conn
     :txn-queue txn-queue/processor}
    :http
    {:server  http/server
     :handler #::ds{:start  (fn [{{:keys [:fluree/connection :fluree/txn-queue]
                                   :as cfg
                                   {:keys [routes middleware]} :http}
                                  ::ds/config}]
                              (log/debug "ds/config:" cfg)
                              (http/app {:fluree/conn      connection
                                         :fluree/txn-queue txn-queue
                                         :http/routes      routes
                                         :http/middleware  middleware}))
                    :config {:http              (ds/ref [:env :http/server])
                             :fluree/connection (ds/ref [:fluree :conn])
                             :fluree/txn-queue  (ds/ref [:fluree :txn-queue])}}}}})

(defmethod ds/named-system :base
  [_]
  base-system)

(defmethod ds/named-system :dev
  [_]
  (let [ec (env-config :dev)]
    (log/info "dev config:" (pr-str ec))
    (ds/system :base {[:env] ec})))

(defmethod ds/named-system ::ds/repl
  [_]
  (ds/system :dev))

(defmethod ds/named-system :prod
  [_]
  (ds/system :base {[:env] (env-config :prod)}))

(defmethod ds/named-system :docker
  [_]
  (ds/system :prod {[:env] (env-config :docker)}))

(defn run-server
  "Runs an HTTP API server in a thread, with :profile from opts or :dev by
  default. Any other keys in opts override config from the profile.
  Returns a zero-arity fn to shut down the server."
  [{:keys [profile] :or {profile :dev} :as opts}]
  (let [cfg-overrides (dissoc opts :profile)
        overide-cfg   #(merge-with merge % cfg-overrides)
        _             (log/debug "run-server cfg-overrides:" cfg-overrides)
        system        (ds/start profile overide-cfg)]
    #(ds/stop system)))

(defn -main
  [& args]
  (let [profile (or (-> args first keyword) :prod)]
    (ds/start profile)))


(comment ; REPL utils

  (require 'donut.system.repl)

  ;; start REPL system
  (donut.system.repl/start)

  ;; restart REPL system
  (donut.system.repl/restart)

  ;; stop REPL system
  (donut.system.repl/stop))
