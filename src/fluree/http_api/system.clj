(ns fluree.http-api.system
  (:require [donut.system :as ds]
            [aero.core :as aero]
            [clojure.java.io :as io]
            [fluree.http-api.components.http :as http]
            [fluree.http-api.components.fluree :as fluree]
            [fluree.db.util.log :as log])
  (:gen-class))

(defn env-config [& [profile]]
  (aero/read-config (io/resource "config.edn")
                    (when profile {:profile profile})))

(def base-system
  {::ds/defs
   {:env {}
    :fluree
    {:conn fluree/conn}
    :http
    {:server  http/server
     :handler #::ds{:start (fn [{{:keys [:fluree/connection]} ::ds/config}]
                            (http/app connection))
                    :config {:fluree/connection (ds/ref [:fluree :conn])}}}}})

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
        ec (env-config profile)]
    (log/debug "run-server cfg-overrides:" (pr-str cfg-overrides))
    (let [system (ds/start profile {[:env] (merge ec cfg-overrides)})]
      #(ds/stop system))))

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
