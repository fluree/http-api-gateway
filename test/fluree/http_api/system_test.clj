(ns fluree.http-api.system-test
  (:require [clojure.test :refer :all]
            [donut.system :as ds]
            [fluree.http-api.system :as sys]
            [clj-http.client :as http]
            [jsonista.core :as json]
            [clojure.edn :as edn])
  (:import (java.net ServerSocket)))

(defn find-open-port
  ([] (find-open-port nil))
  ([_] ; so it can be used in swap!
   (let [socket (ServerSocket. 0)]
     (.close socket)
     (.getLocalPort socket))))

(defonce api-port (atom nil))

(defmethod ds/named-system :test
  [_]
  (ds/system :dev {[:env] {:http/server {:port @api-port}
                           :fluree/connection
                           {:method      :memory
                            :parallelism 1
                            :defaults
                            {:context
                             {:id     "@id"
                              :type   "@type"
                              :ex     "http://example.com/"
                              :schema "http://schema.org/"}}}}}))

(defn run-test-server
  [run-tests]
  (swap! api-port find-open-port)
  (let [stop-server (sys/run-server {:profile :test})]
    (run-tests)
    (stop-server)))

(use-fixtures :once run-test-server)

(defn api-url [endpoint]
  (str "http://localhost:" @api-port "/fluree/" (name endpoint)))

(defn post [endpoint req]
  (http/post (api-url endpoint) (assoc req :throw-exceptions false)))

(defn create-rand-ledger
  [name-root]
  (let [ledger-name (str name-root "-" (random-uuid))
        req         (pr-str {:ledger  ledger-name
                             :context {:foo "http://foobar.com/"}
                             :txn     [{:id      :ex/create-test
                                        :type    :foo/test
                                        :ex/name "create-endpoint-test"}]})
        headers     {"Content-Type" "application/edn"
                     "Accept"       "application/edn"}
        res         (update (post :create {:body req :headers headers})
                            :body edn/read-string)]
    (if (= 201 (:status res))
      (get-in res [:body :alias])
      (throw (ex-info "Error creating random ledger" res)))))

(deftest ^:integration create-endpoint-test
  (testing "can create a new ledger w/ JSON"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                        {:ledger  ledger-name
                         :context {:foo "http://foobar.com/"}
                         :txn     [{:id      :ex/create-test
                                    :type    :foo/test
                                    :ex/name "create-endpoint-test"}]})
          headers     {"Content-Type" "application/json"
                       "Accept"       "application/json"}
          res         (post :create {:body req :headers headers})]
      (is (= 201 (:status res)))
      (is (= {:address address
              :alias   ledger-name
              :t       -1}
             (-> res :body (json/read-value json/keyword-keys-object-mapper))))))
  (testing "can create a new ledger w/ EDN"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (pr-str {:ledger  ledger-name
                               :context {:foo "http://foobar.com/"}
                               :txn     [{:id      :ex/create-test
                                          :type    :foo/test
                                          :ex/name "create-endpoint-test"}]})
          headers     {"Content-Type" "application/edn"
                       "Accept"       "application/edn"}
          res         (post :create {:body req :headers headers})]
      (is (= 201 (:status res)))
      (is (= {:address address
              :alias   ledger-name
              :t       -1}
             (-> res :body edn/read-string))))))

;; TODO: Make load work in memory conns and then reenable
#_(deftest ^:integration transaction-test
    (testing "can transact in JSON"
      (let [ledger-name (create-rand-ledger "transact-endpoint-test")
            _           (println "RAND LEDGER:" ledger-name)
            address     (str "fluree:memory://" ledger-name "/main/head")
            req         (json/write-value-as-string
                          {:ledger  ledger-name
                           :context {:bar "http://barfoo.com/"}
                           :txn     {:id      :ex/transaction-test
                                     :type    :bar/test
                                     :ex/name "transact-endpoint-test"}})
            headers     {"Content-Type" "application/json"
                         "Accept"       "application/json"}
            res         (post :transact {:body req :headers headers})]
        (is (= 200 (:status res)))
        (is (= {:address address, :alias ledger-name, :t -2}
               (-> res :body (json/read-value json/keyword-keys-object-mapper)))))))

(deftest ^:integration query-test
  ;; TODO
  (is true))