(ns fluree.http-api.integration.basic-transaction-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [fluree.http-api.integration.test-system :refer :all]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration ^:json create-endpoint-json-test
  (testing "can create a new ledger w/ JSON"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                       {"f:ledger" ledger-name
                        "@context" ["" {"foo" "http://foobar.com/"}]
                        "@graph"   [{"id"      "ex:create-test"
                                     "type"    "foo:test"
                                     "ex:name" "create-endpoint-test"}]})
          _           (println "REQUEST:" (pr-str req))
          res         (api-post :create {:body req :headers json-headers})]
      (is (= 201 (:status res))
          (str "response was:" (pr-str res)))
      (is (= {"address" address
              "alias"   ledger-name
              "t"       1}
             (-> res :body json/read-value)))))
  (testing "responds with 409 error if ledger already exists"
    (let [ledger-name (str "create-endpoint-" (random-uuid))
          req         (json/write-value-as-string
                       {"f:ledger" ledger-name
                        "@context" {"f" "https://ns.flur.ee/ledger#"} #_["" {"foo" "http://foobar.com/"}]
                        "@graph"   [{"id"      "ex:create-test"
                                     "type"    "foo:test"
                                     "ex:name" "create-endpoint-test"}]})
          res-success (api-post :create {:body req :headers json-headers})
          _           (assert (= 201 (:status res-success)))
          res-fail    (api-post :create {:body req :headers json-headers})]
      (is (= 409 (:status res-fail))))))

(deftest ^:integration ^:edn create-endpoint-edn-test
    (testing "can create a new ledger w/ EDN"
      (let [ledger-name (str "create-endpoint-" (random-uuid))
            address     (str "fluree:memory://" ledger-name "/main/head")
            req         (pr-str {:f/ledger ledger-name
                                 :context  ["" {:foo "http://foobar.com/"}]
                                 :graph    [{:id      :ex/create-test
                                             :type    :foo/test
                                             :ex/name "create-endpoint-test"}]})
            res         (api-post :create {:body req :headers edn-headers})]
        (is (= 201 (:status res)))
        (is (= {:address address
                :alias   ledger-name
                :t       1}
               (-> res :body edn/read-string)))))
    (testing "responds with 409 error if ledger already exists"
      (let [ledger-name (str "create-endpoint-" (random-uuid))
            req         (pr-str {:f/ledger ledger-name
                                 :context  ["" {:foo "http://foobar.com/"}]
                                 :graph    [{:id      :ex/create-test
                                             :type    :foo/test
                                             :ex/name "create-endpoint-test"}]})
            res-success (api-post :create {:body req :headers edn-headers})
            _           (assert (= 201 (:status res-success)))
            res-fail    (api-post :create {:body req :headers edn-headers})]
        (is (= 409 (:status res-fail))))))

(deftest ^:integration ^:json transaction-json-test
  (testing "can transact in JSON"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                        {"f:ledger" ledger-name
                         "@graph"   {"id"      "ex:transaction-test"
                                     "type"    "schema:Test"
                                     "ex:name" "transact-endpoint-json-test"}})
          res         (api-post :transact {:body req :headers json-headers})]
      (is (= 200 (:status res)))
      (is (= {"address" address, "alias" ledger-name, "t" 2}
             (-> res :body json/read-value)))))
  (testing "can transact in JSON with top-level @context"
    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                        {"f:ledger" ledger-name
                         "@context" ["" {"foo" "http://foo.com"}]
                         "@graph"   {"id"      "ex:transaction-test"
                                     "type"    "schema:Test"
                                     "foo:bar" "Baz"
                                     "ex:name" "transact-endpoint-json-test"}})
          res         (api-post :transact {:body req :headers json-headers})]
      (is (= 200 (:status res)))
      (is (= {"address" address, "alias" ledger-name, "t" 2}
             (-> res :body json/read-value))))

    (let [ledger-name (create-rand-ledger "transact-endpoint-json-test")
          address     (str "fluree:memory://" ledger-name "/main/head")
          req         (json/write-value-as-string
                        {"f:ledger" ledger-name
                         "@context" ["" {"foo" "http://foo.com"}]
                         "@graph"   [{"id"      "ex:transaction-test"
                                      "type"    "schema:Test"
                                      "foo:bar" "Baz"
                                      "ex:name" "transact-endpoint-json-test"}
                                     {"id"      "ex:transaction-test2"
                                      "type"    "schema:Test"
                                      "foo:bar" "Quux"
                                      "ex:name" "transact-endpoint-json-test2"}]})
          res         (api-post :transact {:body req :headers json-headers})]
      (is (= 200 (:status res)))
      (is (= {"address" address, "alias" ledger-name, "t" 2}
             (-> res :body json/read-value))))))

(deftest ^:integration ^:edn transaction-edn-test
    (testing "can transact in EDN"
      (let [ledger-name (create-rand-ledger "transact-endpoint-edn-test")
            address     (str "fluree:memory://" ledger-name "/main/head")
            req         (pr-str
                          {:f/ledger ledger-name
                           :graph    [{:id      :ex/transaction-test
                                       :type    :schema/Test
                                       :ex/name "transact-endpoint-edn-test"}]})
            res         (api-post :transact {:body req :headers edn-headers})]
        (is (= 200 (:status res)))
        (is (= {:address address, :alias ledger-name, :t 2}
               (-> res :body edn/read-string))))))
