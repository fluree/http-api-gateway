(ns fluree.http-api.integration.default-context-test
  (:require [clojure.test :refer :all]
            [fluree.http-api.integration.test-system :refer :all]
            [jsonista.core :as json]))

(use-fixtures :once run-test-server)

(deftest ^:integration get-default-context-test
  (testing "can retrieve default context for a ledger"
    (let [ledger-name         (create-rand-ledger "get-default-context-test")
          default-context-req {:body    (json/write-value-as-string
                                         {:ledger ledger-name})
                               :headers json-headers}
          default-context-res (api-get :defaultContext default-context-req)]
      (is (= 200 (:status default-context-res)))
      (is (= {"ex"     "http://example.com/",
              "f"      "https://ns.flur.ee/ledger#",
              "foo"    "http://foobar.com/",
              "id"     "@id",
              "rdf"    "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
              "schema" "http://schema.org/",
              "type"   "@type"}
             (-> default-context-res :body json/read-value))))))

(deftest ^:integration update-default-context-test
  (testing "can update default context for a ledger"
    (let [ledger-name          (create-rand-ledger "get-default-context-test")
          default-context-req  {:body    (json/write-value-as-string
                                          {:ledger ledger-name})
                                :headers json-headers}
          default-context-res  (api-get :defaultContext default-context-req)
          default-context-0    (-> default-context-res :body json/read-value)
          update-req           {:body    (json/write-value-as-string
                                          {:ledger ledger-name
                                           :txn [{:ex/name "Foo"}]
                                           :opts
                                           {:defaultContext
                                            (-> default-context-0
                                                (assoc "foo-new"
                                                       (get default-context-0 "foo"))
                                                (dissoc "foo"))}})
                                :headers json-headers}
          update-res           (api-post :transact update-req)
          _                    (assert (= 200 (:status update-res)))
          default-context-res' (api-get :defaultContext default-context-req)
          default-context-1    (-> default-context-res' :body json/read-value)]
      (is (= 200 (:status update-res)))
      (is (= {"foo-new" "http://foobar.com/"
              "ex"      "http://example.com/"
              "f"       "https://ns.flur.ee/ledger#"
              "id"      "@id"
              "rdf"     "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
              "schema"  "http://schema.org/"
              "type"    "@type"}
             default-context-1)))))
