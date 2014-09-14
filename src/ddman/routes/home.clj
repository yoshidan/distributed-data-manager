(ns ddman.routes.home
  (:require [compojure.core :refer :all]
            [clojure.string :as str]
            [noir.response :as noir]
            [noir.session :as session]
            [ddman.layout :as layout]
            [ddman.db.core :as dbcore]
            [ddman.util :as util]))

(defn search-page []
  (layout/render "search.html" {:url "thin:@192.168.33.22:1521:XE"} ))

(defn group-page [groupname]
  (layout/render "group.html" {:group (dbcore/get-group groupname) :shards (dbcore/search-shards groupname)
                              :pshards (dbcore/search-physical-shards groupname)
                              :info  (session/flash-get :info) :error (session/flash-get :error)}))

(defn init-group [url username password dialect tables virtualcounts hashfunctions keycolumns]
  (try
    (do
      (apply list (map #(dbcore/init-group %1 url username password %3 dialect %2 %4)
        (str/split tables #"\s+") (str/split virtualcounts #"\s+") (str/split hashfunctions #"\s+") (str/split keycolumns #"\s+")))
        (noir/redirect (str "/group?groupname=" (first (str/split tables #"\s+")) )))
    (catch Exception e
      (do
        (.printStackTrace e)
        (layout/render "search.html" { :error (str "Init Error : " (.getMessage e))
                   :result (dbcore/search url username password dialect)
                   :url url :username username :password password :dialect dialect})))))

(defn search [url username password dialect]
  (layout/render "search.html" {
                                 :result (dbcore/search url username password dialect)
                                  :url url :username username :password password :dialect dialect}))

;シャード追加
(defn add-shard [groupname url username password]
  (try
    (do
      (dbcore/add-shard groupname url username password)
      (session/flash-put! :info "Add Complete "))
  (catch Exception e
    (do
      (.printStackTrace e)
      (session/flash-put! :error (str "Add Error : " (.getMessage e) )))))
  (noir/redirect (str "/group?groupname=" groupname )))

;リバランス処理
(defn rebalance [groupname]
  (try
    (session/flash-put! :info (str "Rebalance Complete : " (dbcore/rebalance groupname) " moved"))
    (catch Exception e
        (.printStackTrace e)
        (session/flash-put! :error (str "Rebalance Error : " (.getMessage e) ))))
  (noir/redirect (str "/group?groupname=" groupname )))

;リリース処理
(defn release [groupname url user]
  (try
    (session/flash-put! :info (str "Release Complete : " (dbcore/release groupname url user) " moved"))
    (catch Exception e
      (.printStackTrace e)
      (session/flash-put! :error (str "Release Error : " (.getMessage e) ))))
  (noir/redirect (str "/group?groupname=" groupname )))

(defn home-page []
  (layout/render
    "home.html" {:content (util/md->html "/md/docs.md")}))

(defn about-page []
  (layout/render "about.html"))

(defroutes home-routes
  (GET "/" [] (home-page))
  (GET "/about" [] (about-page))
  (GET "/search" [] (search-page))
  (POST "/rebalance" [groupname] (rebalance groupname))
  (POST "/search" [url username password dialect] (search url username password dialect))
  (POST "/shard" [groupname url username password] (add-shard groupname url username password))
  (GET "/group" [groupname] (group-page groupname))
  (POST "/release" [groupname url user] (release groupname url user))
  (POST "/group/init" [url username password dialect tables virtualcounts hashfunctions keycolumns] (init-group url username password dialect tables virtualcounts hashfunctions keycolumns)))
