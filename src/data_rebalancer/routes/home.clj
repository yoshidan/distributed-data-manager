(ns data-rebalancer.routes.home
  (:require [compojure.core :refer :all]
            [clojure.string :as str]
            [noir.response :as noir]
            [data-rebalancer.layout :as layout]
            [data-rebalancer.db.core :as dbcore]
            [data-rebalancer.util :as util]))

(defn search-page []
  (layout/render "search.html" {:url "thin:@192.168.33.22:1521:XE"} ))

(defn group-page [groupname]
  (layout/render "group.html" {:group (dbcore/get-group groupname) :shards (dbcore/search-shards groupname)}))

(defn init-group [url username password dialect tables virtualcounts hashfunctions keycolumns]
  (apply list (map #(dbcore/init-group 1 %1 url username password %3 dialect %2 %4)
      (str/split tables #"\s+") (str/split virtualcounts #"\s+") (str/split hashfunctions #"\s+") (str/split keycolumns #"\s+")))
  (noir/redirect (str "/group?groupname=" (first (str/split tables #"\s+")) )))

(defn search [url username password dialect]
  (layout/render "search.html" {
                                 :result (dbcore/search url username password dialect)
                                  :url url :username username :password password :dialect dialect}))

(defn show-ddl [groupname url username]
  (dbcore/show-ddl groupname url username))

(defn add-shard [groupname url username password]
  (dbcore/add-shard groupname url username password)
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
  (POST "/search" [url username password dialect] (search url username password dialect))
  (POST "/shard" [groupname url username password] (add-shard groupname url username password))
  (GET "/group" [groupname] (group-page groupname))
  (POST "/group/init" [url username password dialect tables virtualcounts hashfunctions keycolumns] (init-group url username password dialect tables virtualcounts hashfunctions keycolumns)))
