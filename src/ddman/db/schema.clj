(ns ddman.db.schema
  (:require [clojure.java.jdbc :as sql]
            [noir.io :as io]))

(def db-store "site.db")

(def db-spec {:classname "org.h2.Driver"
              :subprotocol "h2"
              :subname (str (io/resource-path) db-store)
              :user "sa"
              :password ""
              :make-pool? true
              :naming {:keys clojure.string/lower-case
                       :fields clojure.string/upper-case}})
(defn initialized?
  "checks to see if the database schema is present"
  []
  (.exists (new java.io.File (str (io/resource-path) db-store ".mv.db"))))

(defn create-virtualshards-table
  []
  (sql/db-do-commands
    db-spec
    (sql/drop-table-ddl :virtualshards)
    (sql/create-table-ddl
      :virtualshards
      [:id  "varchar(20) PRIMARY KEY AUTO_INCREMENT"]
      [:shardid "varchar(20)"]
      [:virtualname "varchar(64)"]
      [:hashvalue "varchar(30)"])))

(defn create-groups-table
  []
  (sql/db-do-commands
    db-spec
    (sql/drop-table-ddl :groups)
    (sql/create-table-ddl
      :groups
      [:name "varchar(30) PRIMARY KEY"]
      [:keycolumn "varchar(30)"]
      [:hashfunction "varchar(256)"]
      [:database "varchar(24)"]
      [:virtualcount "int"])))

(defn create-shards-table
  []
  (sql/db-do-commands
    db-spec
    (sql/drop-table-ddl :shards)
    (sql/create-table-ddl
      :shards
      [:id "varchar(20) PRIMARY KEY AUTO_INCREMENT"]
      [:groupname "varchar(30)"]
      [:url "varchar(256)"]
      [:user "varchar(30)"]
      [:password "varchar(30)"]
      [:hashvalue "varchar(256)"]
      )))

(defn create-tables
  "creates the database tables used by the application"
  []
  (create-groups-table)
  (create-shards-table)
  (create-virtualshards-table))
