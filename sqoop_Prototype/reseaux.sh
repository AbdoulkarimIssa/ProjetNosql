#! bin/bash

## Ce script remonte les tables de la base de données reseaux vers HDFS

hdfs dfs -rmdir /user/maria_dev/reseaux/pdc
sqoop import --connect jdbc:mysql://192.168.1.17:3306/parc --username root --password root  --table pdc -m 1 --target-dir /user/maria_dev/reseaux/pdc

hdfs dfs -rmdir /user/maria_dev/reseaux/pdk
sqoop import --connect jdbc:mysql://192.168.1.17:3306/parc --username root --password root  --table pdk -m 1 --target-dir /user/maria_dev/reseaux/pdk

hdfs dfs -rmdir /user/maria_dev/reseaux/communes
sqoop import --connect jdbc:mysql://192.168.1.17:3306/parc --username root --password root  --table communes -m 1 --target-dir /user/maria_dev/reseaux/communes

hdfs dfs -rmdir /user/maria_dev/reseaux/regions
sqoop import --connect jdbc:mysql://192.168.1.17:3306/parc --username root --password root  --table region -m 1 --target-dir /user/maria_dev/reseaux/regions

hdfs dfs -rmdir /user/maria_dev/reseaux/departement
sqoop import --connect jdbc:mysql://192.168.1.17:3306/parc --username root --password root  --table departements -m 1 --target-dir /user/maria_dev/reseaux/departement




