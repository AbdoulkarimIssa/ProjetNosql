#!/bin/bash

## Ce script remonte toute les données de la base parc vers hdfs

hdfs dfs -rmdir /user/maria_dev/parc/compteurs #suppression de l'ancien fichier
sqoop import --connect jdbc:mysql://192.168.1.17:3306/parc --username root --password root  --table compteurs -m 1 --target-dir /user/maria_dev/parc/compteurs

hdfs dfs -rmdir /user/maria_dev/parc/concentrateurs
sqoop import --connect jdbc:mysql://192.168.1.17:3306/parc --username root --password root  --table concentrateurs -m 1 --target-dir /user/maria_dev/parc/concentrateurs


