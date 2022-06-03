#!/bin/bash

## Ce script remonte toute les données de la base parc vers hdfs

hdfs dfs -rm -r /user/maria_dev/parc/compteurs #suppression de l'ancien fichier
sqoop import --connect jdbc:mysql://161.3.45.94:3306/parc --username root --password root  --table compteurs -m 1 --target-dir /user/maria_dev/parc/compteurs

hdfs dfs -rm -r /user/maria_dev/parc/concentrateurs
sqoop import --connect jdbc:mysql://161.3.45.94:3306/parc --username root --password root  --table concentrateurs -m 1 --target-dir /user/maria_dev/parc/concentrateurs


