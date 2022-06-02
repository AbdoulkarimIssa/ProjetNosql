### Création des fausses données.

Nous avons décider de créer de fausses données afin de pouvoir réellement testé nos différents prototypes d'architectures.

Ainsi dans le dossier `./Fake_Date/` on peut retrouver les données suivantes : 
- Dans events : l'ensemble des fichiers csv qui repertorient les messages envoyés par l'ensemble des compteurs et concentrateurs du réseau. Nous avons des données sur 4 ans avec un fichier par jour.
- Dans SI_Park_Data : l'ensemble des données concernant le Park matériel et les informations reliées.
- Un fichier CSV téléchargé sur [le site de DataGouv](https://www.data.gouv.fr/fr/) qui fait la correspondance entre les communes, les départements et les régions.
- Un notebook Python, qui nous a permis de créer l'ensemble des données reparties dans les différents dossiers.

### Utilisation des fausses données.

A partir de ces données nous avons réaliser quelques proptotypes comme:
- Des jobs Spark de manipulation de données et d'extraction d'indicateurs.
- Un ETL Kafka qui s'occupe de générer les données envoyés au Broker Kafka via la lectures des fichiers du dossier events, puis un consumer qui lie les données envoyées au broker puis identifier la date d'occurence des evenements et les inscrires dans le bon sous dossier (partitionnement) en local. Par manque de temps nous n'avons pas utiliser le kafka connect vers le HDFS mais c'est une feature à developper.
- Des bashs Sqoop qui simulent le rapatriement de données de bases relationnelles vers le HDFS.

### Kafka prototype

Ce proptotype met en scène l'ingestion de données d'un Kafka producer à partir de fichiers plats csv, l'envoi vers le Kafka Broker, la récupération et l'analyse des lignes du fichiers par le Kafka Consumer pour finir par l'écriture de façon partionnée en local. 

Pour faire fonctionner cette dernière partie sur HDFS, il suffirait d'utiliser Kafka Connect avec un connecteur Sink vers HDFS.

Pour faire une démo, il suffit de récupérer le projet :
` git clone <url_projet>`

Aller dans le dossier Kafka_Prototype :
` cd Kafka_Prototype`

Avoir Docker d'installer [pour l'installer](https://docs.docker.com/get-docker/) puis lancer la commande :
` docker-compose up`

Ouvrir le notebook et éxécuter les commandes une à une.



