## Migrer les fichiers vers la machine

Commande de la VM
```bash
scp -P 2222 ~/Documents/DE2/DATA/NoSQL2/ProjetNosql/scoop/*.sh maria_dev@localhost:/home/maria_dev/sqoopsh/
```

## Lancement des scripts

```bash 
chmod +x fichiers.sh

##après
sh fichiers.sh
```
## Cron pour les jobs

Creation des cron dans le node

```bash 
crontab -e
``` 
```bash

0 5 * * * /path/nom_fichier.sh 

```