# Syrph The Net

Sous-module Monitoring GeoNature pour le protocole Syrph The Net.

## Installation

### 1) Import de la typologie et des habitats STN

Executer le script SQL en tant qu'utilisateur `postgres` :

```bash
cd for_install
sudo su postgres
psql -d geonature2db -f ajout_typo_habitats_stn.sql
exit
```

Ce script :
- cree la typologie `TYPO_STN` dans `ref_habitats.typoref` si necessaire
- importe le CSV `habitats_stn.csv` dans `ref_habitats.habref`
- repeuple `ref_habitats.autocomplete_habitat`

### 2) Generation de `site.json`

Generer ensuite `site.json` a partir du template, avec le `cd_typo` local :

```bash
cd for_install
./update_site_cd_typo.sh geonature2db geonatureadmin <CD_TYPO>
```

Pour recuperer la valeur de `CD_TYPO` :

```bash
sudo -u postgres psql -d geonature2db -tA -c "SELECT cd_typo FROM ref_habitats.typoref WHERE cd_table='TYPO_STN' LIMIT 1;"
```

Exemple :

```bash
./update_site_cd_typo.sh geonature2db geonatureadmin 37
```

### 3) Installation standard du sous-module

Suivre ensuite la procedure classique d'installation d'un sous-module Monitoring :
[Installation d'un sous-module](https://github.com/PnX-SI/gn_module_monitoring#installation-dun-sous-module)

