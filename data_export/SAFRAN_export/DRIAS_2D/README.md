# DRIAS export 2D

## Introduction
### Avant propos
L'objectif de ce code est de simplifier l'export de donnée [NetCDF](https://fr.wikipedia.org/wiki/NetCDF) pour le portail [DRIAS](http://www.drias-climat.fr/accompagnement/sections/311) des données hydro-climatiques de [Explore2](https://professionnels.ofb.fr/fr/node/1244). 

L'ensemble de ce code et des informations qu'il contient on été tiré du pdf [Format_NetCDF_SAFRAN_2D_Hydro_Portail-DRIAS_v2023.05.pdf](https://github.com/louis-heraut/Explore2_toolbox/blob/main/resources/Format_NetCDF_SAFRAN_2D_Hydro_Portail-DRIAS_v2023.05.pdf) fourni dans le cadre du projet [Explore2](https://professionnels.ofb.fr/fr/node/1244). L'organisation du document a été modifié pour rendre son appréhension la plus aisée possible compte tenu de sa transformation en code R.

Il peut évidemment se trouver des incohérences donc il ne faut pas hésiter à me contacter (Louis Héraut). Pour des problèmes de compréhesion, il est aussi possible que Flore Tocquer soit plus à même de donner des éléments de réponse.

### La cohérence croisée
La mise en place d’un double niveau d’information (éléments du nom
du fichier et métadonnées) nécessite de contrôler la cohérence entre
les deux, mais est primordiale car contribue à la qualité du jeu de
données. Tout comme la standardisation des unités et des noms est
primordiale pour éviter les confusions et simplifier le traitement
des données par les utilisateurs.

Exemple de variables hydrologiques :

| Accronyme | Name    | Long name                | Units  | Cell methods |
| --------- | ------- | -------------------------| ------ | ------------ |
| DRAINC    | DRAINC  | Drainage for tile nature | mm     | time:sum     |
| EVAPC     | EVAPC   | Evapotranspiration       | mm     | time:sum     |
| RUNOFFC   | RUNOFFC | Runoff for tile nature   | mm     | time:sum     |
| SWE       | SWE     | Snow Water Equivalent    | mm     | time:mean ?  |
| SWI       | SWI     | Soil Water Index         | -      | time:mean ?  |

Voir aussi :
> [CF Standard Name Table](http://cfconventions.org/Data/cf-standard-names/current/build/cf-standard-name-table.html)</br>
> [IPCC Standard Output from Coupled Ocean-Atmosphere GCMs]( https://pcmdi.llnl.gov/mips/cmip3/variableList.html)</br>
> [CMIP5-CMOR-Tables]( https://wcrp-cmip.github.io/WGCM_Infrastructure_Panel//cmor_and_mip_tables.html)</br>
> [Data Reference Syntax (DRS) for bias-adjusted CORDEX](http://is-enes-data.github.io/CORDEX_adjust_drs.pdf)</br>


## Documentation du code

### Téléchargement
Le plus simple est de télécharger l'intégralité du repository github associé et de retrouver ces fichiers en suivant le chemin *help/SAFRAN_export/DRIAS_2D* afin de pouvoir les modifiers et les exécuter. Pour cela, il est possible de le faire en ligne de commande : 

``` 
git clone https://github.com/louis-heraut/Explore2_toolbox
```

Ou alors en le téléchargeant en *zip* avec le lien suivant [Explore2_toolbox-main.zip](https://github.com/louis-heraut/Explore2_toolbox/archive/refs/heads/main.zip).


### Structure
Ce dossier comprend deux types de scripts.

Les scripts dont le nom est précédé d'un chiffre sont utilisées pour la saisie d'information nécessaire à la constitution du NetCDF. Ils seront appelés scripts d'information.

Le dernier script nommé *DRIAS_export.R* est le script d'exécution qui gère la création en tant que tel du NetCDF.


### Principe de fonctionnement
#### Principe général
Comme précédemment expliqué, les variables R qui seront traitées par le script d'exécution ont toute le même format de nom `NCf$xxxx.yyyy` ou `NCf$xxxx.00.yyyy`. Ce dernier format correspond à une chaîne de caractère suivi d'un dollar, puis d'une chaîne de caractère alphanumérique, suivie d'un point ".", puis d'un nombre à deux chiffres, suivie d'un nouveau point "." et d'une nouvelle chaîne de caractère alphanumérique. L'idée étant que la première chaîne de caractère `NCf` correspond à l'environnement de stockage des variables propre à la construction du NetCDF en cours (... en gros c'est l'identifiant du fichier NetCDF) et c'est le dollar qui permet d'y accéder. La seconde chaîne de charactère `xxxx` correspond à la variable (ou dimension) dont il est question dans le NetCDF et la troisième et dernière chaîne de charactère `yyyy`, à l'attribut (ou paramètres associés) que l'on veut renseigner. Enfin, le nombre `00` est facultatif et est simplement présent pour gérer l'ordre d'apparence des attributs dans le NetCDF final.

Ainsi, rajouter une variable (ou dimension) et modifier ces attributs dans le NetCDF final se réduit à simplement modfier le nom d'une variable R et sa valeur associée.

#### Principe avancé
Pour une meilleur compréhesion du fonctionnement du code il est important de préciser quelques règles d'édition des variables R :

* Définir une dimension du NetCDF se fait par le biais de la création d'une variable R `NCf$x.name = "a"`. Ici `a` est le nom affichier dans le NetCDF de la dimension et `x` l'identification interne au code R de cette variable. Il est aussi nécessaire de définir une variable R `NCf$x.value = Value` où cette fois-ci `Value` est par exemple un vecteur de données qui défini les valeurs associées à la dimension identifiée dans R par `x` et nomé dans le NetCDF `a`. Par soucis de clareté, le mieux est souvent de définir une dimension nommée dans le NetCDF de la même manière que sont identification interne à R tel que `NCf$y.name = "y"`.

* Définir une variable du NetCDF se fait dans un premier tant de la même manière que pour une dimension par le biais d'une variable R `NCf$var.name = "b"`. Cependant, il n'est pas nécessaire cette fois-ci de lui associée une autre variable R de type `NCf$var.value = Value` mais une variable R de type `NCf$var.dimension = "x"`. Cette dernière permet donc de faire le lien entre la variable `b` et la dimension `x`. Si la dimension saisie est "", aucune dimension ne sera associée à cette variable mais cette déclaration dans R est quand même nécessaire.

* Pour une variable (et plus rarement pour une dimension), il est possible de donner la "précision" ou plutôt le type de donnée qui lui est associées. Pour cela, il faut définir une variable R `NCf$x.precision = "type"` où `"type"` est choisi parmi 'short', 'integer', 'float', 'double', 'char' et 'byte'. Attention... si une variable de caractère (donc de type 'char') est renseignée et prend en entrée une dimension caractérisant le longueur de cette chaîne de caractère, il est impératif que la dimension associée prenne comme valeur un vecteur de 1 à la longueur voulue de la chaîne de caractère et que cette même dimension présente un paramètre `NCf$dim.is_nchar_dimension = TRUE` qui précise ce comportement spécial. 

* Tout autre attribut d'une variable ou dimension du NetCDF est défni par une variable R `NCf$var.00.att = "attribut"` où de manière général `"attribut"` est une chaîne de charactère et de préférence un nombre à deux chiffres (ici `00`) est utilisé pour préciser sa position d'apparition dans le NetCDF.

* Un attribut du NetCDF défini comme `NCf$global.00.att = "attribut"` précise avec le marqueur `global` que cet attribut est global dans le NetCDF.

* Un attribut du NetCDF défini comme `NCf$title.00.att = "attribut"` précise avec le marqueur `title` que cet attribut permet la construction du titre du fichier NetCDF en accolant par des "_" l'ensemble des attributs fournis non vide.


### Scripts d'information
L'objectif est de vérifier, modifier, compléter selon vos besoins l'ensemble des variables R présentes dans les scripts d'information.

Le découpage de ces scripts est réalisé de sorte à simplifier la compréhension des éléments à saisir et leur documentation mais leur ordre de remplissage n'a pas d'importance. 


### Scripts d'exécution
Les modifications de ce script nommé *DRIAS_export.R* ne sont pas recommandées puisque c'est lui qui gère la création interne à R du NetCDF. Il est donc préférable de simplement exécuter entièrement ce script une fois que les scripts d'information sont remplis comme voulu.

Si vous voulez utiliser ce processu de création de NetCDF dans un autre contexte et de manière plus fine, vous pouvez trouver l'ensemble de la documentation du package R [NCf sur sa page github associée](https://github.com/louis-heraut/NCf).

Son exécution sans modifications préalable des scripts d'information produit un résultat générique avec les exemples pré-remplis.