# 🌬️ Supervision d'un parc éolien — Pipeline data end to end Microsoft Fabric
**Finalité du projet :** Réduire les temps d'arrêt non planifiés et maximiser la production énergétique en transformant les signaux IoT bruts en alertes proactives et tableaux de bord décisionnels.

## 📖 Présentation du projet
Pour produire un maximum d'électricité, un parc éolien doit tourner sans interruption. 
Sur cette installation, trois éoliennes envoient des mesures toutes les dix minutes. 
Le problème : sans un système automatique pour rassembler et analyser ces chiffres en continu, les pannes passent inaperçues, les interventions prennent du retard et le parc perd de l'énergie.

Ce projet met en place une plateforme unifiée sous **Microsoft Fabric** pour automatiser le traitement des données brutes et déclencher des interventions ciblées avant l'arrêt d'urgence.

## 🎯 Objectifs
Concevoir, déployer et orchestrer une plateforme de données unifiée et évolutive afin de :
* **Automatiser l'ingestion** et le traitement des séries temporelles issues des capteurs IoT.
* **Structurer les données** selon les standards d'une architecture médaillon (Bronze, Silver, Gold).
* **Fournir des outils d'aide à la décision** via des tableaux de bord interactifs et un système d'alerte automatisé en cas de dérive de performance.

## 🛠️ Stack technique & compétences acquises
Ce projet m'a permis d'acquérir une expérience concrète sur:
* **Stockage & Infrastructure** : OneLake, Lakehouse (format Delta Parquet)
* **Ingestion & Transformation** : Dataflow Gen2, Data Pipelines, PySpark, Spark SQL
* **Modélisation & Mesures** : Modèle sémantique en étoile, DAX
* **Restitution & Alertes** : Power BI, Data Activator (Teams, Outlook)

ℹ️ Note technique : 
Le mécanisme d'alerte configuré avec Data Activator a été validé et testé sur l'environnement Fabric, 
mais s'appuie sur des fonctionnalités en préversion (preview) non versionnées dans ce dépôt GitHub.
 
## 🚀 Pistes d'amélioration
* **Industrialisation** : Déploiement multi-environnements (Dev / Test / Prod) via Fabric Git Integration.
* **Fiabilisation** : Contrôles automatisés de qualité de données (Data Quality checks) en amont de la couche Silver.
* **Traçabilité**: Intégration du lignage de données sous Microsoft Purview.
* **Data Science**: Modèle prédictif de détection précoce d'anomalies sur les séries temporelles.
