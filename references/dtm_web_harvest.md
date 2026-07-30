# DTM : contrat de récolte web (reverse-engineering vérifié 2026-07-25)

> Pourquoi ce document : l'**API v3 de DTM n'est pas le bon canal**. Vérifié sur le Liban, elle était **21 rounds / 9 mois en retard** sur le site (round 88 / oct. 2025 contre round 109 / 23 juil. 2026), tout en déclarant l'opération `"Active"`. Les données courantes vivent sur le site, dans le catalogue de datasets et les rapports. Ce fichier fige les contrats de récolte.

## ⚠ CORRECTIONS DU 2026-07-25 (fin de journée) — lire avant tout le reste

Ce fichier a été écrit le matin avec des affirmations trop confiantes. Trois d'entre elles, qui portaient l'architecture, sont **fausses** ; mesurées à nouveau le même jour :

| Affirmation initiale | Réalité mesurée |
|---|---|
| « `/datasets`, `/datasets/{slug}`, `/reports` → HTTP 403 même avec un UA Chrome complet (WAF) » | **Faux.** Les 5 cibles testées répondent **200** depuis Python stdlib. |
| « La récolte HTML exige Playwright. Ne pas perdre de temps à chercher un jeu d'en-têtes qui passe : testé, ça ne passe pas. » | **Faux, et coûteux.** Les 403 venaient de **notre propre `_portal_get`** (UA Chrome tronqué + `Accept` minimal) et de mon propre martèlement du site. Un profil d'en-têtes **cohérent** passe. |
| « Le statut d'accès n'est PAS dans le listing » | **Faux.** `div.dtm-file-download` est dans le listing : ouvert/verrouillé se lit par ligne, sans ouvrir la fiche. |

**Le profil d'en-têtes décide** (même URL, même minute) : UA urllib par défaut → 200 · `humanitarian-secondary-data/1.0` (= `config.USER_AGENT`) → **403** · UA Chrome seul → **403** · UA Chrome + `Accept`/`Accept-Language`/`Sec-Fetch-*` → 200. Ce n'est pas « Python contre navigateur », c'est **navigateur à demi imité** qui est rejeté.

### Ce que le catalogue contient réellement (balayage des 124 pages, 2026-07-25)

- **2 375 datasets**, dont **2 250 avec lien de téléchargement direct (95 %)** et **125 verrouillés**.
- **93 des 125 verrouillés sont le Liban** (0 ouvert sur 93). Le verrou est une décision **de mission**, pas une politique DTM. Mon diagnostic précédent (« la donnée DTM se demande par mail ») était un constat libanais généralisé à tort.
- Les 32 verrouillés restants dessinent une règle : **microdonnée d'enquête ménages fermée** (Ukraine 14 : Mobility & Needs Assessment, Conditions of Return, Registered IDPs ; Cameroun MSNA ; South Sudan Population Count), **suivi de mobilité agrégé ouvert**.
- Volumes par pays : Yémen 415 (413 ouverts), Iraq 262, RDC 157, Nigeria 157, Mozambique 124, Burundi 123, Soudan 120 (120 ouverts), Libye 103, **Liban 93 (0 ouvert)**, Afghanistan 44 (44 ouverts).
- **42 lignes sans pays** = produits **régionaux** (« Europe — Mixed Migration Flows to Europe, Quarterly Overview »). Toute assertion pays doit tolérer un pays vide.

---

## Les deux canaux, et ce que chacun sert

Ce ne sont **pas** deux voies concurrentes vers la même donnée :

| Besoin | Canal | Pourquoi |
|---|---|---|
| **Le chiffre publié le plus récent** | ReliefWeb API v2 | Indexe les produits DTM sans retard (Liban round 109 présent), chiffres dans le `body`, PDF téléchargeables, tous pays, pas de clé |
| **Le fichier de données exploitable** | catalogue `dtm.iom.int/datasets` | Le seul endroit qui sert le `.xlsx` (Afghanistan Flow Monitoring : 12 242 lignes ventilées date × point de passage × sexe × âge) |
| Séries agrégées admin 0/1/2 par round | API v3 (clé) | Structuré, mais peut retarder de plusieurs mois (voir plus bas) |

**Mesure du partage** : sur un échantillon des **200 rapports IOM les plus récents** (sur 35 567), **167 pièces jointes, 100 % `application/pdf`, 0 tableur** ; et le titre exact du produit dataset (« Flow Monitoring Counting ») donne **0 résultat** sur ReliefWeb. Formulation prudente : sur cet échantillon récent, ReliefWeb ne porte pas les fichiers de données DTM. Ce n'est pas un balayage des 35 567.

## ReliefWeb : le contrat (voie du CHIFFRE)

**Vérifié 2026-07-25.** ReliefWeb indexe les produits DTM et son API v2 est pleinement programmatique.

```python
POST https://api.reliefweb.int/v2/reports?appname={keyring sds.reliefweb/appname}
{
  "filter": {"operator": "AND", "conditions": [
      {"field": "country.iso3", "value": "lbn"},
      {"field": "source.shortname", "value": "IOM"}]},
  "query":  {"value": "\"Mobility Snapshot\"", "fields": ["title"]},
  "fields": {"include": ["title", "body", "date.original", "url",
                         "file.url", "file.filename", "file.mimetype", "file.filesize"]},
  "sort": ["date.original:desc"], "limit": 12
}
```

Ce que ça donne (mesuré) :
- **204 rapports DTM pour le Liban**, dont les Mobility Snapshots rounds 105 à **109 (23 juil. 2026)** — donc **aucun retard**, contrairement à l'API DTM (round 88).
- **`body` porte souvent les chiffres directement** : round 108 → `412,701` IDP, `741,111` retours, **`380,461` hors sites, `32,240` en sites** (plus granulaire que la fiche dataset elle-même). Round 107 → `430,600` / `732,594`.
- **Les PDF sont téléchargeables** depuis `reliefweb.int/attachments/...` : testé, HTTP 200, 1 426 281 octets, magic `%PDF-`. Pas de WAF, pas de clé.
- ⚠ **Le `body` peut être VIDE sur le rapport le plus récent** (round 109 : 0 caractère alors que le PDF est là) → repli sur le PDF. Sur Windows, extraire avec **`fitz` (PyMuPDF)**, pas le Read tool (rule `windows-env`).
- Marche pour **tous les pays et tous les produits DTM** (vus au passage : Syrie Emergency Mobility Tracking, Sud-Soudan Intentions Survey, rapports régionaux « Escalation in the Middle East »).

**Conséquence d'architecture** : le client DTM n'a pas besoin de scraper le catalogue. Il interroge ReliefWeb (client déjà en place, réparé le 2026-07-25 : v2 + appname pré-approuvé) filtré sur `source.shortname=IOM`, et ne retombe sur les surfaces web ci-dessous que si ReliefWeb n'indexe pas le produit cherché.

**Généralisation** : le même mécanisme vaut pour **n'importe quelle agence** dont l'API est en retard ou fermée (`source.shortname` = IOM, UNHCR, OCHA, WFP, REACH...). ReliefWeb devient la voie de secours universelle pour la donnée publiée en rapport.

---

## Accès des surfaces web depuis Python (corrigé)

| Surface | Depuis Python stdlib | Condition |
|---|---|---|
| `dtm.iom.int/{pays}`, `/datasets`, `/datasets/{slug}`, `/reports` | ✅ **200** | jeu d'en-têtes **cohérent** (voir `PORTAL_HEADERS`) ou urllib nu ; un UA navigateur **à demi imité** → 403 |
| `/dtm_download_track/{file_id}?file=1&type=node&id={nid}` (fichier) | ✅ **200**, sert le `.xlsx` | idem |
| `POST /views/ajax` (catalogue par pays) | ✅ | — |
| API v3 `dtmapi.iom.int/v3/*` | ✅ | clé `Ocp-Apim-Subscription-Key` |
| ArcGIS `gisportal.iom.int/arcgis/rest/...` | ✅ | sans clé |

→ **Playwright est inutile ici.** Garder un délai de 0,5 s entre requêtes : le balayage de 124 pages est la cause la plus probable des 403 transitoires que j'avais pris pour un WAF permanent.

## Filtrage côté serveur : la facette pays, jamais `search=`

- **Facette pays** : `?f%5B0%5D=dataset_country:{id}` — filtre réellement, compose avec `&page=N`, et un **id invalide renvoie 0 ligne** (fail-closed, pas de corpus mondial silencieux).
- Les 56 paires `(id, pays)` sont **dans le HTML de `/datasets`** (bloc de facettes) → moissonnables, jamais à coder en dur. Sudan 82 · Liban 9442 · Yémen 85 · RDC 62 · Ukraine 1088 · Afghanistan 56.
- ⚠ **`search=` est un plein texte, PAS un filtre pays** : `search=lebanon` renvoie **11 lignes Syrie sur 20**. S'en servir comme repli de scope pays fabriquerait exactement le genre de réponse fausse-mais-plausible que la revue de juillet a listé. C'est un **refus** du skill, pas un commentaire de code.
- ⚠ Le slug pays de chaque ligne n'est **pas** le libellé slugifié : RDC → `democratic-republic-congo` (et non `democratic-republic-of-the-congo`). Une assertion sur un libellé slugifié ferait un faux positif sur les 157 datasets de la RDC. La table `DTM_CATALOGUE_COUNTRIES` gèle le slug observé.
- Tri : `sort_by=field_dataset_published_date&sort_order=DESC`. Le formulaire n'expose que `search`, `sort_by`, `sort_order`.

## Surface 1 — Catalogue global `/datasets`

- **Listing rendu côté serveur**, aucune vue AJAX. Pagination `?page=N` (N commence à 0), ~16-19 lignes/page. Formulaire GET exposé : `search`, `sort_by`, `sort_order`.
- **Tous pays confondus** (vérifié : Liban, Afghanistan, Yémen, Somalie, Libye sur la page 0).
- **Structure réelle d'une ligne** (HTML brut, pas le DOM post-JS) : le marqueur de découpe est `<div class="report-item1">`, pas `div.row` (qui est aussi utilisé ailleurs dans la page).

```html
<div class="report-item1"><div class="row"><div class="col-lg-12 col-md-12">
  <h5><a href="" class="title"><a href="/datasets/{slug}" hreflang="en">TITRE</a></a></h5>
  <div class="content">DESCRIPTION tronquée, entités HTML doublement échappées…</div>
  <!-- PRÉSENT seulement si le dataset est OUVERT : -->
  <div class="dtm-file-download"><i class="fa-regular fa-circle-down"></i>
    <a href="https://dtm.iom.int/dtm_download_track/{file_id}?file=1&amp;type=node&amp;id={nid}"
       download>Download Dataset</a></div>
  <div class="date">
    <span>Jul 23 2026</span> · <span><a href="/regions/{region}">RÉGION</a></span>
    · <span><a href="/{pays}">PAYS</a></span>
    · <span>Mobility Tracking, Baseline Assessment</span>   <!-- activité(s) -->
    · <span>Infosheet</span>                                 <!-- format -->
  </div>
</div></div></div>
```

- Champs récoltables sans ouvrir la fiche : `slug`, `title`, `description`, `date`, `region`, `country` (slug), `activities`, `format`, **et le lien de téléchargement**.
- **Le statut d'accès EST dans le listing** : `div.dtm-file-download` présent = ouvert, absent = verrouillé. Une requête par page suffit pour savoir ce qui est téléchargeable ; inutile d'ouvrir les fiches.
- ⚠ **Ne pas mettre en cache les URL de téléchargement** : `file_id` et `nid` sont propres à la publication ; un nouveau round en fabrique de nouveaux. Re-résoudre depuis le catalogue au moment de télécharger.
- ⚠ Un fichier servi en HTTP 200 peut être une page d'erreur Drupal. Vérifier les octets de tête (`PK` pour xlsx, `%PDF-`) avant d'écrire, sinon on stocke un `.xlsx` silencieusement corrompu.

## Surface 2 — Fiche dataset `/datasets/{slug}`

- **Détection du verrou** : la page contient `REQUEST ACCESS` et la phrase *« A more detailed version of this dataset is available, to get access kindly click on the 'Request Access' button »*. Aucun lien `.xlsx`/`.csv`/`dtm_download`, aucun formulaire de téléchargement.
- ⚠ **Le verrou ne couvre QUE la version détaillée (admin2/admin3).** Les **agrégats nationaux sont publiés en clair sur la fiche**. C'est la clé du contournement légitime : on n'a pas besoin du fichier pour les chiffres de tête.
- **Deux formats de texte** (gérer les deux) :
  1. **Narratif** (tous les rounds) : `recorded ([\d,]+) internally displaced persons` et `([\d,]+) IDPs have begun returning`
  2. **Puces** (rounds récents seulement) : `([\d,]+) IDPs outside collective sites`, `([\d,]+) IDPs across ([\d,]+) collective sites`, `([\d,]+) returned IDPs reported`, avec les variations `(-9% compared to 15 July)`
  3. Certains rounds anciens n'ont **ni l'un ni l'autre** (round 104 vérifié) → renvoyer `None`, jamais 0.
- Métadonnées structurées : `As of ([0-9]{1,2} \w+ 20\d\d)`, `Period Covered {date} - {date}`, `Population Groups`, `Unit of Analysis` (Admin Area 2/3), `Type of Survey`, `Keywords`, `Geographical Scope`, contact e-mail.
- **Bonus** : la page expose une variable JS `dataset_dataset_admin_pcodes = ["LB5","LB8",...]` = les pcodes admin1 couverts. Donnée structurée gratuite.

## Surface 3 — Rapports `/reports` et `/reports?fresh=true`

- Même structure de listing que `/datasets` (`div.row`, `?page=N`, ~19 lignes/page), liens `/reports/{slug}`.
- `?fresh=true` trie sur les plus récents (vérifié : profils CoRI Syrie, Intentions Survey Sud-Soudan en tête au 2026-07-25).
- **C'est là que se trouve la donnée qu'un dataset verrouillé refuse** : les rapports publient les chiffres et souvent les ventilations. À exploiter en repli systématique.

## Surface 4 — Catalogue par pays via `POST /views/ajax` (le seul chemin Python)

```
POST https://dtm.iom.int/views/ajax
Content-Type: application/x-www-form-urlencoded
X-Requested-With: XMLHttpRequest
view_name=dataset & view_display_id=country_datasets & view_args={nid}
& view_path=/node/{nid} & view_base_path=datasets-1 & pager_element=0 & page=0
```
- Le `{nid}` du pays se lit dans `drupalSettings.nid` de la page pays (Liban = **9442**).
- Réponse = tableau de commandes Drupal AJAX ; le HTML est dans les entrées portant une clé `data` (aplatir récursivement, `data` peut être une liste).
- Autres vues du même endpoint : `view_name=maps`/`country_maps`, `view_name=reports`/`search_report_country`.

## Surface 5 — `dtm_cdw/get` (à ne pas privilégier)

POST form-urlencoded, paramètre **`requestType`** (et non `method`). 15 types découverts : `GetCountryActivityRounds`, `CountryAllActivities`, `CountryActivityAllAggregation`, `CountryActivityMonthly`, `CrisisActivities`, `GlobalBaselineActivity`, `GlobalSiteAssesmentActivity`, `get_country_latest_report`, `MobilityImpactGlobalFigures{,1,PoEs}`, `MobilityImpactCountry{Figures,PoEs,Restrictions}`, `MobilityImpactRestrictionContext`. Tous ceux testés renvoient `200` avec `[]` ou vide, **y compris depuis la page avec session**. Les chiffres de tête de la page pays sont de toute façon rendus côté serveur (voir ci-dessous).

## Surface 6 — Chiffres de tête d'une page pays

Rendus côté serveur, sélecteurs stables :

| Sélecteur | Contenu (Liban, 2026-07-25) |
|---|---|
| `#dtm-total-figure` | `375,090` (IDP) |
| `.dtm-total-figure-date` | `Jul 2026` |
| `#dtm-returnees-figure` | `753,024` |
| `#dtm-migrants-figure` | `-` (absent pour le Liban) |

## Forme des fichiers téléchargés (vérifiée, 2 fichiers réels)

Deux gabarits distincts, et le mauvais réflexe de lecture donne des colonnes vides sans erreur :

**a) Masterlist multi-feuilles** — `2026_24_05_DTM_SDN_IDPs_Returnees_Snapshot_006_Public_v1.xlsx` (240 988 o) :
- 5 feuilles : `Read Me`, `IDPs Masterlist (ADMIN1)`, `IDPs Masterlist (ADMIN2)` (191 × 26), `Returnees Masterlist (ADMIN1)`, `Returnees Masterlist (ADMIN2)`.
- ⚠ **`sheet=0` renvoie le « Read Me »** (un disclaimer). Sélectionner la feuille **par nom**.
- ⚠ **En-tête sur plusieurs lignes** : r1 = bandeau de titre, r3 = groupes fusionnés, **r4 = les vrais noms de colonnes**, **r5 = les tags HXL**, **données à partir de r6**. `header=0` donne le bandeau.
- Colonnes : `STATE OF DISPLACEMENT`, `STATE CODE`, `LOCALITY OF DISPLACEMENT`, `LOCALITY_ CODE`, `IDPs`, `HHs`, puis une **matrice état d'origine × état de déplacement** (18 colonnes).
- ✅ **Tags HXL présents** (r5) : `#adm1+name`, `#adm1+pcode`, `#adm2+name`, `#adm2+pcode`, `#affected+idps+ind`, `#affected+idps+hh` → clé de jointure directe pour la couche de standardisation, et pcodes OCHA (`SD15034`) déjà là.

**b) Série longue mono-feuille** — `Afghanistan Flow Monitoring Counting Data 10Jan2024To18July2026.xlsx` (1 377 003 o) :
- 1 feuille, **12 242 lignes × 10 colonnes**, en-tête sur r1, données dès r2.
- `ReportingDate`, `Direction` (Inflow/Outflow), `FlowMonitoringPoint` (`Islam Qala - Taybad`, `Zaranj – Milak`, `Torkham – Bab-i-Peshawar`), puis `Male Under 5`, `Male Above 5`, `Total Male`, `Female Under 5`, `Female Above 5`, `Total Female`, `Total Headcount`.
- ⚠ **Valeurs décimales** (`3633.942583732058`) : ce sont des **estimations pondérées**, pas des comptages entiers. Ne pas les présenter comme un dénombrement exact ; arrondir à l'affichage et le dire.
- ⚠ Ventilation **sexe × âge (seuil 5 ans) uniquement**. **Aucune ventilation handicap** dans les produits DTM vérifiés.

## Règle de skill

Quand l'utilisateur demande un chiffre ou un fichier DTM :

1. **Dire d'abord ce qui existe** : `catalogue_summary(iso3)` → nombre de datasets, part téléchargeable, date du plus récent, activités couvertes. Un pays hors des 56 → `UnmappedCountry`, on le dit, on ne cherche pas au hasard.
2. **Lire la fraîcheur avant le chiffre** : l'API v3 peut retarder de plusieurs mois (Liban : round 88 contre 109). Comparer à la date du catalogue et **ne jamais servir le chiffre de l'API comme courant** sans cette comparaison.
3. **Le fichier d'abord si la question est analytique** ; le chiffre publié (ReliefWeb / fiche) si la question est « combien, aujourd'hui ». 95 % des datasets sont téléchargeables : ne pas annoncer un verrou avant de l'avoir constaté sur la ligne.
4. **Si le dataset est verrouillé** → le dire explicitement, nommer le contact de la mission, puis chercher le chiffre dans **le dernier rapport publié** (ReliefWeb ou `/reports?fresh=true`), qui expose généralement la donnée.
5. **Ne jamais franchir le verrou**, et **ne jamais retomber sur `search=<pays>`** comme scope pays. Reverse-engineer un dashboard public est légitime ; contourner une barrière d'accès ne l'est pas, et un plein-texte n'est pas un filtre.
6. **Toujours estampiller** : round, `as_of`, période couverte, type d'évaluation, surface d'origine (API / ArcGIS / catalogue / rapport), et pour un fichier : feuille lue et ligne d'en-tête.
7. **Ne jamais inventer un chiffre absent.** Si le fichier ne porte pas la ventilation demandée (handicap, par exemple), le dire et proposer la source qui l'a.

## Résultat déjà obtenu (preuve que le contrat fonctionne)

`LBN_data/raw/dtm_lebanon_mobility_rounds.csv` — série hebdomadaire récoltée sur les fiches publiques :

| Round | Au | IDP | Retours |
|---|---|---|---|
| 109 | 2026-07-22 | 375 090 | 753 024 |
| 108 | 2026-07-15 | 412 701 | 741 111 |
| 107 | 2026-07-08 | 430 600 | 732 594 |
| 106 | 2026-07-01 | 499 784 | 646 107 |
| 105 | 2026-06-24 | 704 445 | 523 249 |

Round 109 en plus : 345 361 hors sites, 29 729 dans 273 sites collectifs. Cohérence interne : le caseload total (IDP + retours) reste ~1,13-1,23 M pendant que les IDP chutent de 47 % et les retours montent de 44 %.
