************ S2papers

CREATE fulltext index on LEMAS (after lemas extraction)

Falta rellenar los campos  ESP_contri y AIselection


**************** S2authors

CREATE index on S2authorID (after table creation)

Faltan por rellenar los siguientes campos

    orcidID VARCHAR(20),
    orcidGivenName VARCHAR(40),
    orcidFamilyName VARCHAR(100),
    scopusID BIGINT(20),
    name VARCHAR(256),
    influentialCitationCount SMALLINT UNSIGNED,
    ESP_affiliation TINYINT(1)

*************** paperAuthor

PRIMARY KEY (paperID, authorID),

*************** paperEntity
PRIMARY KEY (paperID, entityID),

*************** citations
PRIMARY KEY (paperID1, paperID2),

isInfluential 
Rellenar también el campo con el tipo de influencia


================================
    DB._conn.query('SET GLOBAL connect_timeout=60000')
    DB._conn.query('SET GLOBAL wait_timeout=60000')
    DB._conn.query('SET GLOBAL interactive_timeout=60000')
