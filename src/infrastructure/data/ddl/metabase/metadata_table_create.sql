CREATE TABLE metabase.parameter (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
value real NOT NULL,
type varchar(12) NOT NULL,
description varchar(256) NOT NULL,
created timestamp with time zone NOT NULL,
updated timestamp with time zone NOT NULL,
updated_by varchar(24) NOT NULL,
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);


CREATE TABLE metabase.score (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
value real NOT NULL,
type varchar(12) NOT NULL,
metric varchar(24) NOT NULL,
description varchar(256) NOT NULL,
created timestamp with time zone NOT NULL,
updated timestamp with time zone NOT NULL,
updated_by varchar(24) NOT NULL,
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);


CREATE TABLE metabase.datasource (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
type varchar(12) NOT NULL,
version integer NOT NULL,
title varchar(128),
description varchar(256),
webpage varchar(256) NOT NULL,
link varchar(256) NOT NULL,
link_type varchar(24) NOT NULL,
uris TEXT[],
media_type varchar(32),
frequency integer NOT NULL,
lifecycle integer NOT NULL,
coverage varchar(64),
creator varchar(256) NOT NULL,
maintainer varchar(256),
has_changed boolean NOT NULL,
source_updated timestamp with time zone,
created timestamp with time zone NOT NULL,
created_by varchar(32) NOT NULL,
updated timestamp with time zone,
updated_by varchar(24),
extracted timestamp with time zone,
last_extract date,
next_extract date,
PRIMARY KEY (id)
);


COMMENT ON COLUMN metabase.datasource.next_extract
IS 'Next extract = DATE(today) + lifecycle computed by the extractor ONLY.';

CREATE TABLE metabase.dataset (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
type varchar(12) NOT NULL,
version integer NOT NULL,
description varchar(256),
uri varchar(256) NOT NULL,
derived_from char(36),
created timestamp with time zone NOT NULL,
updated timestamp with time zone,
updated_by varchar(24),
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);


CREATE TABLE metabase.featuretransform (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
type varchar(12) NOT NULL,
description varchar(256) NOT NULL,
transformer varchar(32) NOT NULL,
inputs INTEGER[] NOT NULL,
outputs INTEGER[] NOT NULL,
executed timestamp with time zone NOT NULL,
feature_id char(36) NOT NULL,
created timestamp with time zone NOT NULL,
updated timestamp with time zone NOT NULL,
updated_by varchar(24) NOT NULL,
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);

CREATE INDEX ON metabase.featuretransform
(feature_id);


CREATE TABLE metabase.feature (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
type varchar(12) NOT NULL,
description varchar(256),
required Boolean NOT NULL,
datatype varchar(32) NOT NULL,
created timestamp with time zone NOT NULL,
domain VARCHAR[] NOT NULL,
updated timestamp with time zone NOT NULL,
updated_by varchar(24) NOT NULL,
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);


CREATE TABLE metabase.trainingevent (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
type varchar(12) NOT NULL,
description varchar(256) NOT NULL,
model_id char(36) NOT NULL,
input_dataset_id char(36) NOT NULL,
parameter_ids CHAR[] NOT NULL,
score_ids CHAR[] NOT NULL,
predictions_dataset_id char(36) NOT NULL,
feature_transform_ids TEXT[] NOT NULL,
started timestamp with time zone NOT NULL,
ended timestamp with time zone NOT NULL,
created timestamp with time zone NOT NULL,
updated timestamp with time zone NOT NULL,
updated_by varchar(24) NOT NULL,
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);

CREATE INDEX ON metabase.trainingevent
(model_id);
CREATE INDEX ON metabase.trainingevent
(input_dataset_id);
CREATE INDEX ON metabase.trainingevent
(predictions_dataset_id);


CREATE TABLE metabase.prediction (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
type varchar(12) NOT NULL,
description varchar(256) NOT NULL,
training_event_id char(36) NOT NULL,
uri varchar(256) NOT NULL,
created timestamp without time zone NOT NULL,
updated timestamp with time zone NOT NULL,
updated_by varchar(24) NOT NULL,
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);

CREATE INDEX ON metabase.prediction
(training_event_id);


CREATE TABLE metabase.model (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
description varchar(256) NOT NULL,
algorithm varchar(32) NOT NULL,
created timestamp with time zone NOT NULL,
updated timestamp with time zone NOT NULL,
framework varchar(32) NOT NULL,
type varchar(32) NOT NULL,
updated_by varchar(24) NOT NULL,
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);


CREATE TABLE metabase.datasourceevent (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
datasource_id char(36) NOT NULL,
started timestamp with time zone NOT NULL,
ended timestamp with time zone NOT NULL,
return_code integer NOT NULL,
return_value varchar(256) NOT NULL,
created timestamp with time zone NOT NULL,
created_by varchar(24) NOT NULL,
PRIMARY KEY (id)
);

CREATE INDEX ON metabase.datasourceevent
(datasource_id);


CREATE TABLE metabase.countstats (
id char(36) NOT NULL,
name varchar(24) NOT NULL,
type varchar(12) NOT NULL,
dataset_id char(36) NOT NULL,
feature_id char(36) NOT NULL,
created timestamp with time zone NOT NULL,
updated timestamp with time zone NOT NULL,
count INTEGER[] NOT NULL,
rank1 varchar(32) NOT NULL,
pct real NOT NULL,
updated_by varchar(24) NOT NULL,
created_by varchar(32) NOT NULL,
PRIMARY KEY (id)
);

CREATE INDEX ON metabase.countstats
(dataset_id);
CREATE INDEX ON metabase.countstats
(feature_id);


ALTER TABLE metabase.featuretransform ADD CONSTRAINT FK_featuretransform__feature_id FOREIGN KEY (feature_id) REFERENCES metabase.feature(id);
ALTER TABLE metabase.trainingevent ADD CONSTRAINT FK_trainingevent__model_id FOREIGN KEY (model_id) REFERENCES metabase.model(id);
ALTER TABLE metabase.trainingevent ADD CONSTRAINT FK_trainingevent__input_dataset_id FOREIGN KEY (input_dataset_id) REFERENCES metabase.dataset(id);
ALTER TABLE metabase.trainingevent ADD CONSTRAINT FK_trainingevent__predictions_dataset_id FOREIGN KEY (predictions_dataset_id) REFERENCES metabase.prediction(id);
ALTER TABLE metabase.prediction ADD CONSTRAINT FK_prediction__training_event_id FOREIGN KEY (training_event_id) REFERENCES metabase.trainingevent(id);
ALTER TABLE metabase.datasourceevent ADD CONSTRAINT FK_datasourceevent__datasource_id FOREIGN KEY (datasource_id) REFERENCES metabase.datasource(id);
ALTER TABLE metabase.countstats ADD CONSTRAINT FK_countstats__dataset_id FOREIGN KEY (dataset_id) REFERENCES metabase.dataset(id);
ALTER TABLE metabase.countstats ADD CONSTRAINT FK_countstats__feature_id FOREIGN KEY (feature_id) REFERENCES metabase.feature(id);
