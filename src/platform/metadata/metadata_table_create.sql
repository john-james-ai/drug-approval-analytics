CREATE TABLE public.parameter (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    value real NOT NULL,
    type varchar(12) NOT NULL,
    description varchar(256) NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);


CREATE TABLE public.score (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    value real NOT NULL,
    type varchar(12) NOT NULL,
    metric varchar(24) NOT NULL,
    description varchar(256) NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);


CREATE TABLE public.datasource (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    type varchar(12) NOT NULL,
    description varchar(256) NOT NULL,
    webpage varchar(256) NOT NULL,
    uri varchar(256) NOT NULL,
    uri_type varchar(24) NOT NULL,
    uris VARCHAR[] NOT NULL,
    lifecycle integer NOT NULL,
    media_type varchar(24) NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);


CREATE TABLE public.dataset (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    type varchar(12) NOT NULL,
    description varchar(256) NOT NULL,
    uri varchar(256) NOT NULL,
    media_type varchar(24) NOT NULL,
    derived_from integer NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON public.dataset
    (derived_from);


CREATE TABLE public.featuretransform (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    type varchar(12) NOT NULL,
    description varchar(256) NOT NULL,
    inputs INTEGER[] NOT NULL,
    outputs INTEGER[] NOT NULL,
    executed timestamp without time zone NOT NULL,
    feature_id integer NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON public.featuretransform
    (feature_id);


CREATE TABLE public.feature (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    type varchar(12) NOT NULL,
    description varchar(256) NOT NULL,
    table_id integer NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON public.feature
    (table_id);


CREATE TABLE public.trainingevent (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    type varchar(12) NOT NULL,
    description varchar(256) NOT NULL,
    model_id integer NOT NULL,
    input_dataset_id integer NOT NULL,
    parameters INTEGER[] NOT NULL,
    scores INTEGER[] NOT NULL,
    predictions_dataset_id integer NOT NULL,
    feature_transforms INTEGER[] NOT NULL,
    started timestamp without time zone NOT NULL,
    ended timestamp with time zone NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON public.trainingevent
    (model_id);
CREATE INDEX ON public.trainingevent
    (input_dataset_id);
CREATE INDEX ON public.trainingevent
    (predictions_dataset_id);


CREATE TABLE public.prediction (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    type varchar(12) NOT NULL,
    description varchar(256) NOT NULL,
    training_event_id integer NOT NULL,
    uri varchar(256) NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);


CREATE TABLE public.model (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    description varchar(256) NOT NULL,
    algorithm varchar(32) NOT NULL,
    training_events INTEGER[] NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);


CREATE TABLE public.datasourceevent (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    type varchar(12) NOT NULL,
    description varchar(256) NOT NULL,
    datasource_id integer NOT NULL,
    started timestamp without time zone NOT NULL,
    ended timestamp with time zone NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON public.datasourceevent
    (datasource_id);


CREATE TABLE public.statistic (
    id integer NOT NULL,
    name varchar(24) NOT NULL,
    type varchar(12) NOT NULL,
    dataset_id integer NOT NULL,
    columns VARCHAR[] NOT NULL,
    computed timestamp without time zone NOT NULL,
    created timestamp without time zone NOT NULL,
    updated timestamp with time zone NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON public.statistic
    (dataset_id);

