CREATE TABLE person_participations_new (
	stream_id INTEGER NOT NULL,
	person_id INTEGER NOT NULL,

	PRIMARY KEY (stream_id, person_id),

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
	FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
);
INSERT INTO person_participations_new SELECT * from person_participations;
DROP TABLE person_participations;
ALTER TABLE person_participations_new RENAME TO person_participations;
