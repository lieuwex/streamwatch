CREATE TABLE game_features_new (
	stream_id INTEGER NOT NULL,
	game_id INTEGER NOT NULL,
	start_time REAL NOT NULL, -- in seconds, can't be greater than stream.duration

	PRIMARY KEY (stream_id, game_id),

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
	FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);
INSERT INTO game_features_new SELECT * from game_features;
DROP TABLE game_features;
ALTER TABLE game_features_new RENAME TO game_features;
