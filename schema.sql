PRAGMA foreign_keys = 1;

CREATE TABLE streams (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	filename TEXT NOT NULL,
	filesize INTEGER NOT NULL, -- in bytes
	ts INTEGER NOT NULL,
	duration REAL NOT NULL -- in seconds
);

CREATE TABLE stream_previews (
	stream_id INTEGER NOT NULL PRIMARY KEY,

	--- tbh dit moeten we zelf regelen, want we moeten de file deleten.
	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);

CREATE TABLE stream_thumbnails (
	stream_id INTEGER NOT NULL,
	thumb_index INTEGER NOT NULL,

	PRIMARY KEY (stream_id, thumb_index),

	--- tbh dit moeten we zelf regelen, want we moeten de file deleten.
	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);

CREATE TABLE clips (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	stream_id INTEGER NOT NULL,
	start_time INTEGER NOT NULL, -- in seconds
	end_time INTEGER NOT NULL, -- in seconds
	title TEXT, -- REVIEW: not null?

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);

CREATE TABLE clip_views (
	clip_id INTEGER NOT NULL,
	user_id INTEGER,
	real_time INTEGER NOT NULL,

	FOREIGN KEY (clip_id) REFERENCES clips(id),
	FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE TABLE persons (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL
);

CREATE TABLE person_participations (
	stream_id INTEGER NOT NULL,
	person_id INTEGER NOT NULL,

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
	FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
);

CREATE TABLE games (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	name TEXT NOT NULL,
	platform TEXT
);

CREATE TABLE users (
	id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
	username TEXT NOT NULL UNIQUE
);
CREATE TABLE stream_progress (
	user_id INTEGER NOT NULL,
	stream_id INTEGER NOT NULL,
	time REAL NOT NULL, -- in seconds, can't be greater than stream.duration

	PRIMARY KEY (user_id, stream_id),

	FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
	FOREIGN KEY (stream_id) REFERENCES stream(id) ON DELETE CASCADE
);

CREATE TABLE game_features (
	stream_id INTEGER NOT NULL,
	game_id INTEGER NOT NULL,
	start_time REAL NOT NULL, -- in seconds, can't be greater than stream.duration

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE,
	FOREIGN KEY (game_id) REFERENCES games(id) ON DELETE CASCADE
);

CREATE TABLE meta (
	key TEXT NOT NULL PRIMARY KEY,
	value TEXT
);
INSERT INTO meta(key, value) values("schema_version", "1");
