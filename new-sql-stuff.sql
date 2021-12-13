--DROP TABLE IF EXISTS stream_hype_datapoints;
DROP VIEW IF EXISTS stream_hype_datapoints;
--CREATE VIEW stream_hype_datapoints AS
--	SELECT
--		dp.stream_id,
--		dp.ts,
--		db.db,
--		chat.messages_per_second
--	FROM
--		(
--			SELECT DISTINCT stream_id,ts FROM (
--				SELECT stream_id,ts FROM stream_decibels
--				UNION ALL
--				SELECT stream_id,ts FROM stream_chatspeed_datapoints
--			) ORDER BY ts ASC
--		) AS dp
--	LEFT JOIN stream_decibels AS db
--		ON db.stream_id = dp.stream_id AND db.ts = dp.ts
--	LEFT JOIN stream_chatspeed_datapoints AS chat
--		ON chat.stream_id = dp.stream_id AND chat.ts = dp.ts;

--DROP VIEW IF EXISTS stream_average_hype;
DROP TABLE IF EXISTS stream_average_hype;
--CREATE VIEW stream_average_hype AS
--	SELECT
--		stream_id,
--		AVG(hype) AS average
--	FROM stream_hype_datapoints
--	GROUP BY stream_id;

DROP VIEW IF EXISTS stream_loudness_datapoints;
--CREATE VIEW stream_loudness_datapoints AS
--	SELECT
--		db.stream_id AS stream_id,
--		db.ts AS ts,
--		db.db AS db,
--		avg_dbs.db AS avg_db,
--		1.0 / (db.db / avg_dbs.db) AS loudness
--	FROM stream_decibels AS db
--	JOIN (
--		SELECT
--			db.stream_id,
--			s.ts,
--			s.duration,
--			AVG(db.db) AS db
--		FROM stream_decibels AS db
--		JOIN streams AS s
--			ON s.id = db.stream_id
--		WHERE s.ts <= db.ts
--		AND db.ts <= (s.ts + s.duration)
--		AND db.db <> -1e1000
--		GROUP BY db.stream_id
--	) AS avg_dbs
--		ON avg_dbs.stream_id = db.stream_id
--	WHERE avg_dbs.ts <= db.ts
--	AND db.ts <= (avg_dbs.ts + avg_dbs.duration)
--	AND db.db <> -1e1000;

--DROP TABLE IF EXISTS stream_loudness;
CREATE TABLE IF NOT EXISTS stream_loudness (
	stream_id INTEGER NOT NULL,
	ts INTEGER NOT NULL,
	momentary REAL,
	short_term REAL,
	integrated REAL,
	lra REAL,

	PRIMARY KEY (stream_id, ts),

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);

--DROP TABLE IF EXISTS stream_chatspeed_datapoints;
CREATE TABLE IF NOT EXISTS stream_chatspeed_datapoints (
	stream_id INTEGER NOT NULL,
	ts INTEGER NOT NULL,
	messages INTEGER NOT NULL,

	PRIMARY KEY (stream_id, ts),

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);
--DROP TABLE IF EXISTS stream_decibels;
CREATE TABLE IF NOT EXISTS stream_decibels (
	stream_id INTEGER NOT NULL,
	ts INTEGER NOT NULL,
	db REAL NOT NULL,

	PRIMARY KEY (stream_id, ts),

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);

DROP VIEW streams_view;
CREATE VIEW streams_view AS
	SELECT
		s.id,
		titles.title AS title,
		titles.type AS title_type,
		s.filename,
		s.filesize,
		s.ts,
		s.duration,
		s.preview_count,
		s.thumbnail_count,
		s.has_chat,
		r.positives AS rating_positives,
		r.count AS rating_count,
		0.0 AS hype_average,
		datapoints_json AS datapoints,
		jumpcuts_json AS jumpcuts,
		p.json AS persons,
		g.json AS games

	FROM streams AS s

	JOIN titles
		ON titles.stream_id = s.id

	LEFT JOIN person_participations_json AS p
		ON p.stream_id = s.id

	LEFT JOIN game_features_json AS g
		ON g.stream_id = s.id

	LEFT JOIN stream_ratings_total AS r
		ON r.stream_id = s.id

	ORDER BY s.ts DESC;
