DROP TABLE IF EXISTS stream_hype_datapoints;
CREATE TABLE stream_decibels (
	stream_id INTEGER NOT NULL,
	ts INTEGER NOT NULL,
	db REAL NOT NULL,

	PRIMARY KEY (stream_id, ts),

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);
CREATE TABLE stream_chatspeed_datapoints (
	stream_id INTEGER NOT NULL,
	ts INTEGER NOT NULL,
	messages_per_second REAL NOT NULL,

	PRIMARY KEY (stream_id, ts),

	FOREIGN KEY (stream_id) REFERENCES streams(id) ON DELETE CASCADE
);

DROP VIEW IF EXISTS stream_average_hype;
CREATE VIEW stream_average_hype AS
	SELECT
		stream_id,
		AVG(hype) AS average
	FROM stream_hype_datapoints
	GROUP BY stream_id;

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
		hype.average AS hype_average,
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

	LEFT JOIN stream_average_hype AS hype
		ON hype.stream_id = s.id

	ORDER BY s.ts DESC;
