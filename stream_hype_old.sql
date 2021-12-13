-- If only sqlite supported FULL OUTER JOIN...
CREATE VIEW stream_hype_datapoints AS
	SELECT
		dp.stream_id,
		dp.ts,
		db.db,
		chat.messages_per_second
	FROM (
			SELECT DISTINCT stream_id,ts
			FROM (
				SELECT stream_id,ts FROM stream_decibels
				UNION ALL
				SELECT stream_id,ts FROM stream_chatspeed_datapoints
			) ORDER BY ts ASC
		) AS dp
	LEFT JOIN stream_decibels AS db
		ON db.stream_id = dp.stream_id AND db.ts = dp.ts
	LEFT JOIN stream_chatspeed_datapoints AS chat
		ON chat.stream_id = dp.stream_id AND chat.ts = dp.ts;
