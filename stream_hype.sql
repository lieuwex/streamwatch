-- If only sqlite supported FULL OUTER JOIN...
DROP VIEW IF EXISTS stream_hype_datapoints;
CREATE VIEW stream_hype_datapoints AS
	SELECT
		dp.stream_id,
		dp.ts,
		loudness.momentary AS loudness,
		chat.messages AS messages,
		(
			(
				CASE
					WHEN loudness.momentary IS NULL then 0.0
					ELSE (1.0/2.0 * (1.0 + TANH(5.0 * (loudness.momentary + 75.0/2.0)/80.0)))
				END
			) + (
				CASE
					WHEN chat.messages IS NULL then 0.0
					ELSE (chat.messages / 5.0)
				END
			)
		) AS hype
	FROM (
			SELECT DISTINCT stream_id,ts
			FROM (
				SELECT stream_id,ts FROM stream_loudness
				UNION ALL
				SELECT stream_id,ts FROM stream_chatspeed_datapoints
			)
		) AS dp
	LEFT JOIN stream_loudness AS loudness
		ON loudness.stream_id = dp.stream_id AND loudness.ts = dp.ts
	LEFT JOIN stream_chatspeed_datapoints AS chat
		ON chat.stream_id = dp.stream_id AND chat.ts = dp.ts;

-- SAD
DROP VIEW IF EXISTS stream_hype_datapoints_sad;
CREATE VIEW stream_hype_datapoints_sad AS
	SELECT
		dp.stream_id,
		dp.ts,
		loudness.momentary AS loudness,
		chat.messages AS messages
	FROM (
			SELECT DISTINCT stream_id,ts
			FROM (
				SELECT stream_id,ts FROM stream_loudness
				UNION ALL
				SELECT stream_id,ts FROM stream_chatspeed_datapoints
			)
		) AS dp
	LEFT JOIN stream_loudness AS loudness
		ON loudness.stream_id = dp.stream_id AND loudness.ts = dp.ts
	LEFT JOIN stream_chatspeed_datapoints AS chat
		ON chat.stream_id = dp.stream_id AND chat.ts = dp.ts;
