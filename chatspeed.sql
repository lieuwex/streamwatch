CREATE VIEW stream_chatspeed AS
	SELECT
		stream_id,
		ts,
		AVG(messages) OVER (PARTITION BY stream_id ORDER BY ts ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS speed
	FROM stream_chatspeed_datapoints;
