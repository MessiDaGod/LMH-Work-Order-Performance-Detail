WITH CTE
AS (
	SELECT 1 AS 'KeyColumn'
		,ROW_NUMBER() OVER (
			PARTITION BY wo.hmy
			,wo.dtcall ORDER BY fc.tstamp DESC
			) rownum
		,wo.hmy wonum
		,wh.sStatus HistoryStatus
		,FORMAT(wo.dtCall, 'M/dd/yyyy hh:mm tt') dtCall
		,FORMAT(FC.tstamp, 'M/dd/yyyy hh:mm tt') FirstContactTime
		,FORMAT(ISNULL(MIN(wod.dtActStart), wo.dtWCompl), 'M/dd/yyyy hh:mm tt') workstart
		,ISNULL(MIN(wod.dtActStart), wo.dtWCompl) AS ResponseDate
		,ISNULL(MAX(wod.dtActFinish), wo.dtWCompl) AS CompletionDate
		,DATENAME(WEEKDAY, wo.dtcall) AS DayOfCall
		,isnull(wo.sPriority, '') Priority
		,CASE 
			WHEN wo.spriority LIKE '%Emergency%'
				AND (
					(
						DATENAME(WEEKDAY, wo.dtCall) = 'Saturday'
						OR DATENAME(WEEKDAY, wo.dtCall) = 'Sunday'
						)
					OR (
						(
							DATEPART(HOUR, wo.dtcall) BETWEEN 19
								AND 23
							OR DATEPART(HOUR, wo.dtcall) BETWEEN 0
								AND 7
							)
						)
					)
				AND DATEDIFF(MINUTE, wo.dtcall, ISNULL(MIN(wod.dtActStart), wo.dtWCompl)) <= 60
				THEN 'Yes (1 Hour)'
			WHEN wo.spriority LIKE '%Emergency%'
				AND DATEDIFF(MINUTE, wo.dtcall, ISNULL(MIN(wod.dtActStart), wo.dtWCompl)) <= 30
				THEN 'Yes (30 Mins)'
			ELSE CASE 
					WHEN wo.spriority LIKE '%Routine%'
						AND (
							DATEADD(DAY, 1, CAST(wo.dtcall AS DATE)) <= ISNULL(MIN(wod.dtActStart), wo.dtWCompl)
							OR DATEDIFF(HOUR, wo.dtcall, ISNULL(MIN(wod.dtActStart), wo.dtWCompl)) <= 24
							)
						THEN 'Yes'
					END
			END AS 'CombineGoal'
		,CASE 
			WHEN isnull(rtrim(wo.sStatus), 'NONE') IN (
					'NONE'
					,'NULL'
					,''
					)
				THEN 'NONE'
			ELSE wo.sStatus
			END 'STATUS'
		,count(DISTINCT wo.hMy) Count
		,CASE 
			WHEN lmh.complete_goal = 0
				THEN 'Yes'
			WHEN lmh.pending_reason NOT LIKE 'Qualified%'
				THEN 'Yes'
			ELSE 'No'
			END JerrodGoal
	FROM mm2wO wo
	LEFT JOIN lmhwodata lmh ON lmh.hmy = wo.hmy
	LEFT JOIN mm2wodet wod ON wo.hmy = wod.hwo
	LEFT JOIN property p ON (p.hmy = wo.hproperty)
	LEFT JOIN unit u ON (wo.hunit = u.hmy)
	LEFT JOIN building b ON (wo.hbuilding = b.hmy)
	LEFT JOIN vendor v ON (wo.hVendor = v.hmyperson)
	LEFT JOIN mm2wotmpl tmpl ON (wo.htmpl = tmpl.hmy)
	LEFT JOIN mmasset asset ON (wo.hasset = asset.hmy)
	LEFT JOIN person emp ON (wod.hperson = emp.hmy)
	LEFT JOIN MM2EMPLRATES skill ON (wod.hrate = skill.hmy)
	INNER JOIN WOHistory wh ON wh.hWO = WO.HMY
	INNER JOIN (
		SELECT MIN(wh.hmy) whHmy
			,wh.hwo
			,wh.sStatus WOStatus
			,wh.dtUserModified AS 'tstamp'
			,isnull(pu.uName, 'DBO') UserName
		FROM WOHistory wh
		LEFT JOIN pmuser pu ON pu.hmy = wh.hUserModifiedBy
		--WHERE wh.sStatus = 'First Contact'
		GROUP BY wh.sStatus
			,isnull(pu.uName, 'DBO')
			,wh.hWO
			,wh.dtUserModified
		) FC ON FC.whHmy = wh.hMy
	WHERE (
			wo.SPRIORITY LIKE '%Emergency%'
			OR wo.SPRIORITY LIKE '%Routine%'
			)
		AND p.hmy IN (
			SELECT hProperty
			FROM listprop2
			WHERE hPropList IN (1017)
			)
		AND wo.dtcall BETWEEN '2020-05-01'
			AND dateadd(day, 1, '2020-05-31')
	GROUP BY isnull(wo.sPriority, '')
		,CASE 
			WHEN isnull(rtrim(wo.sStatus), 'NONE') IN (
					'NONE'
					,'NULL'
					,''
					)
				THEN 'NONE'
			ELSE wo.sStatus
			END
		,wo.DTCALL
		,wo.DTWCOMPL
		,FC.tstamp
		,wo.SPRIORITY
		,wo.HMY
		,wo.SSTATUS
		,wh.sStatus
		,lmh.complete_goal
		,lmh.pending_reason
		,wh.sStatus
		/*ORDER BY CASE 
		WHEN isnull(rtrim(wo.sStatus), 'NONE') IN (
				'NONE'
				,'NULL'
				,''
				)
			THEN 'NONE'
		ELSE wo.sStatus
		END
	,isnull(wo.sPriority, '')*/
	)
SELECT DISTINCT wonum
	,rownum
	,dtCall
	,FirstContactTime
	,workstart
	,ResponseDate
	,CompletionDate
	,DayOfCall
	,Priority
	,CombineGoal
	,HistoryStatus
FROM CTE
WHERE HistoryStatus IS NOT NULL
	--WHERE WONUM = 10279313
	--GROUP BY wonum
	--	,dtCall
	--	,FirstContactTime
	--	,workstart
	--	,ResponseDate
	--	,CompletionDate
	--	,DayOfCall
	--	,Priority
	--	,CombineGoal
	--	,STATUS
	--WHERE STATUS IN ('Pending / Vendor','Pending / Parts', 'Pending / Appointment', 'Pending / Continuous Work')
