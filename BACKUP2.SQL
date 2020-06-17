

/* Should get 1566 records for all of May 2020 Calls */
SELECT 1 AS 'KeyColumn'
	,wo.hmy wonum
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
INNER JOIN lmhwodata lmh ON lmh.hmy = w.hmy
LEFT JOIN mm2wodet wod ON wo.hmy = wod.hwo
LEFT JOIN property p ON (p.hmy = wo.hproperty)
LEFT JOIN unit u ON (wo.hunit = u.hmy)
LEFT JOIN building b ON (wo.hbuilding = b.hmy)
LEFT JOIN vendor v ON (wo.hVendor = v.hmyperson)
LEFT JOIN mm2wotmpl tmpl ON (wo.htmpl = tmpl.hmy)
LEFT JOIN mmasset asset ON (wo.hasset = asset.hmy)
LEFT JOIN person emp ON (wod.hperson = emp.hmy)
LEFT JOIN MM2EMPLRATES skill ON (wod.hrate = skill.hmy)
LEFT JOIN (
	SELECT wh.hwo
		,wh.sStatus WOStatus
		,MIN(wh.dtUserModified) AS 'tstamp'
		,isnull(pu.uName, 'DBO') UserName
	FROM WOHistory wh
	LEFT JOIN pmuser pu ON pu.hmy = wh.hUserModifiedBy
	WHERE wh.sStatus = 'First Contact'
	GROUP BY wh.hmy
		,wh.sStatus
		,isnull(pu.uName, 'DBO')
	) FC ON FC.hwo = wo.hmy
WHERE (
		wo.SPRIORITY LIKE '%Emergency%'
		OR wo.SPRIORITY LIKE '%Routine%'
		) 	#CONDITIONS#
	/*AND p.hmy IN (
		SELECT hProperty
		FROM listprop2
		WHERE hPropList IN (1017)
		)
	AND wo.dtcall BETWEEN '2020-05-01'
		AND dateadd(day, 1, '2020-05-31')*/

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
ORDER BY CASE 
		WHEN isnull(rtrim(wo.sStatus), 'NONE') IN (
				'NONE'
				,'NULL'
				,''
				)
			THEN 'NONE'
		ELSE wo.sStatus
		END
	,isnull(wo.sPriority, '');
	
SELECT isnull(wo.sPriority, '') Code
	,isnull(wo.sPriority, '') Priority
	,CASE 
		WHEN isnull(rtrim(wo.sStatus), 'NONE') IN (
				'NONE'
				,'NULL'
				,''
				)
			THEN 'NONE'
		ELSE wo.sStatus
		END STATUS
	,count(DISTINCT wo.hMy) Count
INTO lmhwohistorytemp
FROM mm2wO wo
LEFT JOIN mm2wodet wod ON wo.hmy = wod.hwo
LEFT JOIN property p ON (p.hmy = wo.hproperty)
LEFT JOIN unit u ON (wo.hunit = u.hmy)
LEFT JOIN building b ON (wo.hbuilding = b.hmy)
LEFT JOIN vendor v ON (wo.hVendor = v.hmyperson)
LEFT JOIN mm2wotmpl tmpl ON (wo.htmpl = tmpl.hmy)
LEFT JOIN mmasset asset ON (wo.hasset = asset.hmy)
LEFT JOIN person emp ON (wod.hperson = emp.hmy)
LEFT JOIN MM2EMPLRATES skill ON (wod.hrate = skill.hmy)
WHERE wo.dtCall BETWEEN '05/01/2020'
		AND '05/31/2020'
	AND (
		wo.SPRIORITY LIKE '%Emergency%'
		OR wo.SPRIORITY LIKE '%Routine%'
		)
	AND wo.hproperty IN (
		SELECT lp.hProperty
		FROM property p1
		INNER JOIN listprop2 lp ON (p1.HMY = lp.hPropList)
		WHERE scode IN ('fortsam')
		)
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
ORDER BY isnull(wo.sPriority, '')


SELECT 1 AS 'KeyColumn'
	,FORMAT(w.dtCall, 'M/dd/yyyy hh:mm tt') dtCall
	,FORMAT(FirstContact.tstamp, 'M/dd/yyyy hh:mm tt') FirstContactTime
	,FORMAT(ISNULL(MIN(d.dtActStart), w.dtWCompl), 'M/dd/yyyy hh:mm tt') workstart
	,ISNULL(MIN(d.dtActStart), w.dtWCompl) AS ResponseDate
	,ISNULL(MAX(d.dtActFinish), w.dtWCompl) AS CompletionDate
	,DATENAME(WEEKDAY, w.dtcall) AS DayOfCall
	,CASE 
		WHEN w.spriority LIKE '%Emergency%'
			AND (
				(
					DATENAME(WEEKDAY, w.dtCall) = 'Saturday'
					OR DATENAME(WEEKDAY, w.dtCall) = 'Sunday'
					)
				OR (
					(
						DATEPART(HOUR, w.dtcall) BETWEEN 19
							AND 23
						OR DATEPART(HOUR, w.dtcall) BETWEEN 0
							AND 7
						)
					)
				)
			AND DATEDIFF(MINUTE, w.dtcall, ISNULL(MIN(d.dtActStart), w.dtWCompl)) <= 60
			THEN 'Yes (Weekend/After hours)'
		WHEN w.spriority LIKE '%Emergency%'
			AND DATEDIFF(MINUTE, w.dtcall, ISNULL(MIN(d.dtActStart), w.dtWCompl)) <= 30
			THEN 'Yes'
		WHEN w.spriority NOT LIKE '%Emergency%'
			THEN NULL
		ELSE 'No'
		END AS 'EmergencyGoal'
	,CASE 
		WHEN w.spriority LIKE '%Routine%'
			AND (DATEDIFF(HOUR, w.dtcall, ISNULL(MIN(d.dtActStart), w.dtWCompl)) <= 24)
			THEN 'Yes'
		WHEN w.spriority NOT LIKE '%Routine%'
			THEN NULL
		ELSE 'No'
		END AS 'RoutineGoal'
	,CASE 
		WHEN w.spriority LIKE '%Emergency%'
			AND (
				(
					DATENAME(WEEKDAY, w.dtCall) = 'Saturday'
					OR DATENAME(WEEKDAY, w.dtCall) = 'Sunday'
					)
				OR (
					(
						DATEPART(HOUR, w.dtcall) BETWEEN 19
							AND 23
						OR DATEPART(HOUR, w.dtcall) BETWEEN 0
							AND 7
						)
					)
				)
			AND DATEDIFF(MINUTE, w.dtcall, ISNULL(MIN(d.dtActStart), w.dtWCompl)) <= 60
			THEN 'Yes (Weekend/After hours)'
		WHEN w.spriority LIKE '%Emergency%'
			AND DATEDIFF(MINUTE, w.dtcall, ISNULL(MIN(d.dtActStart), w.dtWCompl)) <= 30
			THEN 'Yes'
		WHEN w.spriority LIKE '%Emergency%'
			AND ISNULL(MIN(d.dtActStart), w.dtWCompl)
		ELSE CASE 
				WHEN w.spriority LIKE '%Routine%'
					AND (
						DATEADD(DAY, 1, CAST(w.dtcall AS DATE)) <= ISNULL(MIN(d.dtActStart), w.dtWCompl)
						OR DATEDIFF(HOUR, w.dtcall, ISNULL(MIN(d.dtActStart), w.dtWCompl)) <= 24
						)
					THEN 'Yes'
				END
		END AS 'CombineGoal'			
	,w.spriority Priority
	,w.scategory Category
	,w.ssubcat subcategory
	,FORMAT(w.dtwcompl, 'M/dd/yyyy hh:mm tt') AS 'dtcompl'
	,dbo.FormatPhoneNumber(w.sphone) AS 'phone'
	,w.sbriefdesc issue
	,w.sfulldesc
	,w.stechnotes
	,w.scallername sCALLER
	,u.scode uscode
	,p.scode pscode
	,w.hmy wonum
	,w.SSTATUS
	,FORMAT(FirstContact.tstamp, 'M/dd/yyyy hh:mm tt') AS 'tstamp'
	,DATENAME(MONTH, w.dtcall) AS 'Month'
	,'#@@OutputType#' AS 'OutputType'
INTO lmhwohistorytemp
FROM mm2wo w
INNER JOIN property p ON p.hmy = w.hproperty
INNER JOIN lmhwodata lmh ON lmh.hmy = w.hmy
LEFT JOIN mm2wodet d ON d.hwo = w.hmy
LEFT JOIN unit u ON u.hmy = w.hunit
OUTER APPLY (
	SELECT wh.hmy
		,wh.sStatus WOStatus
		,wh.dtUserModified AS 'tstamp'
		,wh.sstatus
		,isnull(pu.uName, 'DBO') UserName
	FROM WOHistory wh
	LEFT JOIN pmuser pu ON pu.hmy = wh.hUserModifiedBy
	WHERE hWO = w.hmy
		AND wh.sStatus = 'First Contact'
	) FirstContact
WHERE (
		w.spriority LIKE '%Emergency%'
		OR w.spriority LIKE '%Routine%'
		) #Conditions#
GROUP BY FORMAT(w.dtCall, 'M/dd/yyyy hh:mm tt')
	,FORMAT(FirstContact.tstamp, 'M/dd/yyyy hh:mm tt')
	,DATENAME(WEEKDAY, w.dtcall)
	,w.spriority
	,w.scategory
	,w.ssubcat
	,FORMAT(w.dtwcompl, 'M/dd/yyyy hh:mm tt')
	,dbo.FormatPhoneNumber(w.sphone)
	,w.sbriefdesc
	,w.sfulldesc
	,w.stechnotes
	,w.scallername
	,u.scode
	,p.scode
	,w.hmy
	,w.SSTATUS
	,FORMAT(FirstContact.tstamp, 'M/dd/yyyy hh:mm tt')
	,DATENAME(MONTH, w.dtcall)
	,w.DTCALL
	,w.DTWCOMPL
ORDER BY w.spriority
	,w.scategory
	,w.ssubcat;

	
//END SELECT 

//SELECT Detail 

SELECT *
FROM lmhwohistorytemp

//END SELECT
