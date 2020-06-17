/*
FOR Emergency work orders 30 minute to respond during the workday, 1 hour after hours and weekends to RESPOND, NO COMPLETE
for routine they have to respond and complete within 24 hours or the end of the next business day WHICHEVER IS GREATER (response and complete)
they want the first contact time
NO URGENT INCLUDED
*/
//SELECT NO CRYSTAL 

IF OBJECT_ID('lmhwohistorytemp') IS NOT NULL
	DROP TABLE lmhwohistorytemp;

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
