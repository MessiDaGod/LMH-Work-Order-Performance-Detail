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


SELECT SUM(1)
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
		AND '06/01/2020'
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
