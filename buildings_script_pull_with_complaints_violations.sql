--To get buildings information profiles on each property that a person/company owns, run the following script. Change values in first "where" clause below to customize.

select documentid, PARTYTYPE
into #properties
from acris.ACRIS.PARTIES 
where 
	name like 'PUT COMPANY OR NAME HERE' or address1 like 'PUT ASSOCIATED ADDRESS HERE' or address2 like 'PUT ASSOCIATED ADDRESS HERE TOO'



------------ ACRIS PART 1 -------------
begin try drop table #results end try
begin catch end catch

select 
    P.DOCUMENTID, bbl, streetnumber,streetname, L.unit, propertytype,
    doctype, docdate, docamount, recordedfiled, modifieddate, pcttransferred,
    P.partytype, name, ADDRESS1, ADDRESS2, streetnumber + ' ' + streetname as PropertyAddress,
    DENSE_RANK() OVER (Partition by L.BBL, doctype order by cast([recordedfiled] as date) desc) as DocRecent
    
into #results
from #properties as A
	left join acris.ACRIS.LEGAL as L 
		on A.DOCUMENTID = L.documentid
    left join acris.ACRIS.MASTER as M 
        on L.documentid = M.documentid
    left join acris.ACRIS.PARTIES as P 
        on L.documentid = P.documentid



------------ ACRIS PART 2 -------------

begin try drop table #Properties_Acris end try
begin catch end catch

select 
    MD.BBL, 
    max(MD.PropertyAddress) as PropertyAddress,
    max(MD.unit) as Unit, 
    Max(MD.propertytype) as PropertyType,
    max(MD.name) as Name, 
    max(MD.address1) as Address1, 
    max(MD.address2) as Address2,
    max(case when MD.doctype='DEED' and MD.PARTYTYPE='1' and docrecent=1 then MD.name else null end) as FinalSeller,
    max(case when MD.doctype='DEED' and MD.PARTYTYPE='2' and docrecent=1 then MD.name else null end) as FinalBuyer,
    max(case when MD.doctype='DEED' and MD.PARTYTYPE='1' and docrecent=1 then MD.docdate else null end) as FinalDeedDate,
    max(case when MD.doctype='DEED' and MD.PARTYTYPE='1' and docrecent=1 then MD.DOCAMOUNT else null end) as FinalDeedPrice,
    max(case when MD.doctype='MTGE' and docrecent=1 then MD.docdate else null end) as MortgageDate,
    max(case when MD.doctype='MTGE' and docrecent=1 then MD.DOCAMOUNT else null end) as MortgageAmt,
    max(case when MD.doctype='MTGE' and docrecent=1 and MD.PARTYTYPE='1' then MD.name else null end) as MtgeBorrower,
    max(case when MD.doctype='MTGE' and docrecent=1 and MD.PARTYTYPE='2' then MD.name else null end) as MtgeLender

into #Properties_Acris
FROM #results as MD
GROUP BY MD.BBL

SELECT * FROM #Properties_Acris
--Spot Check for an address

-------------HPD VIOLATIONS PART 1-------------
begin try drop table #HPDResults end try
begin catch end catch

select
hpdv.*, DENSE_RANK() OVER (Partition by violationid order by cast(CurrentStatusDate as datetime2) desc) as RecentStatus

into #HPDResults
from #Properties_Acris as A
    left join [buildings_and_violations].[dbo].[HPD_AllViolations_20151231] as HPDV
        on A.BBL = HPDV.BBL
where cast(hpdv.inspectiondate as datetime2) > cast(A.FinalDeedDate as datetime2)

SELECT * FROM #HPDResults


-------------HPD VIOLATIONS PART 2-------------

begin try drop table #HPDVResults end try
begin catch end catch

select
BBL, 
COUNT(DISTINCT case when CLASS = 'C' and RecentStatus = 1 and CURRENTSTATUS LIKE '%CLOSED%' then VIOLATIONID else null end) as Closed_HPD_C_Violations,
COUNT(DISTINCT case when CLASS = 'B' and RecentStatus = 1 and CURRENTSTATUS LIKE '%CLOSED%' then VIOLATIONID else null end) as Closed_HPD_B_Violations,
COUNT(DISTINCT case when CLASS = 'A' and RecentStatus = 1 and CURRENTSTATUS LIKE '%CLOSED%' then  VIOLATIONID else null end) as Closed_HPD_A_Violations,
COUNT(DISTINCT case when RecentStatus = 1 and CURRENTSTATUS LIKE '%CLOSED%' then  VIOLATIONID else null end) as Closed_HPD_Violations,
COUNT(DISTINCT case when CLASS = 'A' and RecentStatus = 1 and CURRENTSTATUS NOT LIKE '%CLOSED%'  then  VIOLATIONID else null end) as Open_HPD_A_Violations,
COUNT(DISTINCT case when CLASS = 'B' and RecentStatus = 1 and CURRENTSTATUS NOT LIKE '%CLOSED%' then  VIOLATIONID else null end) as Open_HPD_B_Violations,
COUNT(DISTINCT case when CLASS = 'C' and RecentStatus = 1 and CURRENTSTATUS NOT LIKE '%CLOSED%' then  VIOLATIONID else null end) as Open_HPD_C_Violations,
COUNT(DISTINCT case when RecentStatus = 1 and CURRENTSTATUS NOT LIKE '%CLOSED%' then  VIOLATIONID else null end) as Open_HPD_Violations
into #HPDVResults 
from #HPDResults
GROUP BY BBL

select * from #HPDVResults

-------------OATH Fines-------------
begin try drop table #OATHResults end try
begin catch end catch

--select top 100 * from [buildings_and_violations].[dbo].[OATH_ECB_Violations]

select 
oath.BBL,
count(distinct oath.[Ticket Number]) as OATH_Violations,
sum(cast(oath.[Balance Due] as money)) as OATH_TotalBalanceDue,
sum(cast(oath.[Total Violation Amount] as money)) as OATH_TotalFines

into #OATHResults
from #Properties_Acris as A
    left join [buildings_and_violations].[dbo].[OATH_ECB_Violations] as oath
        on A.BBL = oath.BBL
WHERE oath.[Violation Date] > A.FinalDeedDate
AND oath.[Issuing Agency] NOT LIKE 'DOB%'
AND oath.[Issuing Agency] NOT LIKE '%BUILDINGS%'
GROUP BY oath.BBL

select * From #OATHResults

-- DOB VIOLATIONS --------------------------------------------------

begin try drop table #DOBViolations end try
begin catch end catch

select 
DOB.BBL_DASH,
count(distinct dob.[NUMBER]) as DOB_Violations
into #DOBViolations
from #Properties_Acris as A
    left join [buildings_and_violations].[dbo].[DOB_Violations] as dob
        on A.BBL = dob.BBL_DASH
WHERE dob.[ISSUE_DATE_FORMAT] > A.FinalDeedDate
GROUP BY dob.BBL_DASH

select * From #DOBViolations
--where BBL_DASH = '1-437-30'

-- DOB Complaints --------------------------

begin try drop table #DOBComplaints end try
begin catch end catch

select 
DOB.BBL_DASH,
count(distinct case when dob.[Status] LIKE '%CLOSED%' then dob.[Complaint Number] else null end) as Closed_DOB_Complaints,
count(distinct case when dob.[Status] NOT LIKE '%CLOSED%' then dob.[Complaint Number] else null end) as Open_DOB_Complaints
into #DOBComplaints
from #Properties_Acris as A
    left join [buildings_and_violations].[dbo].[DOB_Complaints_Received] as dob
        on A.BBL = dob.BBL_DASH
WHERE dob.[Date Entered] > A.FinalDeedDate
GROUP BY dob.BBL_DASH

select * From #DOBComplaints
--where BBL_DASH = '1-437-30'

-- --------------------------- Stop Work Orders --------------------------------------------------------
begin try drop table #StopWorkResults end try
begin catch end catch

select 
DOB.BBL_DASH,
count(distinct case when dob.[Status] LIKE '%CLOSED%' then dob.[Complaint Number] else null end) as Closed_Stop_Work_Orders,
count(distinct case when dob.[Status] NOT LIKE '%CLOSED%' then dob.[Complaint Number] else null end) as Open_Stop_Work_Orders
into #StopWorkResults
from #Properties_Acris as A
    left join [buildings_and_violations].[dbo].[DOB_Complaints_Received] as dob
        on A.BBL = dob.BBL_DASH
WHERE dob.[Date Entered] > A.FinalDeedDate
AND dob.[Disposition Code] = 'A3'
GROUP BY dob.BBL_DASH

select * From #StopWorkResults


-- DOB ECB VIOLATIONS ----------------------------------------------------------


begin try drop table #DOBECBResults end try
begin catch end catch

select 
ECB.BBL_DASH,
count(distinct case when ECB_VIOLATION_STATUS LIKE '%RESOLVE%' then ECB.[ECB_VIOLATION_NUMBER] else NULL end) as Closed_DOB_ECB_Violations,
count(distinct case when ECB_VIOLATION_STATUS NOT LIKE '%RESOLVE%' then ECB.[ECB_VIOLATION_NUMBER] else NULL end) as Open_DOB_ECB_Violations,
sum(cast(ecb.[Balance_Due] as money)) as DOB_ECB_TotalBalanceDue,
sum(cast(ecb.[PENALITY_IMPOSED] as money)) as DOB_ECB_TotalFines
into #DOBECBResults
from #Properties_Acris as A
    left join [buildings_and_violations].[dbo].[DOB_ECB_Violations] as ECB
        on A.BBL = ECB.BBL_DASH
WHERE ecb.[ISSUE_DATE_FORMAT] > A.FinalDeedDate 
GROUP BY ECB.BBL_DASH

select * From #DOBECBResults
--where BBL_DASH = '1-622-25'

-------------DOF RENT STABILIZATION INFORMATION
begin try drop table #DOFResults end try
begin catch end catch

select
    DOF.BBL, 
    DOFUnits = min(DOF.unitstotal),
    max(cast(DOF.[2007uc] AS INT)) as RentStab2007, max(cast (DOF.[2008uc] AS INT)) as RentStab2008, max(cast (DOF.[2009uc] AS INT)) as RentStab2009, max(cast (DOF.[2010uc] AS INT)) as RentStab2010, max(cast (DOF.[2011uc] AS INT)) as RentStab2011, max(cast (DOF.[2012uc] AS INT)) as RentStab2012, max(cast (DOF.[2013uc] AS INT)) as RentStab2013, 
    RentStab2014 = max(cast (DOF.[2014uc] as int)),
    ResUnits = CAST(max(DOF.[unitsres])as int),
        Case when max(datepart(yyyy, FinalDeedDate)) <= 2007 then max(cast (DOF.[2007uc] AS INT))
        when max(datepart(yyyy, FinalDeedDate)) = 2008 then max(cast (DOF.[2008uc] AS INT))
        when max(datepart(yyyy, FinalDeedDate)) = 2009 then max(cast (DOF.[2009uc] AS INT)) 
        when max(datepart(yyyy, FinalDeedDate)) = 2010 then max(cast (DOF.[2010uc] AS INT))
        when max(datepart(yyyy, FinalDeedDate)) = 2011 then max(cast (DOF.[2011uc] AS INT)) 
        when max(datepart(yyyy, FinalDeedDate)) = 2012 then max(cast (DOF.[2012uc] AS INT))
        when max(datepart(yyyy, FinalDeedDate)) = 2013 then max(cast (DOF.[2013uc] AS INT))
        when max(datepart(yyyy, FinalDeedDate)) >= 2014 then max(cast (DOF.[2014uc] AS INT)) 
        else Null end as RentStabatPurchase

into #DOFResults
from #Properties_Acris as A
    left join [emma].[dbo].[DOF_joined] as DOF
        on A.BBL = DOF.BBL
GROUP BY DOF.BBL

Select * 
From #DOFResults


-- -----------311 Complaints --------------------------------------
begin try drop table #Complaints311 end try
begin catch end catch
-- Naive Address Join
SELECT a.BBL, count(distinct c.[UniqueKey]) as Complaints_311_Count
INTO #Complaints311
FROM #Properties_Acris as a
	LEFT JOIN [nyc311].[dbo].[data20151030] as c
	ON a.[PropertyAddress]=c.[IncidentAddress]
WHERE (
	c.ComplaintType LIKE '%HEAT%'
	OR c.ComplaintType LIKE '%LEAD%'
	OR c.ComplaintType LIKE '%WATER%'
	OR c.ComplaintType LIKE '%PAINT%'
	OR c.Descriptor LIKE '%DUST%'
	)
GROUP BY a.BBL
;
SELECT * FROM #Complaints311

-------------MERGING ALL TOGETHER
begin try drop table #FINALTABLE end try
begin catch end catch

select
    --
    A.BBL,
    max(A.PropertyAddress) as PropertyAddress, 
    Max(A.Unit) as Unit,
    max(A.PropertyType) as PropertyType,
    -- Acris
    max(A.Address1) as BuyerAddress1,
    max(A.Address2) as BuyerAddress2, 
    max(A.FinalSeller) as FinalSeller, 
    max(A.FinalBuyer) as FinalBuyer, 
    max(A.FinalDeedDate) as FinalDeedDate, 
    max(A.FinalDeedPrice) as FinalDeedPrice,
    max(A.MortgageDate) as MortgageDate, 
    max(A.MortgageAmt) as MortgageAmt, 
    max(A.MtgeBorrower) as MtgeBorrower, 
    max(A.MtgeLender) as MtgeLender, 
    -- hpd violations
    max(Closed_HPD_C_Violations) as Closed_HPD_C_Violations_SincePurchase, 
    max(Open_HPD_C_Violations) AS Open_HPD_C_Violations_SincePurchase,
    max(Closed_HPD_B_Violations) as Closed_HPD_B_Violations_SincePurchase, 
    max(Open_HPD_B_Violations) AS Open_HPD_B_Violations_SincePurchase, 
    max(Closed_HPD_A_Violations) as Closed_HPD_A_Violations_SincePurchase, 
    max(Open_HPD_A_Violations) AS Open_HPD_A_Violations_SincePurchase, 
    max(Closed_HPD_VIOLATIONS) as Closed_HPD_Violations_SincePurchase, 
    max(Open_HPD_Violations) as Open_HPD_Violations_SincePurchase,
    --oath violations
    max(OATH_TotalFines) as ECB_TotalFines_2010orPurchase_to2016, 
    max(OATH_TotalBalanceDue) as ECB_TotalBalanceDue_2010orPurchase_to2016,
    max(OATH_Violations) as ECB_TotalECBViolations_2010orPurchase_to2016,
    --dob ecb violations here
    max(DOB_ECB_TotalFines) as DOB_ECB_TotalFines_SincePurchase, 
    max(DOB_ECB_TotalBalanceDue) as DOB_ECB_TotalBalanceDue_SincePurchase,
    max(Closed_DOB_ECB_Violations) as Closed_DOB_ECB_TotalECBViolations_SincePurchase,
    max(Open_DOB_ECB_Violations) as Open_DOB_ECB_TotalECBViolations_SincePurchase,
    -- dob complaints
    max(Closed_DOB_Complaints) as Closed_DOB_Complaints_SincePurchase,
    max(Open_DOB_Complaints) as Open_DOB_Complaints_SincePurchase,
    -- stop work orders (subset dob complaints)
    max(Closed_Stop_Work_Orders) as Closed_Stop_Work_Order_SincePurchase,
    max(Open_Stop_Work_Orders) as Open_Stop_Work_Order_SincePurchase,
    -- dob violations
    max(DOB_Violations) as DOB_Violations_SincePurchase,
    -- DOF
    max(RentStabatPurchase) as RentStab_at_Purchase_or_2007,
    max(RentStab2014 - RentStabatPurchase) as Deregulated_Since_Purchase_or_2007,
    max(RentStab2014) as RentStab_2014,
	-- 311
	max(Complaints_311_Count) as Complaints_311_Count

Into #FINALTABLE
FROM #Properties_Acris as A
    left join #HPDVResults AS HPDV
     on A.BBL = HPDV.BBL
     left join #OATHResults AS ECB
     on A.BBL = ECB.BBL
     left join #DOFResults AS DOF
     on A.BBL = DOF.BBL
     left join #DOBViolations AS DOBv
     on A.BBL = DOBv.BBL_DASH
     left join #DOBComplaints as DOBc
     on A.BBL = DOBc.BBL_DASH
     LEFT JOIN #DOBECBResults AS DOBECB
     on A.BBL = DOBECB.BBL_DASH
     LEFT JOIN #StopWorkResults as SW
     on A.BBL = SW.BBL_DASH
	 LEFT JOIN #Complaints311 C
	 ON A.BBL = c.BBL

group by A.BBL


Select *
From #FINALTABLE
