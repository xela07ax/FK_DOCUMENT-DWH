Скрипт для проверки а Аджинити	Скрипт с заменами для джоба в Дата Стейдж
/* Modifikatciya iz INSURER_ PENALTY_DELAY_SCHEDULE to INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY	
Erdyakov Aleksey 01.11.16 */

with
SRC_TOTAL as
(
select rr_3.*  

, CAST(case when rr_3.INSURER_PENALTY_BANKRUPT_RTK_GID = 1   or rr_3.INSURER_PENALTY_BANKRUPT_RTK_GID = 1 then 0 else 1 end as Integer)  as check_flg, 
    cast(
'src_code=' || rr_3.src_code || ';ASV_BANKRUPT_RTK_RESPONSIBILITY;'
|| case when rr_3.INSURER_PENALTY_BANKRUPT_RTK_GID = 1 then 'INSURER_PENALTY_BANKRUPT_RTK_GID/BANKRUPT_RTK_ID,' else ',' end
|| case when rr_3.INSURER_RESPONSIBILITY_AMOUNT_GID = 1 then 'INSURER_RESPONSIBILITY_AMOUNT_GID/R_AMOUNT_ID,' else ',' end
as nvarchar(2048))as field1
--SELECT rr_3.*
 from 
-- rr_3 (8)   ???????? ?????? ?????? ??? ????????? INS, DEL, UPD
( 
--!! (7) rr_2_2 -  ??????????? ??????????, ??? ???? ?? ????????
 select distinct new_uid  as uid
  , INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY_GID
  , INSURER_PENALTY_BANKRUPT_RTK_GID, INSURER_RESPONSIBILITY_AMOUNT_GID, DECISION_SUM
  , id
  ,entity_code
  ,src_code
  ,new_sd as start_date
  ,new_ed as end_date
 -- -- ??? ???????? ???????? ???????
  ,cast(case when (start_date1  <> new_sd or new_ed < new_sd ) and new_uid <> -999 then 1 else 0 end  as Byteint) as is_del
 --, max_uid
 from 
( 
 -- (6)  rr_2_1 - ??????????? ??????????, ??? ???? ?? ????????
  select -- nvl(trg.uid, 0) as uid,
INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY_GID
  ,id
  ,src_code
  ,entity_code
  ,INSURER_PENALTY_BANKRUPT_RTK_GID, INSURER_RESPONSIBILITY_AMOUNT_GID, DECISION_SUM
  , start_date as start_date1, end_date as end_date1
  ,min(start_date) over (partition by src_code, entity_code,  id, gr, is_disabled) as new_sd --v3.0
  ,max(end_date) over (partition by src_code, entity_code,  id, gr, is_disabled) as new_ed --v3.0
  ,new_uid 
  ,max(new_uid) over (partition by src_code, entity_code,  id, gr ) as max_uid --v3.0
from
--- new  rr_g1 ---
(select  rr_g.*, sum(gr_start) over (partition by rr_g.src_code, rr_g.entity_code, rr_g.id order by rr_g.START_DATE)  as  gr
,case when is_disabled || part = '12'  then start_date-1  else end_date_p1 end  as end_date--new3
from
( -- (5)  rr_g - ??????????? end_date
 select rr.*
  /*, nvl(lead(rr.start_date-1) over (partition by rr.src_code, rr.entity_code, rr.id  order by rr.START_DATE)
     , case when is_disabled = 0 then  to_date('31.12.9999', 'dd.mm.yyyy') else rr.start_date -1 end
        ) as end_date*/
  ,nvl(
	lead(rr.start_date-1) over (partition by rr.src_code, rr.entity_code, rr.id , for_calc_period  order by rr.START_DATE)
    ,case when is_disabled = 0 then  to_date('31.12.9999', 'dd.mm.yyyy') else rr.start_date -1 end
    ) as end_date_p1  --new3  
	
	-- Beryom znachimye polya, tol'ko bes id tablicy i vstavlyem v etot nabor dly sverki, on soedinit stolbcy i pokajet predydushiy nabor (dumayu budet 2 nabora, odin kotorye dubliruyut sucshestvuyucshie? a drugoq schuschestvuyuschie)
, lag(nvl(INSURER_PENALTY_BANKRUPT_RTK_GID,0) ||'@'|| 
     nvl(INSURER_RESPONSIBILITY_AMOUNT_GID,0) ||'@'|| 
     nvl(DECISION_SUM,0) ||'@'|| 
     is_disabled) over (partition by  src_code, entity_code, id  order by start_date )  as atr
,case when(atr = nvl(INSURER_PENALTY_BANKRUPT_RTK_GID,0) ||'@'|| 
          nvl(INSURER_RESPONSIBILITY_AMOUNT_GID,0) ||'@'|| 
          nvl(DECISION_SUM,0) ||'@'|| 
          is_disabled) then 0 else 1 end as gr_start
 from 
     --  (4) rr -- ?????????? ????????? ?????? ?? 1 ???? ? ??????? rank()  
    (  
	--===========================================================================================================================
--===========================================================================================================================
--===========================================================================================================================
--===========================================================================================================================
--===========================================================================================================================
									 select rr_r.*
    ,rank() over ( partition by rr_r.src_code, rr_r.entity_code, rr_r.id, rr_r.start_date  order by rr_r.part) as rnk 
    ,max(uid) over ( partition by rr_r.src_code, rr_r.entity_code, rr_r.id, rr_r.start_date ) as new_uid -- ?oiau acyou uid ec TARGET
  ,case when part = 1 then 1 when part= 2 and is_disabled = 0 then 1 else 2 end as for_calc_period --new3
  from 
  -- (3) rr_r - Dobavlyaem polya 1. ranc() )  SRC ?   ?????? TRG
  ( 

	--===========================================================================================================================
	--5. Если соответствия не найдено, то целевое поле заполняется константой = 1
	--Если на источнике значение NULL, то целевое поле заполняется константой = 0
	--===========================================================================================================================
	 
		select nvl(m_trg.INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY_GID, -999) as INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY_GID -- Vtoraya stroka v Mmappinge (GID v m-tablitce on je surrogantnyi kluch))
					  ,src_all.id -- Id istochnika vidimo systemnoe
					  ,case when src_all.BANKRUPT_RTK_ID is null then 0 else nvl(m_IPD.INSURER_PENALTY_BANKRUPT_RTK_GID, 1) end as INSURER_PENALTY_BANKRUPT_RTK_GID
					  ,case when src_all.R_AMOUNT_ID is null then 0 else nvl(m_IRA.INSURER_RESPONSIBILITY_AMOUNT_GID, 1) end as INSURER_RESPONSIBILITY_AMOUNT_GID
			          ,src_all.DECISION_SUM
					  
					  ,CAST(case when src_all.AUD_TYPE = 'DL' then 1 else 0  end as Integer) as  IS_DISABLED -- pometka na udalenie 
					  
					  ,src_all.start_date
					  
                      ,1 as part -- Neponytno zachem
                      ,-999 as uid -- nuponytno zachem
					  ,src_all.entity_code -- Neponyatno poka zachem? ved' budet vyvodit'cya tolko u istochnika
					  ,src_all.src_code
-- SELECT *

                  from (
 
-- Itogi gruppirovki MAX_AUD_TIME
 --=============================
	select id -- Poka ponyatno chto on prosto est', odnako ponytno chto eto SID kotoryi v M_ tablitce
	,BANKRUPT_RTK_ID, R_AMOUNT_ID,DECISION_SUM, AUD_TYPE, AUD_TIME, SD , MAX_AUD_TIME, entity_code, SRC_CODE, sd as START_DATE
	-- SELECT *
 from (
 select  cast (RTK_RESPONSIBILITY_ID as nvarchar(1024)) as id -- Svyzyvaemsya s  DWH.M_INSURER_PENALTY_BANKRUPT_RTK_SCHEDUL
 		,BANKRUPT_RTK_ID -- Svyzyvaemsya s  DWH.M_INSURER_PENALTY_BANKRUPT_RTK 
		,R_AMOUNT_ID  -- Svyzyvaemsya s DWH.M_INSURER_RESPONSIBILITY_AMOUNT
		,RTK_RESPONSIBILITY_DECISION_SUM as DECISION_SUM -- Ssumma zadoljennosty
		
		,AUD_TYPE, AUD_TIME -- systemnye polya *
		,date(AUD_TIME) as SD -- Otsekaem vremya, ostavlyia tol'ko datu izmeneniya, ona budet START_DATE
		,max(AUD_TIME) over (partition by aud_user, cast (RTK_RESPONSIBILITY_ID as nvarchar(1024)), date(AUD_TIME) ) as MAX_AUD_TIME --v3.0 Prostavlyaem vsem zapisya maksimalnoe vremya lyubyh izmeneniy za svoi den'
		,cast(950 as smallint) as entity_code -- Chislovoy kod tablytcy-istochnika dannyh ( dlya DELAY_SCHEDULE .ENTITY_CODE = 950)
		
		,cast(AUD_USER as smallint) as SRC_CODE  --v3.0 Chislovoy kod systemy-istochnika dannyh PENALTY_DELAY_SCHEDULE .SRC_CODE BETWEEN 129 AND 300
	   -- SELECT  *
       from  UTS_SRC001.UTS_SRC001_USR.ASV_BANKRUPT_RTK_RESPONSIBILITY SRC 
          where LOAD_DATE >=  to_timestamp( '10-01-2005  12:00:00', 'dd.mm.yyyy hh24:mi:ss') 
            and LOAD_DATE <= to_timestamp( '10-01-2025  12:00:00', 'dd.mm.yyyy hh24:mi:ss')
            and AUD_TYPE in ('UP', 'DL', 'PT', 'RR')
       		 and cast(AUD_USER as smallint) between 129 and 300 
				)src1
			 where AUD_TIME = MAX_AUD_TIME 
--===================================
			  )src_all
                     -- !!!   M_Table
    				left join UTS_DWH.UTS_DWH_USR.M_INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY m_trg
	   		on cast(src_all.id as nvarchar(1024)) = m_trg.SID 
				and src_all.SRC_CODE = m_trg.SRC_CODE
					and m_trg.ENTITY_CODE = 1006
					AND m_trg.SRC_CODE BETWEEN 129 AND 300
			left join  UTS_DWH.UTS_DWH_USR.M_INSURER_PENALTY_BANKRUPT_RTK m_IPD
			on cast(src_all.BANKRUPT_RTK_ID as nvarchar(1024)) = m_IPD.SID
				and cast(src_all.SRC_CODE as smallint) = m_IPD.SRC_CODE
					AND m_IPD.ENTITY_CODE = 1005
					AND m_IPD.SRC_CODE BETWEEN 129 AND 300
			left join  UTS_DWH.UTS_DWH_USR.M_INSURER_RESPONSIBILITY_AMOUNT m_IRA
			on cast(src_all.R_AMOUNT_ID as nvarchar(1024)) = m_IRA.SID
				and cast(src_all.SRC_CODE as smallint) = m_IRA.SRC_CODE
					AND m_IRA.ENTITY_CODE = 156
					AND m_IRA.SRC_CODE BETWEEN 129 AND 300
			 		 
-- for union orign 2

 union --distinct
--===========================================================================================================================
--===========================================================================================================================			
								
		select  nvl(trg.BANKRUPT_RTK_RESPONSIBILITY_GID, -999) as INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY_GID
				,a.id 
				,BANKRUPT_RTK_GID
				,INSURER_RESPONSIBILITY_AMOUNT_GID
				,DECISION_SUM
				,CAST(case when trg.start_date >= a.first_sd then 1 else 0  end as Integer) as  IS_DISABLED -- pometka na udalenie --new3
				,trg.start_date
				
				,2 as part 
    			,trg.uid,
				a.entity_code,
				a.src_code
				
	-- SELECT *
   from 
    (
				select distinct cast (RTK_RESPONSIBILITY_ID as nvarchar(1024)) as id 
				,cast(950 as smallint) as entity_code
				,cast(AUD_USER as smallint) as SRC_CODE --v3.0
				,min(AUD_TIME)  over (partition by  src_code, RTK_RESPONSIBILITY_ID ) as first_sd --new3
		-- SELECT *
       from  UTS_SRC001.UTS_SRC001_USR.ASV_BANKRUPT_RTK_RESPONSIBILITY SRC 
          where LOAD_DATE >=  to_timestamp( '10-01-2005  12:00:00', 'dd.mm.yyyy hh24:mi:ss') 
            and LOAD_DATE <= to_timestamp( '10-01-2025  12:00:00', 'dd.mm.yyyy hh24:mi:ss')
            and AUD_TYPE in ('UP', 'DL', 'PT', 'RR')
       		 and cast(AUD_USER as smallint) between 129 and 300
			    ) A
 inner join  UTS_DWH.UTS_DWH_USR.M_INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY m_trg
  on m_trg.sid = a.id and m_trg.src_code = a.src_code and  m_trg.entity_code = a.entity_code
 inner join UTS_DWH.UTS_DWH_USR.INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY trg
  on trg.BANKRUPT_RTK_RESPONSIBILITY_GID = m_trg.INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY_GID
    --and trg.end_date >= to_date('#P_START_DATE#', 'dd.mm.yyyy hh24:mi:ss')  - 1
	and trg.END_DATE >= a.first_sd -1
  
			 
--===========================================================================================================================					 
 ) rr_r
   ) rr
    where rr.rnk= 1
--===========================================================================================================================
--===========================================================================================================================
--===========================================================================================================================
--===========================================================================================================================
--===========================================================================================================================
	 ) rr_g
  ) rr_g1 -- v3.0
  --group by  rr_g.legal_form_gid,  id, code, name
where is_disabled = 0   or ( is_disabled = 1 and new_uid <> -999 )
  ) rr_2

where 
-- ?? ????? ????????? ??? ?????? (uid=-999) , ??? ??????? ???? ?????? ?????? (maz_uid <> -999 ) ??? ?????? sd > ed
not (uid = -999 and (max_uid <> -999 or new_sd > new_ed))
) rr_3
  --  ??????, ???????? ?? ?????? 
  left outer join UTS_DWH.UTS_DWH_USR.INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY trg
   on trg.uid = rr_3.uid
where    trg.uid is null or IS_DEL = 1
   or nvl(trg.end_date, to_date('01.01.1900 00:00:00', 'dd.mm.yyyy hh24:mi:ss')) <> rr_3.end_date
or  nvl(rr_3.INSURER_PENALTY_BANKRUPT_RTK_GID  , -999) <> nvl(trg.BANKRUPT_RTK_RESPONSIBILITY_GID, -999)
or  nvl(rr_3.INSURER_RESPONSIBILITY_AMOUNT_GID  , -999) <> nvl(trg.INSURER_RESPONSIBILITY_AMOUNT_GID, -999)
or  nvl(rr_3.DECISION_SUM, -999) <> nvl(trg.DECISION_SUM, -999)

)

select 
uid
  , INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY_GID as BANKRUPT_RTK_RESPONSIBILITY_GID
  , INSURER_PENALTY_BANKRUPT_RTK_GID, INSURER_RESPONSIBILITY_AMOUNT_GID, DECISION_SUM
,id,entity_code,src_code,start_date,end_date,is_DEL,check_flg,field1-- , to_timestamp('#p_Update_date#', 'dd.mm.yyyy hh24:mi:ss') as update_date

from SRC_TOTAL
where uid <> -999
   
union all
   
select --next value for SQ_INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY as 
uid 
  , INSURER_PENALTY_BANKRUPT_RTK_RESPONSIBILITY_GID as BANKRUPT_RTK_RESPONSIBILITY_GID
  , INSURER_PENALTY_BANKRUPT_RTK_GID as BANKRUPT_RTK_GID
  , INSURER_RESPONSIBILITY_AMOUNT_GID, DECISION_SUM
,id,entity_code,src_code,start_date,end_date,is_DEL,check_flg,field1-- , to_timestamp('#p_Update_date#', 'dd.mm.yyyy hh24:mi:ss') as update_date

From SRC_TOTAL
where uid = -999 


