--Postgresql

--Question 1

/*The query will return the top  5 properties ID by amounts received from the properties during the last 3 months(relation to today), 
if a rent started before the period and continued into the period the days will calculated relatively to the beginning of the period */
select PropertyId
from (
  select T1.PropertyId, ROW_NUMBER() OVER(PARTITION BY T1.PropertyId ORDER BY 
                                          sum(CASE WHEN date_part('month',now ())-date_part('month',FromDate) <=3 Then ToDate-FromDate
                                               ELSE (ToDate::date -(CURRENT_DATE - INTERVAL '3 months')::date) END) *max(PricePerNight) DESC)  as row_num 
  from (select PropertyId ,TO_DATE(ToDate,'DD/MM/YYYY') as ToDate  ,TO_DATE(FromDate, 'DD/MM/YYYY') as FromDate from Rentals) T1 
  inner join 
  Properties T2
  on T1.PropertyId=T2.PropertyId
  where ToDate-(CURRENT_DATE - INTERVAL '3 months')::date>=0
  group by T1.PropertyId) T3
  where row_num<=5;
  
  
--Question 2        

/*The query will return those neighborhoods where properties not rented during the past week,propertiesthe count ,and the amount of money lost during this week*/
select NeighbourhoodName,count (1) as number_of_properties ,sum(pricepernight)*7 as total_amount 
from Properties T1 inner join Neighbourhoods T2
on T1.NeighbourhoodId=T2.NeighbourhoodId
where PropertyId not in 
  (select PropertyId
  from Rentals
  where TO_DATE(ToDate,'DD/MM/YYYY')::date between (CURRENT_DATE - INTERVAL '2 week')::date AND  (CURRENT_DATE - INTERVAL '1 week')::date)
  group by NeighbourhoodName