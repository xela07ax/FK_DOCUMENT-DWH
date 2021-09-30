
/*
  1111111             1111111            222222222222222                ,----,
 1::::::1            1::::::1           2:::::::::::::::22            .'   .' \
1:::::::1           1:::::::1           2::::::222222:::::2         ,----,'    |
111:::::1           111:::::1           2222222     2:::::2         |    :  .  ;
   1::::1              1::::1                       2:::::2         ;    |.'  /
   1::::1              1::::1                       2:::::2         `----'/  ;
   1::::1              1::::1                    2222::::2            /  ;  /
   1::::l              1::::l               22222::::::22            ;  /  /-,
   1::::l              1::::l             22::::::::222             /  /  /.`|
   1::::l              1::::l            2:::::22222          ___ ./__;      :
   1::::l              1::::l           2:::::2             /  .\|   :    .'
   1::::l              1::::l           2:::::2             \  ; ;   | .'
111::::::111        111::::::111        2:::::2       222222 `--"`---'
1::::::::::1 ...... 1::::::::::1 ...... 2::::::2222222:::::2
1::::::::::1 .::::. 1::::::::::1 .::::. 2::::::::::::::::::2
111111111111 ...... 111111111111 ...... 22222222222222222222

RL  L.ST L.ST
L   R.ED+1 L.ED
 */

--Инициализация таблиц
drop table TMP_TABLE_1122 if exists;
create table TMP_TABLE_1122 as
  (
    --1. Мы собираем все данные с правой таблицы, которые полностью входят между ST и ED левой
    SELECT R.FK_DOCUMENT_GID, L.START_DATE as L_ST,  L.END_DATE as L_ED
      , 1 as NEW2_TABLES_L, 1 as NEW2_TABLES_R
      , L.START_DATE as NEW2_ST, L.START_DATE as NEW2_ED
      , 1 as NEW3_TABLES_L, 0 as NEW3_TABLES_R
      , CAST (L.START_DATE::DATE + cast('1 days' as interval) as DATE) as NEW3_ST, L.END_DATE as NEW3_ED
    --select *
    from  TMP_TABLE_L L
      INNER JOIN   --select * from
      TMP_TABLE_R R
        ON L.FK_DOCUMENT_GID = R.FK_DOCUMENT_GID
           AND R.START_DATE < L.START_DATE
           AND L.START_DATE < R.END_DATE
           AND R.END_DATE <  L.END_DATE
  );


drop table TMP_TABLE_1122_FIN if exists;
create table TMP_TABLE_1122_FIN as
  (
    SELECT FK_DOCUMENT_GID
      , NEW2_TABLES_L as TABLES_L, NEW2_TABLES_R as TABLES_R
      , NEW2_ST as START_DATE, NEW2_ED as END_DATE
      , L_ST OLD_LST , L_ED OLD_LED
      , 1 part
    FROM  TMP_TABLE_1122
    UNION
    SELECT FK_DOCUMENT_GID
      , NEW3_TABLES_L as TABLES_L, NEW3_TABLES_R as TABLES_R
      , NEW3_ST as START_DATE, NEW3_ED as END_DATE
      , L_ST OLD_LST , L_ED OLD_LED
      , 2 part
    FROM  TMP_TABLE_1122
  );
--select * from TMP_TABLE_1122_FIN;
