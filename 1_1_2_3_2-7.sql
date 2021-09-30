

/*
  1111111             1111111            222222222222222
 1::::::1            1::::::1           2:::::::::::::::22     .--,-``-.
1:::::::1           1:::::::1           2::::::222222:::::2   /   /     '.           ,----,
111:::::1           111:::::1           2222222     2:::::2  / ../        ;        .'   .' \
   1::::1              1::::1                       2:::::2  \ ``\  .`-    '     ,----,'    |
   1::::1              1::::1                       2:::::2   \___\/   \   :     |    :  .  ;
   1::::1              1::::1                    2222::::2         \   :   |     ;    |.'  /
   1::::l              1::::l               22222::::::22          /  /   /      `----'/  ;
   1::::l              1::::l             22::::::::222            \  \   \        /  ;  /
   1::::l              1::::l            2:::::22222           ___ /   :   |      ;  /  /-,
   1::::l              1::::l           2:::::2               /   /\   /   :     /  /  /.`|
   1::::l              1::::l           2:::::2              / ,,/  ',-    ___ ./__;      :
111::::::111        111::::::111        2:::::2       222222 \ ''\        /  .\|   :    .'
1::::::::::1 ...... 1::::::::::1 ...... 2::::::2222222:::::2  \   \     .'\  ; ;   | .'
1::::::::::1 .::::. 1::::::::::1 .::::. 2::::::::::::::::::2   `--`-,,-'   `--"`---'
111111111111 ...... 111111111111 ...... 22222222222222222222

 RNK = 1 AND MAX_RNK <> 1

L  L.ST R.ST-1
LR   R.ST R.ED
L  R.ED+1 lead(L.ST-1)
 */

drop table TMP_TABLE_11232_PRE if exists;
create table TMP_TABLE_11232_PRE as
  (
    SELECT FK_DOCUMENT_GID, L_ST OLD_LST, L_ED OLD_LED
      , NEW1_TABLES_L, NEW1_TABLES_R
      , NEW1_ST, NEW1_ED
      , NEW2_TABLES_L,NEW2_TABLES_R
      , NEW2_ST, NEW2_ED
      , NEW3_TABLES_L, NEW3_TABLES_R
      ,  NEW3_ST, CAST(NEW3_ED::DATE - cast('1 days' as interval) as DATE) as NEW3_ED
    FROM (
           SELECT FK_DOCUMENT_GID, L_ST, L_ED,RNK
             , 1 as NEW1_TABLES_L, 0 as NEW1_TABLES_R
             , L_ST as NEW1_ST, CAST (R_ST - cast('1 days' as interval) as DATE) as NEW1_ED
             , 1 as NEW2_TABLES_L, 1 as NEW2_TABLES_R
             , R_ST as NEW2_ST, R_ED as NEW2_ED
             , 1 as NEW3_TABLES_L, 0 as NEW3_TABLES_R
             , CAST (R_ED + cast('1 days' as interval) as DATE) as NEW3_ST, lead(R_ST) over (partition by FK_DOCUMENT_GID order by RNK) as NEW3_ED
           --select *  ,lead(R_ST) over (partition by FK_DOCUMENT_GID order by L_ST, L_ED)
           FROM TMP_TABLE_1123L_JOIN_R
           WHERE RNK IN (1,2) AND MAX_RNK > 1
         ) LR
    WHERE RNK = 1
  );



drop table TMP_TABLE_11232_FIN if exists;
create table TMP_TABLE_11232_FIN as
  (
    SELECT FK_DOCUMENT_GID
      , NEW1_TABLES_L as TABLES_L, NEW1_TABLES_R as TABLES_R
      , NEW1_ST as START_DATE, NEW1_ED as END_DATE
      , OLD_LST , OLD_LED
      , 1 part
    FROM  TMP_TABLE_11232_PRE

    UNION

    SELECT FK_DOCUMENT_GID
      , NEW2_TABLES_L as TABLES_L, NEW2_TABLES_R as TABLES_R
      , NEW2_ST as START_DATE, NEW2_ED as END_DATE
      , OLD_LST , OLD_LED
      , 2 part
    FROM  TMP_TABLE_11232_PRE

    UNION

    SELECT FK_DOCUMENT_GID
      , NEW3_TABLES_L as TABLES_L, NEW3_TABLES_R as TABLES_R
      , NEW3_ST as START_DATE, NEW3_ED as END_DATE
      , OLD_LST , OLD_LED
      , 3 part
    FROM  TMP_TABLE_11232_PRE
  );
--select * from TMP_TABLE_11232_FIN;
 
