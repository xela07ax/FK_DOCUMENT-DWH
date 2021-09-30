
/*
  1111111             1111111            222222222222222
 1::::::1            1::::::1           2:::::::::::::::22
1:::::::1           1:::::::1           2::::::222222:::::2      .--,-``-.        .--,-``-.
111:::::1           111:::::1           2222222     2:::::2     /   /     '.     /   /     '.       ,---,
   1::::1              1::::1                       2:::::2    / ../        ;   / ../        ;   ,`--.' |
   1::::1              1::::1                       2:::::2    \ ``\  .`-    '  \ ``\  .`-    ' /    /  :
   1::::1              1::::1                    2222::::2      \___\/   \   :   \___\/   \   ::    |.' '
   1::::l              1::::l               22222::::::22            \   :   |        \   :   |`----':  |
   1::::l              1::::l             22::::::::222              /  /   /         /  /   /    '   ' ;
   1::::l              1::::l            2:::::22222                 \  \   \         \  \   \    |   | |
   1::::l              1::::l           2:::::2                  ___ /   :   |    ___ /   :   |   '   : ;
   1::::l              1::::l           2:::::2                 /   /\   /   :   /   /\   /   :   |   | '
111::::::111        111::::::111        2:::::2       222222   / ,,/  ',-    ___/ ,,/  ',-    ___ '   : |
1::::::::::1 ...... 1::::::::::1 ...... 2::::::2222222:::::2   \ ''\        /  .\ ''\        /  .\;   |.'
1::::::::::1 .::::. 1::::::::::1 .::::. 2::::::::::::::::::2    \   \     .'\  ; \   \     .'\  ; '---'
111111111111 ...... 111111111111 ...... 22222222222222222222     `--`-,,-'   `--" `--`-,,-'   `--"




RNK > 1 AND MAX_RNK <> RNK

LR   R.ST R.ED
L  R.ED+1 lead(R.ST-1)

 */


drop table TMP_TABLE_112331_PRE if exists;
create table TMP_TABLE_112331_PRE as
  (
    SELECT FK_DOCUMENT_GID, L_ST OLD_LST, L_ED OLD_LED
      , NEW2_TABLES_L,NEW2_TABLES_R
      , NEW2_ST, NEW2_ED
    --select *
    FROM (
           SELECT FK_DOCUMENT_GID, L_ST, L_ED, R_ST, R_ED, RNK,MAX_RNK
             ,CAST (lag(R_ED) over (partition by FK_DOCUMENT_GID, L_ST order by R_ST) + cast('1 days' as interval) as DATE) as LAG_ED
             , CAST (lead(R_ST) over (partition by FK_DOCUMENT_GID, L_ST order by R_ST) - cast('1 days' as interval) as DATE) as LEAD_ST
             , 1 as NEW2_TABLES_L, 1 as NEW2_TABLES_R
             , R_ST as NEW2_ST, R_ST as NEW2_ED
           --select * ,CAST (lag(R_ED) over (partition by FK_DOCUMENT_GID, L_ST order by R_ST) + cast('1 days' as interval) as DATE) as LAG_ED, CAST (lead(R_ST) over (partition by FK_DOCUMENT_GID, L_ST order by R_ST) - cast('1 days' as interval) as DATE) as LEAD_ST
           FROM TMP_TABLE_1123L_JOIN_R
           WHERE MAX_RNK  > 1
         ) LR
    WHERE RNK <> MAX_RNK
    AND RNK > 1
    AND LEAD_ST = R_ST
    AND LAG_ED = R_ED
  );


drop table TMP_TABLE_112331_FIN if exists;
create table TMP_TABLE_112331_FIN as
  (

    SELECT FK_DOCUMENT_GID
      , NEW2_TABLES_L as TABLES_L, NEW2_TABLES_R as TABLES_R
      , NEW2_ST as START_DATE, NEW2_ED as END_DATE
      , OLD_LST , OLD_LED
      , 1 part
    FROM  TMP_TABLE_112331_PRE

  );
