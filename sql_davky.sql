--pridani sloupce geom
SELECT AddGeometryColumn ('sourad','geom',5514,'POINT',2);

--prevedeni souradnic na format double precision
ALTER TABLE uzpr21_b.sourad 
    ALTER x_jtsk TYPE double precision USING x_jtsk::double precision;

ALTER TABLE uzpr21_b.sourad 
    ALTER y_jtsk TYPE double precision USING y_jtsk::double precision;

--vytvoreni geometrie
UPDATE sourad SET geom = ST_SetSRID(ST_MakePoint(-y_jtsk, -x_jtsk), 5514);

--validace dat
SELECT st_isvalid(sourad.geom)
                FROM sourad 
                WHERE st_isvalid(sourad.geom) IS NOT TRUE;

--pocet jednotlivych nalezu podle typu
SELECT specif, count(specif)
FROM nalezy
GROUP by specif 
HAVING count(specif) > 10
ORDER BY count(specif) DESC;

--pocet nalezu v pasech od vodnich toku
SELECT count(*)                                  
FROM   sourad as targetFeautre                       
JOIN   vodnitoky as bufferFeautre                       
ON     targetFeautre.geom @ bufferFeautre.geom  
AND    st_within(targetFeautre.geom, st_buffer(bufferFeautre.geom, 100));

--pocet nalezu v pasech od sidel
SELECT count(*)                                  
FROM   sourad as targetFeautre                       
JOIN   sidlaplochy as bufferFeautre                       
ON     targetFeautre.geom @ bufferFeautre.geom  
AND    st_within(targetFeautre.geom, st_buffer(bufferFeautre.geom, 100));

--pocet nalezu v maloplosnych chranenych oblastech
SELECT count(*)
FROM sourad as s
JOIN aopk.maloplosna_chranena_uzemi as u
ON s.geom @ u.geom
AND st_within(s.geom, u.geom);

--pocet nalezu v velkoplosnych chranenych oblastech
SELECT count(*)
FROM sourad as s
JOIN aopk.velkoplosna_chranena_uzemi as u
ON s.geom @ u.geom
AND st_within(s.geom, u.geom);

--pocet nalezu ve velkoplosnych a malopolosnych chranenych oblastech zaroven
SELECT count(*) 
FROM (
      SELECT s.geom
      FROM sourad as s
      JOIN aopk.maloplosna_chranena_uzemi as u
      ON s.geom @ u.geom
      AND st_within(s.geom, u.geom)
) as s
JOIN aopk.velkoplosna_chranena_uzemi as u
ON s.geom @ u.geom
AND st_within(s.geom, u.geom)

--pocty nalezu podle kraju
SELECT k.text, count(*)
                FROM sourad as s
                FULL JOIN inspire_au as k
                ON s.geom @ k.geom
                AND st_within(s.geom, k.geom)
                WHERE k.localisedcharacterstring = 'Kraj'
                GROUP BY k.text;

--pocty nalezist podle obci
SELECT k.text, count(*)
                FROM sourad as s
                FULL JOIN inspire_au as k
                ON s.geom @ k.geom
                AND st_within(s.geom, k.geom)
                WHERE k.localisedcharacterstring = 'Obec'
                GROUP BY k.text
                ORDER BY count(s.geom) DESC
                LIMIT 20;

--pocty nalezu podle casove epochy
SELECT field_2, count(*)
                FROM komponen as k
                JOIN doby1 as d
                ON k.kult =  d.kult
                GROUP BY field_2
                ORDER BY count(*) DESC;

--pocet nalezu podle spravnich hranic prahy
SELECT o.nazev, count(*)
                FROM sourad  as s
                JOIN ruian_praha.spravniobvody as o
                ON s.geom @ o.geom
                AND st_within(s.geom, o.geom)
                GROUP BY o.nazev
                ORDER BY count(*) DESC;












