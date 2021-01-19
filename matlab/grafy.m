clear,clc
format long g

%% vodni toky

 rozmezi={'0m -   100m',  
 '100m -   200m', 
 '200m -   300m',  
 '300m -   400m', 
 '400m -   500m',  
 '500m -   600m', 
 '600m -   700m',  
 '700m -   800m',  
 '800m -   900m', 
 '900m - 1000m' } ;

X=categorical(rozmezi);
X=reordercats(X,rozmezi);

pocet=[21697
        24375
        18982
         15153
         10554
          8488
          6890
          6039
           5544
           4620];
       
figure(1)
bar(X,pocet)
title('Pocet nalezu v intervalech od vodnich toku')
ylabel('Pocet nalezu')
xlabel('Rozmezi')

%% sidla

pocet=[43003
1559
1043
736
465
387
391
277
214
205];

figure(2)
bar(X,pocet)
title('Pocet nalezu v intervalech od sidel')
ylabel('Pocet nalezu')
xlabel('Rozmezi')


%% nalezy

rozmezi={'keramika' 
'kosti zv.' 
'kámen' 
'železo' 
'bronz' 
'kámen-ši' 
'mazanice' 
'kosti lid.' 
'kámen-bn' 
'sklo' 
'uhlíky' 
'hlína' 
'kost' 
'kov' 
'dřevo' 
'struska' 
'kámen-dž' 
'stříbro' 
'kámen stav.' 
'ker. staveb.' 
'měď' 
'švartna' 
'paleobot. m.' 
'cihla' 
'zlato' 
'malta' 
'jantar' 
'kamenina'
'paroh' 
'kámen-stav.' 
'kůže' }
pocet=[58657
6452
6328
6014
5307
4352
4070
3057
2794
2608
2186
1703
1579
1448
1330
1226
1007
648
545
399
340
317
304
295
282
272
257
253
248
246
206];

X=categorical(rozmezi);
X=reordercats(X,rozmezi);

figure(3)
bar(X,pocet)
title('Pocet nalezu podle druhu')
ylabel('Pocet nalezu')
xlabel('Druh')