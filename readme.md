# Three2One  
將多個輸入的 shapefile 依照指定的欄位合併輸出成個別的 shapefile 檔案。

## 環境  
必要安裝：
* gdal
* pandas
  
## 使用  
`usage: Three2One.py [-h] -i I [I ...] -n N [N ...] -e E`
-i：輸入的 shapefile 路徑。
-n：合併使用的欄位。
-e：輸出的資料夾。
  
範例：
`python Three2One.py -i './TestFCs/Line1.shp' './TestFCs/Line2.shp' './TestFCs/Line3.shp' -n SID1 SID2 SID3 -e './TestFCs/TestExport'`