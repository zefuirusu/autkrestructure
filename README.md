# autkrestructure
Restructure for repository AUTK( version 4; python;).
## Looking back
version|description
---|---
1.0|Simply load `xlsx` book and analysis.
2.0|manage worksheet by `mgl`.
3.0|structure of `mgl` is determined by `xlmap`.
4.0|four key modules:meta,map,table,bookbrother

## Core module structure

```mermaid
graph LR
m0[AUTK]
m1[mapper]
m1d[(structure of table)]
m11[/xlmap/]
m12[/MglMap/]
m13[/ChartMap/]
m2[meta]
m2d[(standard show of <br>input file-structure)]
m21[/from_dir/]
m22[/from_list/]
m23[/from_json/]
m3[calculation]
m3d[(to iterate DataFrame)]
m31[/xlsht/]
m32[/table/]
m33[/superTable/]
m34[/calsht/]
m35[/mortal_gl/]
m36[/super_gl/]
m4[book_brother]
m4d[(manipulate Excel)]
m41[/single_book/]
m42[/bibasic_book/]
m0---m1
m1---m1d
m1---m11
m1---m12
m1---m13
m0---m2
m2---m2d
m2---m21
m2---m22
m2---m23
m0---m3
m3---m3d
m3---m31
m31--->m34
m3---m32
m32--->m35
m3---m33
m33--->m36
m0---m4
m4---m4d
m4---m41
m4---m42
```
## Module-relationship

```mermaid
erDiagram
DataFrame{}
ExcelWorkbook{}
mapper{}
meta{}
calculation{}
book_brother{}
mapper ||--o{ calculation:define_data_structure
meta ||--o{ ExcelWorkbook:define_data_source
calculation ||--o{ DataFrame:iterate_and_calculate
book_brother ||--o{ ExcelWorkbook:copy_paste_etc
ExcelWorkbook ||--o{ calculation:provide_data
```

## Class design

```mermaid
classDiagram
direction TD
%% 省略所有方法的self参数
%% 语法格式为python/rust
class DataFrame
class SingleBookBrother{
	+string file_path
	-string suffix
	-parse_file_type()
	+get_sht()
	+get_matrix()
	+paste_matrix()
	+fill_by_df()
	+DataFrame test_map(XlMap xlmap)
}
class BibasicBookBrother{
	+string left
	+string right
	+void cp_by_df(DataFrame)
}
class Meta{
	+dict show
	-parse_path()
	-parse_dir()
	-parse_list()
	-parse_matrix()
	-parse_json()
}
class XlMap{
	+dict show()
	+from_list()
	+from_json()
}
class MglMap
class ChartMap
class InventoryMap
class XlSht{
	-list row_temp_data
	+DataFrame data
	-void init(Meta xlmeta,XlMap xlmap)
	+load_raw_data()
	+clear()
	+apply_df_func()
	+change_dtype()
	+filter()
	+vlookups()
	+sumifs()
}
class GlSht{
	+getjr()
	+getitem()
	+getAcct()
	+acct_analysis(bool single)
	+correspond()
	+side_split()
	+rand_sample()
}
class ChSht
class InventorySht
class Table{
	-list row_temp_data
	+list xlset
	+DataFrame data
	-void init(Meta xlmeta,XlMap xlmap)
	+append_xl_by_meta()
	+check_xl_cols()
	+load_raw_data()
	+reload()
	+clear_temp()
	+xlcolscan()
	+apply_df_func()
	+apply_xl_func()
	+filter()
	+vlookups()
	+sumifs()
	+change_dtype()
}
class MGL{
	+getjr()
	+getitem()
	+getAcct()
	+acct_analysis(bool single)
	+correspond()
	+side_split()
	+find_opposite()
	+save_cash_gl()
	+rand_sample()
}
class MCA
class MInv

DataFrame --*XlSht:composition
SingleBookBrother ..> BibasicBookBrother:dependency
Meta ..> XlSht:dependency
Meta ..> SingleBookBrother:dependency

XlMap --|> MglMap:inheritance
XlMap --|> ChartMap:inheritance
XlMap --|> InventoryMap:inheritance

XlMap ..> XlSht:dependency
XlSht --o Table:aggregation

MglMap ..> GlSht:dependency
GlSht --o MGL:aggregation

ChartMap ..> ChSht:dependency
ChSht --o MCA:aggregation

InventorySht --o MInv:aggregation
InventoryMap ..> MInv:dependency

XlSht --|> GlSht:inheritance
XlSht --|> ChSht:inheritance
XlSht --|> InventorySht:inheritance

Table --|> MGL:inheritance
Table --|> MCA:inheritance
Table --|> MInv:inheritance

```
