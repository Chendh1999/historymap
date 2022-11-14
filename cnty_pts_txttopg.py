# coding=gbk
import psycopg2

conn = psycopg2.connect(
    database="mapdata",
    user="postgres",
    password="123456",
    host="127.0.0.1",
    port="5432")
print('连接成功')
# 建立游标
cur = conn.cursor()
f=open(r'E:\develop_gis\map website\data\V6 Time Series County Points\v6_time_cnty_pts_utf_wgs84\v6_time_cnty_pts_utf_wgs84.txt','r',encoding='utf-8')
# 按行读入txt
flines=f.readlines()
for line in flines:
    # 去掉干扰，切词
    abbrlist=line.replace("'"," ").split('\t')
    # name_py,name_ch,name_ft,x_coor,y_coor,pres_loc,type_py,type_ch,lev_rank,beg_yr,beg_rule,end_yr,end_rule,note_id,obj_type,sys_id,geo_src,compiler,gecomplr,checker,ent_date,beg_chg_ty,end_chg_ty,geom
    name_py='null'
    if (abbrlist[0]!=''):
        name_py="'"+abbrlist[0]+"'"
    name_ch='null'
    if (abbrlist[1]!=''):
        name_ch="'"+abbrlist[1]+"'"
    name_ft='null'
    if (abbrlist[2]!=''):
        name_ft="'"+abbrlist[2]+"'"
    x_coor='null'
    if (abbrlist[3]!=''):
        x_coor=abbrlist[3]
    y_coor='null'
    if (abbrlist[4]!=''):
        y_coor=abbrlist[4]
    pres_loc='null'
    if (abbrlist[5]!=''):
        pres_loc="'"+abbrlist[5]+"'"
    type_py='null'
    if (abbrlist[6]!=''):
        type_py="'"+abbrlist[6]+"'"
    type_ch='null'
    if (abbrlist[7]!=''):
        type_ch="'"+abbrlist[7]+"'"
    lev_rank='null'
    if (abbrlist[8]!=''):
        lev_rank="'"+abbrlist[8]+"'"
    beg_yr='null'
    if (abbrlist[9]!=''):
        beg_yr=abbrlist[9]
    beg_rule='null'
    if (abbrlist[10]!=''):
        beg_rule="'"+abbrlist[10]+"'"
    end_yr='null'
    if (abbrlist[11]!=''):
        end_yr=abbrlist[11]
    end_rule='null'
    if (abbrlist[12]!=''):
        end_rule="'"+abbrlist[12]+"'"
    note_id='null'
    if (abbrlist[13]!=''):
        note_id=abbrlist[13]
    obj_type='null'
    if (abbrlist[14]!=''):
        obj_type="'"+abbrlist[14]+"'"
    sys_id='null'
    if (abbrlist[15]!=''):
        sys_id=abbrlist[15]
    geo_src='null'
    if (abbrlist[16]!=''):
        geo_src="'"+abbrlist[16]+"'"
    compiler='null'
    if (abbrlist[17]!=''):
        compiler="'"+abbrlist[17]+"'"
    gecomplr='null'
    if (abbrlist[18]!=''):
        gecomplr="'"+abbrlist[18]+"'"
    checker='null'
    if (abbrlist[19]!=''):
        checker="'"+abbrlist[19]+"'"
    ent_date='null'
    if (abbrlist[20]!=''):
        ent_date="'"+abbrlist[20]+"'"
    beg_chg_ty='null'
    if (abbrlist[21]!=''):
        beg_chg_ty="'"+abbrlist[21]+"'"
    end_chg_ty='null'
    if (abbrlist[22]!=''):
        end_chg_ty="'"+abbrlist[22]+"'"
    geom='null'
    if (abbrlist[23]!=''):
        geom="st_geomfromtext('"+abbrlist[23]+"',4326)"
    # 拼接sql语句
    sqltxt="INSERT INTO v6_time_cnty_pts_utf_wgs84(" \
           "name_py,name_ch,name_ft,x_coor,y_coor,pres_loc,type_py,type_ch,lev_rank,beg_yr,beg_rule," \
           "end_yr,end_rule,note_id,obj_type,sys_id,geo_src,compiler,gecomplr,checker,ent_date," \
           "beg_chg_ty,end_chg_ty,geom) VALUES("+name_py+","+name_ch+","+name_ft+","+x_coor+","+y_coor+","\
           +pres_loc+","+type_py+","+type_ch+","+lev_rank+","+beg_yr+","+beg_rule+","+end_yr+","+end_rule+","\
           +note_id+","+obj_type+","+sys_id+","+geo_src+","+compiler+","+gecomplr+","+checker+","+ent_date+","\
           +beg_chg_ty+","+end_chg_ty+","+geom+")"
    print(sqltxt)
    # 执行sql
    cur.execute(sqltxt)

# 关闭连接
conn.commit()
conn.close()
print('插入完成')
