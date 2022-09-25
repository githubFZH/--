
# coding: utf-8

# In[ ]:


import os
import numpy as np
import pandas as pd
import re
from prettytable import PrettyTable
import hashlib
import warnings
warnings.filterwarnings("ignore")

db_path = 'Data/'
user=''
index_list=[['stusno','student',['sno']],['is_sno','student',['sno']]]
table_list=['sc','student']




def welcome():
    print("""
          ##############################################
          
                     
              _          _____  ____  __
             |  __ \|  _ \|  \/  |/ ____|
             | |  | | |_) | \  / | (___  
             | |  | |  _ <| |\/| |\___ \ 
             | |__| | |_) | |  | |____) |
             |_____/|____/|_|  |_|_____/ 
                                                    
                    -> exit:退出 help:语法帮助 <-

          ##############################################
          """)

def help(command):
    command_word=command.split(' ')
    if command_word[1]=='database':   
        print('数据表——————————————')
        for table_name in table_list:

            print(table_name+':')
            db=pd.read_csv('Data/'+table_name+'.csv',index_col=0)
            df=db.loc['type']
            print(df)
        print('索引———————————————')
        for i in range(len(index_list)):
            print('索引名：'+index_list[i][0])
            print('索引表名：'+index_list[i][1])
            print('索引字段：'+str(index_list[i][2]))
        print('视图———————————————')
        db=pd.read_csv('Data/'+'view'+'.csv',index_col=0)
        print(db)
    elif command_word[1]=='table':
        table_name=command_word[2]
        print('数据表——————————————')


        print(table_name+':')
        db=pd.read_csv('Data/'+table_name+'.csv',index_col=0)
        df=db.loc[['type','primary key','unique','not null','foreign key','check']]
        print(df)
    elif command_word[1]=='view':
        view_name=command_word[2]
        db=pd.read_csv('Data/'+'view'+'.csv',index_col=0)
        print(db[db['view_name']==view_name])
    elif command_word[1]=='index':
        index_name=command_word[2]
        for i in range(len(index_list)):
            if index_name in index_list[i]:
                print('索引名：'+index_list[i][0])
                print('索引表名：'+index_list[i][1])
                print('索引字段：'+str(index_list[i][2]))

def get_command():
    command = input("[]> ") if not user else input("[{}]> ".format(user))
        
    return command.strip()

def login():
    global user
    print ("Please Login:")
    username = input("username: ")
    df=pd.read_csv('Data/user.csv',index_col=0,header=0)
    if username in df['user'].values:
        print ("Login Success!Welcome {}! ".format(username))
        user=username
    else:
        a=[[username,username,username,username,username,username,username,username]]
        db=pd.DataFrame(a,columns={'user':'','create':'','select':'','update':'','delete':'','insert':'','grant':'','revoke':''},index=['tuple'])

        df=pd.concat([df,db],axis=0)
        df.to_csv('Data/user.csv')

        print ("Login Success!Welcome {}! ".format(username))
        user = username
   
def run():
    
    welcome()
    login()
    i=1
    while i:
        command = get_command()
        #print command
        if command == 'quit' or command == 'exit':
            print(" Thanks for using L-DBMS. Bye~~")
            i=0
        elif 'help'in command:
            help(command)
        elif command=='exit user':
            user=''
            login()
        else:
            query(command)
        
def creat_table(table_name,columns_list):
    try:
        db=pd.read_csv('Data/'+table_name+'.csv')
        print("表已经存在！请重新建表。")
    except:
        df = pd.DataFrame()
        length = len(columns_list)
        column_names = []
        df_all=pd.DataFrame()
        for i in range(length):
            column = columns_list[i].split(' ')
            if len(column)<=2:
                a=[column[1],'0','0','0','0','无']
                df=pd.DataFrame(a,columns={column[0]:''},index=['type','primary key','unique','not null','foreign key','check'])
                df_all=pd.concat([df_all,df],axis=1)
            else:
                if column[2]=='primary':
                    a=[column[1],'1','1','1','0','无']
                    df=pd.DataFrame(a,columns={column[0]:''},index=['type','primary key','unique','not null','foreign key','check'])
                    df_all=pd.concat([df_all,df],axis=1)
                elif column[2]=='unique':
                    a=[column[1],'0','1','0','0','无']
                    df=pd.DataFrame(a,columns={column[0]:''},index=['type','primary key','unique','not null','foreign key','check'])
                    df_all=pd.concat([df_all,df],axis=1)
                elif column[2]=='not':
                    a=[column[1],'0','0','1','0','无']
                    df=pd.DataFrame(a,columns={column[0]:''},index=['type','primary key','unique','not null','foreign key','check'])
                    df_all=pd.concat([df_all,df],axis=1)
                elif column[2]=='foreign':
                    a=[column[1],'0','0','0','1','无']
                    df=pd.DataFrame(a,columns={column[0]:''},index=['type','primary key','unique','not null','foreign key','check'])
                    df_all=pd.concat([df_all,df],axis=1)
                elif column[2]=='cherk':
                    a=[column[1],'0','0','0','1',column[3]]
                    df=pd.DataFrame(a,columns={column[0]:''},index=['type','primary key','unique','not null','foreign key','check'])
                    df_all=pd.concat([df_all,df],axis=1)
        df_all.to_csv(db_path+table_name+'.csv')
        table_list.append(table_name)
        print('建表操作成功')
    
def insert(table_name, columns_list={},select_sql=[]):
    db=pd.read_csv('Data/'+table_name+'.csv',index_col=0,header=0)
    l=len(columns_list)
    if l==0:
        a=' '.join(select_sql)
        sql_word=a.split(' ')
        if 'where' not in sql_word:

            table_name=sql_word[3]
            if sql_word[1]=='*':
                columns=['*']
            else:
                columns=sql_word[1].split(',')

            df=select(columns,table_name,'',tag='insert')
        else:#带子查询的插入
            where_sql=sql_word[5:]
            table_name=sql_word[3]
            if sql_word[1]=='*':
                columns=['*']
            else:
                columns=sql_word[1].split(',')

            df=select(columns,table_name,where_sql,tag='insert')
        counts=df.shape[0]
        df_all=pd.concat([df,db],axis=0)
    else:
        df_all=pd.DataFrame()


        for k in columns_list:
            a=[[columns_list[k]]]
            db1=pd.DataFrame(a,columns={k},index=['tuple'])
            df_all=pd.concat([df_all,db1],axis=1)
        df_all=pd.concat([df_all,db],axis=0)
        counts=1
    
    if (check_null(df_all))&(check_Constraint(df_all))&(check(df_all)):
        print('插入操作成功')
        print('一共插入%d个元组'%counts)
        df_all.to_csv(db_path+table_name+'.csv')

    else:
        return False
    for i in range(len(index_list)):
        if table_name in index_list[i]:
            creat_index(index_list[i][0],index_list[i][1],index_list[i][2])

def check(df):
    df=pd.read_csv('Data/sc.csv',index_col=0)
    a=list(df.columns)
    for i in a:
        if df.loc['check'][i]!='无':
            check=df.loc['check'][i]
            limit= re.findall('\((.*)\)', check)[0].split(' ')#属性
            print(limit)
            if '>' in limit[0]:
                col=limit[0].split('>')
                db=df.loc['tuple']            
                values=list(db[i])
                for i in range(len(values)):
                    if not values[i]>col[1]:
                        print('元组不满足check约束')
                        return False
            elif '<' in limit[0]:
                col=limit[0].split('<')
                db=df.loc['tuple']
                values=list(db[i])
                for i in range(len(values)):
                    if not values[i]<col[1]:
                        print('元组不满足check约束')
                        return False
            elif '=' in limit[0]:
                col=limit[0].split('=')
                db=df.loc['tuple']
                values=list(db[i])
                for i in range(len(values)):
                    if not values[i]==col[1]:
                        print('元组不满足check约束')
                        return False
        return True

def gather(columns,db):#聚集函数，影响select的输入，返回应该输入的属性列表
    columns_list=[]
    for i in range(len(columns)):
        if '(' in columns[i]:
            if 'count' in columns[i]:
                column= re.findall('\((.*)\)', columns[i])[0].split(',')#属性
                print(db.shape[0])
            elif 'sum' in columns[i]:
                column= re.findall('\((.*)\)', columns[i])[0].split(',')#属性
                column=column[0]
                values=list(db[column])
                All=0
                for i in values:
                    i=float(i)
                    All+=i
                print(All)
            elif 'avg' in columns[i]:
                column= re.findall('\((.*)\)', columns[i])[0].split(',')#属性
                column=column[0]
                values=list(db[column])
                l=len(values)
                All=0
                for i in values:
                    i=float(i)
                    All+=i
                print(All/l)
            elif 'max' in columns[i]:
                column= re.findall('\((.*)\)', columns[i])[0].split(',')#属性
                column=column[0]
                values=list(db[column])
                print(max(values))
            elif 'min' in columns[i]:
                column= re.findall('\((.*)\)', columns[i])[0].split(',')#属性
                column=column[0]
                values=list(db[column])
                print(min(values))
        else:
            columns_list.append(columns[i])
    return columns_list

def query(sql,tag=''):
    sql=sql.lower()
    sql_word = sql.split(" ")
    if sql=='':
        print("请不要输入空SQL语句！")
        return
    operate = sql_word[0]
    if operate == 'create':
        if not safe(sql_word[2],operate):
            return False
        if sql_word[1]=='table':
            columns_list = re.findall('\((.*)\)', sql)[0].split(',')#属性
            
            creat_table(sql_word[2], columns_list)

        elif sql_word[1]=='view':
            view_name=sql_word[2]
            view_word=sql_word[4:]#子查询语句
            result = " ".join(view_word)#连接子查询语句
            creat_view(view_name,result)
        elif sql_word[1]=='index':
            index_name=sql_word[2]
            table_name=sql_word[4]
            values = re.findall('\((.*)\)', sql_word[5])[0].split(',')#索引字段
            a=[index_name,table_name,values]
            index_list.append(a)
            creat_index(index_name,table_name,values)
    elif operate == 'insert':  
        table_name = sql_word[2]
        if not safe(sql_word[2],operate):
            return False
        if sql_word[4]=='values':#单个元组的插入
            sql1=sql
            sql1=sql1.replace('(','')
            sql1=sql1.replace(')','')
            sql_word1 = sql1.split(" ")
            cols = sql_word1[3].split(',')
            cols1=sql_word1[5].split(',')
            columns_list={}
            for i in range(len(cols)):
                columns_list[cols[i]]=cols1[i]

            insert(table_name,columns_list)
        else:#子查询的插入
            select_sql=sql_word[4:]
            insert(table_name,select_sql=select_sql)
    elif operate=='update':
        table_name=sql_word[1]
        columns_list={}
        update_columns_list={}

        cols = sql_word[3].split(',')
        for i in range(len(cols)):
            cols1=cols[i].split('=')
            columns_list[cols1[0]]=cols1[1]
        where_sql=sql_word[5:]
        if not safe(sql_word[1],operate,columns_list):
            return False
        update(table_name,columns_list,where_sql)
    elif operate=='delete':
        if not safe(sql_word[2],operate):
            return False
        #DELETE FROM table_name WHERE column_name = 'Value'
        
        if 'where' not in sql_word:

            table_name=sql_word[2]
            delete(table_name)
        else:#带where的删除
            where_sql=sql_word[4:]
            table_name=sql_word[2]
            delete(table_name,where_sql)
    elif operate=='select':

        #"select * from sc where id=001 and name='xiaohong'"
        table_list=sql_word[3].split(',')
        if sql_word[1]=='*':
            columns=['*']
        else:
            columns=sql_word[1].split(',')
        if not safe(sql_word[3],operate,columns):
            return False  
        if 'where' not in sql_word:#没有where的简单
            if 'union' in sql_word:

                where_index=sql_word.index('union')
                jihe_sql=sql_word[where_index:]
                jihe_sql=" ".join(jihe_sql)
                select(columns,table_list,jihe_sql=jihe_sql)
            elif 'intersect' in sql_word:
                where_index=sql_word.index('intersect')
                
                jihe_sql=sql_word[where_index:]
                jihe_sql=" ".join(jihe_sql)
                select(columns,table_list,jihe_sql=jihe_sql)
            elif 'except' in sql_word:
                where_index=sql_word.index('except')
                
                jihe_sql=sql_word[where_index:]
                jihe_sql=" ".join(jihe_sql)
                select(columns,table_list,jihe_sql=jihe_sql)
            else:
                if 'order' in sql_word:#有没有order by
                    where_index=sql_word.index('order')

                    order_sql=sql_word[where_index:]
                    group_sql=[]
                    having_sql=[]
                elif 'group' in sql_word:#有没有分组
                    where_index=sql_word.index('group')
                    if 'having' in sql_word:#有没有having
                        group_index=sql_word.index('having')
                        order_sql=[]
                        group_sql=sql_word[where_index:group_index]
                        having_sql=sql_word[group_index:]
                    else:
                        order_sql=[]
                        group_sql=sql_word[where_index:]
                        having_sql=[]
                else:
                    order_sql=[]
                    group_sql=[]
                    having_sql=[]

                select(columns,table_list,'',tag,order_sql,group_sql,having_sql,jihe_sql='')
        else:#有where,分割where_sql
            if 'union' in sql_word:

                where_index=sql_word.index('union')
                where_sql=sql_word[5:where_index]
                jihe_sql=sql_word[where_index:]
                jihe_sql=" ".join(jihe_sql)
                select(columns,table_list,where_sql,tag,jihe_sql=jihe_sql)
            elif 'intersect' in sql_word:
                where_index=sql_word.index('intersect')
                where_sql=sql_word[5:where_index]
                jihe_sql=sql_word[where_index:]
                jihe_sql=" ".join(jihe_sql)
                select(columns,table_list,where_sql,tag,jihe_sql=jihe_sql)
            elif 'except' in sql_word:
                where_index=sql_word.index('except')
                where_sql=sql_word[5:where_index]
                jihe_sql=sql_word[where_index:]
                jihe_sql=" ".join(jihe_sql)
                select(columns,table_list,where_sql,tag,jihe_sql=jihe_sql)
            else:
                if 'order' in sql_word:
                    where_index=sql_word.index('order')
                    where_sql=sql_word[5:where_index]
                    order_sql=sql_word[where_index:]
                elif 'group' in sql_word:
                    where_index=sql_word.index('group')
                    if 'having' in sql_word:
                        group_index=sql_word.index('having')
                        where_sql=sql_word[5:where_index]
                        group_sql=sql_word[where_index:group_index]
                        having_sql=sql_word[group_index:]
                    else:
                        where_sql=sql_word[5:where_index]
                        group_sql=sql_word[where_index:]
                        having_sql=[]
                else:
                    group_sql=[]
                    having_sql=[]
                    where_sql=sql_word[5:]
                    order_sql=[]

                select(columns,table_list,where_sql,tag,order_sql,group_sql,having_sql,jihe_sql='')
            
    elif operate=='grant':
        columns=sql_word[1].split(',')
        table_name=sql_word[3]
        username=sql_word[5]
        grant(columns,table_name,username)
    elif operate=='revoke':
        columns=sql_word[1].split(',')
        table_name=sql_word[3]
        username=sql_word[5]     
        revoke(columns,table_name,username)
    else:
        print("请输入正确的SQL语句！")
        
def jihe_f(jihe_sql):
    sql_word=jihe_sql.split(' ')
    sql_word=sql_word[1:]
    table_list=sql_word[3].split(',')
    if sql_word[1]=='*':
        columns=['*']
    else:
        columns=sql_word[1].split(',')
    if 'where' not in sql_word:#没有where的简单
        df=select(columns,table_name,'',tag='insert')
        return df
    else:#有where

        where_sql=sql_word[5:]
        
        df=select(columns,table_list,where_sql,tag='insert')
        return df

def select(columns,table_list,where_sql='',tag='',order_sql='',group_sql='',having_sql='',jihe_sql=''):
    if len(table_list)==1:
        table_name=table_list[0]
        db=pd.read_csv('Data/'+table_name+'.csv',index_col=0,header=0)
        db=db.loc['tuple']
    else:
        
        df_1=pd.read_csv('data/'+table_list[0]+'.csv',index_col=0)
        df_2=pd.read_csv('data/'+table_list[1]+'.csv',index_col=0)
        df_1,df_2=df_1.loc['tuple'],df_2.loc['tuple']
        db=pd.merge(df_1,df_2, how='inner')
    if len(where_sql)==0:#没有where时的简单
        
        if "*" not in columns:#按属性列输出
            if len(jihe_sql)!=0:
                df_jihe=jihe_f(jihe_sql)
                if jihe_sql[0]=='union':
                    db=pd.merge(db,df_jihe, how='outer')
                elif jihe_sql[0]=='intersect':
                    db=pd.merge(db,df_jihe)
                elif jihe_sql[0]=='except':
                    db = db.append(df_jihe)
                    db = db.append(df_jihe)
                    db = db.drop_duplicates(keep=False)
            if tag=='insert':#reture 选择的数据帧
                if len(order_sql)!=0:#说明有order by
                    column=order_sql[2]
                    if order_sql[3]=='desc':#降序
                        db.sort_values(by=column,ascending=False,inplace=True)
                    elif order_sql[3]=='asc':#升序
                        db.sort_values(by=column,inplace=True)
                columns_list=gather(columns,db)
                return db.loc['tuple'][columns_list]
            else:#打印输入
                if len(order_sql)!=0:#order by
                    column=order_sql[2]
                    if order_sql[3]=='desc':
                        db.sort_values(by=column,ascending=False,inplace=True)
                    elif order_sql[3]=='asc':
                        db.sort_values(by=column,inplace=True)
                columns_list=gather(columns,db)
                if len(group_sql)!=0:#有分组
                    column=group_sql[2]
                    grouped = db.groupby(column)
                    for name,group in grouped:
                        if len(having_sql)!=0:#有having
                            limit=having_sql[1]
                            dict_1={}
                            if '>' in limit:
                                cols=limit.split('>')
                                dict_1[cols[0]]=cols[1]
                                symbol='>'
                            elif '<' in limit:
                                cols=limit.split('<')
                                dict_1[cols[0]]=cols[1]
                                symbol='<'
                            elif '=' in limit:
                                cols=limit.split('=')
                                dict_1[cols[0]]=cols[1]
                                symbol='='
                            if '>' in limit:
                                if name>dict_1[column]:
                                    print(name)
                                    print(group.head())
                            elif '<' in limit:
                                if name<dict_1[column]:
                                    print(name)
                                    print(group.head())
                            elif '=' in limit:
                                if name==dict_1[column]:
                                    print(name)
                                    print(group.head())
                        else:
                            print(name)
                            print(group.head())#按组输出
                else:#无分组直接打印
                    if len(columns_list)!=0:
                        print(db[columns_list])
        else:#按*打印输出
            if len(jihe_sql)!=0:
                df_jihe=jihe_f(jihe_sql)
                if jihe_sql[0]=='union':
                    db=pd.merge(db,df_jihe, how='outer')
                elif jihe_sql[0]=='intersect':
                    db=pd.merge(db,df_jihe)
                elif jihe_sql[0]=='except':
                    db = db.append(df_jihe)
                    db = db.append(df_jihe)
                    db = db.drop_duplicates(keep=False)
            if tag=='insert':
                if len(order_sql)!=0:
                    column=order_sql[2]
                    if order_sql[3]=='desc':
                        db.sort_values(by=column,ascending=False,inplace=True)
                    elif order_sql[3]=='asc':
                        db.sort_values(by=column,inplace=True)
                columns_list=gather(columns,db)
                return db.loc['tuple']
            else:
                if len(order_sql)!=0:
                    column=order_sql[2]
                    if order_sql[3]=='desc':
                        db.sort_values(by=column,ascending=False,inplace=True)
                    elif order_sql[3]=='asc':
                        db.sort_values(by=column,inplace=True)
                columns_list=gather(columns,db)
                if len(group_sql)!=0:
                    column=group_sql[2]
                    grouped = db.groupby(column)
                    for name,group in grouped:
                        if len(having_sql)!=0:
                            limit=having_sql[1]
                            dict_1={}
                            if '>' in limit:
                                cols=limit.split('>')
                                dict_1[cols[0]]=cols[1]
                                symbol='>'
                            elif '<' in limit:
                                cols=limit.split('<')
                                dict_1[cols[0]]=cols[1]
                                symbol='<'
                            elif '=' in limit:
                                cols=limit.split('=')
                                dict_1[cols[0]]=cols[1]
                                symbol='='
                            if '>' in limit:
                                if name>dict_1[column]:
                                    print(name)
                                    print(group.head())
                            elif '<' in limit:
                                if name<dict_1[column]:
                                    print(name)
                                    print(group.head())
                            elif '=' in limit:
                                if name==dict_1[column]:
                                    print(name)
                                    print(group.head())
                        else:
                            print(name)
                            print(group.head())
                else:
                    print(db)
    else:#有where时
        
        if 'between' in where_sql:
            column,mins,maxs=where(where_sql)
            db=db.loc['tuple']
            df=db[(db[column]<maxs)&(db[column]>mins)]#选择好数据帧
            #进行返回或打印，重复
            if "*" not in columns:
                if len(jihe_sql)!=0:#是否有集合查询
                    df_jihe=jihe_f(jihe_sql)
                    if jihe_sql[0]=='union':
                        df=pd.merge(df,df_jihe, how='outer')
                    elif jihe_sql[0]=='intersect':
                        df=pd.merge(df,df_jihe)
                    elif jihe_sql[0]=='except':
                        df = df.append(df_jihe)
                        df = df.append(df_jihe)
                        df = df.drop_duplicates(keep=False)
                if tag=='insert':
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    return df[columns_list]
                else:
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    
                    columns_list=gather(columns,df)
                    if len(group_sql)!=0:
                        column=group_sql[2]
                        grouped = df.groupby(column)
                        for name,group in grouped:
                            if len(having_sql)!=0:
                                limit=having_sql[1]
                                dict_1={}
                                if '>' in limit:
                                    cols=limit.split('>')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='>'
                                elif '<' in limit:
                                    cols=limit.split('<')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='<'
                                elif '=' in limit:
                                    cols=limit.split('=')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='='
                                if '>' in limit:
                                    if name>dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '<' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '=' in limit:
                                    if name==dict_1[column]:
                                        print(name)
                                        print(group.head())
                            else:
                                print(name)
                                print(group.head())
                    else:
                         print(df[columns_list])
            else:
                if len(jihe_sql)!=0:
                    df_jihe=jihe_f(jihe_sql)
                    if jihe_sql[0]=='union':
                        df=pd.merge(df,df_jihe, how='outer')
                    elif jihe_sql[0]=='intersect':
                        df=pd.merge(df,df_jihe)
                    elif jihe_sql[0]=='except':
                        df = df.append(df_jihe)
                        df = df.append(df_jihe)
                        df = df.drop_duplicates(keep=False)
                if tag=='insert':
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    
                    columns_list=gather(columns,df)
                    return df
                else: 
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    if len(group_sql)!=0:
                        column=group_sql[2]
                        grouped = df.groupby(column)
                        for name,group in grouped:
                            if len(having_sql)!=0:
                                limit=having_sql[1]
                                dict_1={}
                                if '>' in limit:
                                    cols=limit.split('>')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='>'
                                elif '<' in limit:
                                    cols=limit.split('<')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='<'
                                elif '=' in limit:
                                    cols=limit.split('=')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='='
                                if '>' in limit:
                                    if name>dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '<' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '=' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                            else:
                                print(name)
                                print(group.head())
                    else:
                        print(df)
        elif 'in' in where_sql:
            db=db.loc['tuple']
            column,values=where(where_sql)
            if 'select' in values:#嵌套查询之一in
                columns_1=values[1].split(',')
                table_list_1=values[3].split(',')
                where_sql_1=values[5:]
                df= select(columns_1,table_list_1,where_sql_1,tag='insert')
                values=df[column].tolist()
            
            df=pd.DataFrame()
            for i in values:
                df1=db[db[column]==i]
                df=pd.concat([df,df1],axis=0)#得到想要的df数据帧
            #返回或打印，重复
            if "*" not in columns:
                if len(jihe_sql)!=0:
                    df_jihe=jihe_f(jihe_sql)
                    if jihe_sql[0]=='union':
                        df=pd.merge(df,df_jihe, how='outer')
                    elif jihe_sql[0]=='intersect':
                        df=pd.merge(df,df_jihe)
                    elif jihe_sql[0]=='except':
                        df = df.append(df_jihe)
                        df = df.append(df_jihe)
                        df = df.drop_duplicates(keep=False)
                if tag=='insert':
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    return df[columns_list]
                else:
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    if len(group_sql)!=0:
                        column=group_sql[2]
                        grouped = df.groupby(column)
                        for name,group in grouped:
                            if len(having_sql)!=0:
                                limit=having_sql[1]
                                dict_1={}
                                if '>' in limit:
                                    cols=limit.split('>')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='>'
                                elif '<' in limit:
                                    cols=limit.split('<')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='<'
                                elif '=' in limit:
                                    cols=limit.split('=')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='='
                                if '>' in limit:
                                    if name>dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '<' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '=' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                            else:
                                print(name)
                                print(group.head())
                    else:
                        print(df[columns_list])
            else:
                if len(jihe_sql)!=0:
                    df_jihe=jihe_f(jihe_sql)
                    if jihe_sql[0]=='union':
                        df=pd.merge(df,df_jihe, how='outer')
                    elif jihe_sql[0]=='intersect':
                        df=pd.merge(df,df_jihe)
                    elif jihe_sql[0]=='except':
                        df = df.append(df_jihe)
                        df = df.append(df_jihe)
                        df = df.drop_duplicates(keep=False)
                if tag=='insert':
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    return df
                else:
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    if len(group_sql)!=0:
                        column=group_sql[2]
                        grouped = df.groupby(column)
                        for name,group in grouped:
                            if len(having_sql)!=0:
                                limit=having_sql[1]
                                dict_1={}
                                if '>' in limit:
                                    cols=limit.split('>')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='>'
                                elif '<' in limit:
                                    cols=limit.split('<')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='<'
                                elif '=' in limit:
                                    cols=limit.split('=')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='='
                                if '>' in limit:
                                    if name>dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '<' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '=' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                            else:
                                print(name)
                                print(group.head())
                    else:
                        print(df)    
        elif 'like' in where_sql:
            db=db.loc['tuple']
            column,list_f,list_l=where(where_sql)

            df=pd.DataFrame()
            list1=list(db[column])

            for i in range(len(list1)):
                list2_f=list(list1[i])
                a=list1[i]
                b=a[::-1]
                list2_l=list(b)
                flag=0
                flag1=0
                if '%' in list_l:
                    if len(list_f)!=len(list2_f):
                        flag=1
                    for j in range(len(list_f)):
                        if (list_f[j]!=list2_f[j])&(list_f[j]!='_'):

                            flag=1
                            break
                    if flag==0:
                        df1=db[db[column]==list1[i]]
                        df=pd.concat([df,df1],axis=0)
                else:
                    for j in range(len(list_f)):
                        if (list_f[j]!=list2_f[j])&(list_f[j]!='_'):

                            flag=1
                            break
                    if flag==0:
                        if len(list_l)>0:
                            for k in range(len(list_l)):
                                if (list_l[k]!=list2_l[k])&(list_l[k]!='_'):

                                    flag1=1
                                    break
                            if flag1==0:
                                df1=db[db[column]==list1[i]]
                                df=pd.concat([df,df1],axis=0)
                        else:
                            df1=db[db[column]==list1[i]]
                            df=pd.concat([df,df1],axis=0)#得到想要的df数据帧
            #返回或输入，重复
            if "*" not in columns:
                if len(jihe_sql)!=0:
                    df_jihe=jihe_f(jihe_sql)
                    if jihe_sql[0]=='union':
                        df=pd.merge(df,df_jihe, how='outer')
                    elif jihe_sql[0]=='intersect':
                        df=pd.merge(df,df_jihe)
                    elif jihe_sql[0]=='except':
                        df = df.append(df_jihe)
                        df = df.append(df_jihe)
                        df = df.drop_duplicates(keep=False)
                if tag=='insert':
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    return df[columns_list]
                else:
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    if len(group_sql)!=0:
                        column=group_sql[2]
                        grouped = df.groupby(column)
                        for name,group in grouped:
                            if len(having_sql)!=0:
                                limit=having_sql[1]
                                dict_1={}
                                if '>' in limit:
                                    cols=limit.split('>')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='>'
                                elif '<' in limit:
                                    cols=limit.split('<')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='<'
                                elif '=' in limit:
                                    cols=limit.split('=')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='='
                                if '>' in limit:
                                    if name>dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '<' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '=' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                            else:
                                print(name)
                                print(group.head())
                    else:
                        print(df[columns_list])
            else:
                if len(jihe_sql)!=0:
                    df_jihe=jihe_f(jihe_sql)
                    if jihe_sql[0]=='union':
                        df=pd.merge(df,df_jihe, how='outer')
                    elif jihe_sql[0]=='intersect':
                        df=pd.merge(df,df_jihe)
                    elif jihe_sql[0]=='except':
                        df = df.append(df_jihe)
                        df = df.append(df_jihe)
                        df = df.drop_duplicates(keep=False)
                if tag=='insert':
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    return df
                else:
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    if len(group_sql)!=0:
                        column=group_sql[2]
                        grouped = df.groupby(column)
                        for name,group in grouped:
                            if len(having_sql)!=0:
                                limit=having_sql[1]
                                dict_1={}
                                if '>' in limit:
                                    cols=limit.split('>')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='>'
                                elif '<' in limit:
                                    cols=limit.split('<')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='<'
                                elif '=' in limit:
                                    cols=limit.split('=')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='='
                                if '>' in limit:
                                    if name>dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '<' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '=' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                            else:
                                print(name)
                                print(group.head())
                    else:
                        print(df)           
        else:#or或者and时
            dfs=[]
            link,limit,symbol=where(where_sql)
            for k,i in zip(limit,symbol):


                if i=='=':
                    for i in k:
                        if '.' in i:
                            index=i.index('.')
                            j=i[(index+1):] 
                            if '.' in k[i]:
                                df1=db
                            else:
                                df1=db[db[j]==k[i]]
                        else:
                            df1=db[db[i]==k[i]]
                    dfs.append(df1)
                elif i=='<':
                    for i in k:
                        if '.' in i:
                            index=i.index('.')
                            j=i[(index+1):] 
                            if '.' in k[i]:
                                df1=db
                            else:
                                df1=db[db[j]<k[i]]
                        else:
                            df1=db[db[i]<k[i]]

                    dfs.append(df1)
                elif i=='>':
                    for i in k:
                        if '.' in i:
                            index=i.index('.')
                            j=i[(index+1):] 

                            if '.' in k[i]:
                                df1=db
                            else:
                                df1=db[db[j]>k[i]]
                        else:
                            df1=db[db[i]>k[i]]
                    dfs.append(df1)
            for i in range(len(dfs)):
                if i<len(dfs)-1:
                    if link[i]=='and':


                        dfs[i+1]=pd.merge(dfs[i],dfs[i+1])
                    elif link[i]=='or':

                        dfs[i+1]=pd.merge(dfs[i],dfs[i+1],how='outer')
            df=dfs[len(dfs)-1]
            #输出或返回，重复
            if "*" not in columns:
                if len(jihe_sql)!=0:
                    df_jihe=jihe_f(jihe_sql)
                    if jihe_sql[0]=='union':
                        df=pd.merge(df,df_jihe, how='outer')
                    elif jihe_sql[0]=='intersect':
                        df=pd.merge(df,df_jihe)
                    elif jihe_sql[0]=='except':
                        df = df.append(df_jihe)
                        df = df.append(df_jihe)
                        df = df.drop_duplicates(keep=False)
                if tag=='insert':
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    return df[columns_list]
                else:
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    if len(group_sql)!=0:
                        column=group_sql[2]
                        grouped = df.groupby(column)
                        for name,group in grouped:
                            if len(having_sql)!=0:
                                limit=having_sql[1]
                                dict_1={}
                                if '>' in limit:
                                    cols=limit.split('>')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='>'
                                elif '<' in limit:
                                    cols=limit.split('<')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='<'
                                elif '=' in limit:
                                    cols=limit.split('=')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='='
                                if '>' in limit:
                                    if name>dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '<' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '=' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                            else:
                                print(name)
                                print(group.head())
                    else:
                        print(df[columns_list])
            else:
                if len(jihe_sql)!=0:
                    df_jihe=jihe_f(jihe_sql)
                    jihe_sql=jihe_sql.split(' ')
                    if jihe_sql[0]=='union':
                        df=pd.merge(df,df_jihe, how='outer')
                        print(2)
                    elif jihe_sql[0]=='intersect':
                        df=pd.merge(df,df_jihe)
                    elif jihe_sql[0]=='except':
                        df = df.append(df_jihe)
                        df = df.append(df_jihe)
                        df = df.drop_duplicates(keep=False)
                if tag=='insert':
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    return df
                else:
                    if len(order_sql)!=0:
                        column=order_sql[2]
                        if order_sql[3]=='desc':
                            df.sort_values(by=column,ascending=False,inplace=True)
                        elif order_sql[3]=='asc':
                            df.sort_values(by=column,inplace=True)
                    columns_list=gather(columns,df)
                    if len(group_sql)!=0:
                        column=group_sql[2]
                        grouped = df.groupby(column)
                        for name,group in grouped:
                            if len(having_sql)!=0:
                                limit=having_sql[1]
                                dict_1={}
                                if '>' in limit:
                                    cols=limit.split('>')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='>'
                                elif '<' in limit:
                                    cols=limit.split('<')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='<'
                                elif '=' in limit:
                                    cols=limit.split('=')
                                    dict_1[cols[0]]=cols[1]
                                    symbol='='
                                if '>' in limit:
                                    if name>dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '<' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                                elif '=' in limit:
                                    if name<dict_1[column]:
                                        print(name)
                                        print(group.head())
                            else:
                                print(name)
                                print(group.head())
                    else:
                        print(df) 

def check_Constraint(df):
    columns=list(df.columns)
    for k in columns:
        if(df.loc['primary key'][k]=='1'):
            try:
                db=df.loc['tuple']
                countdata=db[k].value_counts().reset_index()
                a=list(countdata[k])
                for i in range(len(a)):
                    if a[i]!=1:
                        print('不允许重复值')
                        return False

            except:
                continue

        elif df.loc['unique'][k]=='1':
            try:
                db=df.loc['tuple']
                countdata=db[k].value_counts().reset_index()
                a=list(countdata[k])
                for i in range(len(a)):
                    if a[i]!=1:
                        print('不允许重复值')
                        return False
            except:
                continue
    return True

def check_null(db):
    for k in db.columns:
        if(db.loc['primary key'][k]=='1'):
            if db[k].isna().sum()>0:
                print(k+"属性不允许有空值")
                return False
        elif db.loc['not null'][k]=='1':
            if db[k].isna().sum()>0:
                print(k+"属性不允许有空值")  
                return False
    return True

def grant(columns,table_name,username):
    db=pd.read_csv('Data/user.csv',header=0,index_col=0)
    if 'all' in db[db['user']==user]['grant'].values:
        if username in db['user'].values:
            
            for i in range(len(columns)):
                if columns[i]=='all privileges':
                    db.replace(db[db['user']==username],'all',inplace=True)
                elif columns[i]=='create':
                    db[columns[i]].replace(db[db['user']==username][columns[i]],'all',inplace=True)
                else:
                    if "(" in columns[i]:
                        index=columns[i].index('(')
                        index_1=columns[i].index(')')
                        privileges=columns[i][:index]
                        a=columns[i][index:index_1+1]
                    else:
                        privileges=columns[i]
                        a=''
                    db[privileges].replace(db[db['user']==username][privileges],db[db['user']==username][privileges]+','+table_name+a,inplace=True)
        else:        
            print("没有该用户，请先建立用户！")
    else:
        print("用户权限不够！")
    db.to_csv('Data/user.csv')

def revoke(columns,table_name,username):
    db=pd.read_csv('Data/user.csv',header=0,index_col=0)
    if 'all' in db[db['user']==user]['revoke'].values:
        if username in db['user'].values:

            for i in range(len(columns)):
                if "(" in columns[i]:
                    index=columns[i].index('(')
                    index_1=columns[i].index(')')
                    privileges=columns[i][:index]
                    a=columns[i][index:index_1+1]
                else:
                    privileges=columns[i]
                    a=''
                b=str(db[db['user']==username][privileges])
                cols=b.split(' ')
                cols
                b=cols[4]
                b=b.replace('\nName:','')
                if table_name in b:
                    b=b.replace(','+table_name+a,'') 
                    db[privileges].replace(db[db['user']==username][privileges],b,inplace=True)
                    db.to_csv('Data/user.csv')
                else:
                    print("该用户没有该权限")
        else:        
            print("没有该用户，请先建立用户！")
    else:
        print("用户权限不够")

def creat_view(view_name,result):
    db=pd.read_csv('Data/view.csv',index_col=0)
    
    a=[[view_name,result]]
    df=pd.DataFrame(a,columns={'view_name':'','子查询':''},index=['tuple'])
    db=pd.concat([db,df],axis=0)
    db.to_csv('Data/view.csv')

def safe(table_name,root_name,columns=''):
    db=pd.read_csv('data/user.csv',index_col=0)
    column_list=[]
    if len(columns)!=0:
        try:
            for k in columns:
                column_list.append(k)
        except:
            column_list=columns
    if 'all' in db[db['user']==user][root_name].values:
        return True
    elif table_name in str(db[db['user']==user][root_name]):
        for i in column_list:
            if i not in str(db[db['user']==user][root_name]):
                print('没有该列的权限！') 
                return False
        return True
    else :
        print('没有该权限！') 
        return False

def update(table_name,columns_list,where_sql=''):

    db=pd.read_csv('Data/'+table_name+'.csv',index_col=0,header=0)

    df_list=db.loc[['type','primary key','unique','not null','foreign key','check']]
    db=db.loc['tuple']
    counts=0
    if len(where_sql)==0:
        for kk in columns_list:

            b=columns_list[kk]

            db[kk].replace(db[kk],b,inplace=True)

    else:
        if 'between' in where_sql:
            column,mins,maxs=where(where_sql)#'age', '17', '20'
            db=db.loc['tuple']
            counts=df.shape[0]
            db = db.append(df1)
            db = db.append(df1)
            db.drop_duplicates(keep=False,inplace=True)
            for kk in columns_list:

                b=columns_list[kk]

                df1[kk].replace(df1[kk],b,inplace=True)
            db=pd.concat([df1,db],axis=0)


        elif 'in' in where_sql:#'age', ['19', '20']
            db=db.loc['tuple']
            column,values=where(where_sql)
            df=pd.DataFrame()
            for i in values:
                df1=db[db[column]==i]
                db = db.append(df1)
                db = db.append(df1)
                db.drop_duplicates(keep=False,inplace=True)
                for kk in columns_list:

                    b=columns_list[kk]

                    df1[kk].replace(df1[kk],b,inplace=True)
                db=pd.concat([df1,db],axis=0)
            counts=df.shape[0]

        elif 'like' in where_sql:#'name', ['x', 'i', 'a', 'o', 'h', 'o'], []
            db=db.loc['tuple']
            column,list_f,list_l=where(where_sql)

            df=pd.DataFrame()
            list1=list(db[column])

            for i in range(len(list1)):
                list2_f=list(list1[i])
                a=list1[i]
                b=a[::-1]
                list2_l=list(b)
                flag=0
                flag1=0
                if '%' in list_l:
                    if len(list_f)!=len(list2_f):
                        flag=1
                    for j in range(len(list_f)):
                        try:
                            if (list_f[j]!=list2_f[j])&(list_f[j]!='_'):

                                flag=1
                                break
                        except:
                            flag=1
                            break
                    if flag==0:
                        df1=db[db[column]==list1[i]]
                        db = db.append(df1)
                        db = db.append(df1)
                        db.drop_duplicates(keep=False,inplace=True)
                        for kk in columns_list:

                            b=columns_list[kk]

                            df1[kk].replace(df1[kk],b,inplace=True)
                        db=pd.concat([df1,db],axis=0)
                else:
                    for j in range(len(list_f)):
                        try:
                            if (list_f[j]!=list2_f[j])&(list_f[j]!='_'):

                                flag=1
                                break
                        except:
                            flag=1
                            break
                    if flag==0:
                        if len(list_l)>0:
                            for k in range(len(list_l)):
                                try:
                                    if (list_l[k]!=list2_l[k])&(list_l[k]!='_'):

                                        flag=1
                                        break
                                except:
                                    flag=1
                                    break                            
                            if flag1==0:
                                df1=db[db[column]==list1[i]]
                                for kk in columns_list:

                                    b=columns_list[kk]

                                    db[kk].replace(df1[kk],b,inplace=True)
                        else:
                            df1=db[db[column]==list1[i]]
                            db = db.append(df1)
                            db = db.append(df1)
                            db.drop_duplicates(keep=False,inplace=True)
                            for kk in columns_list:

                                b=columns_list[kk]

                                df1[kk].replace(df1[kk],b,inplace=True)
                            db=pd.concat([df1,db],axis=0)
            counts=df.shape[0]

        else:
            dfs=[]
            link,limit,symbol=where(where_sql)
            for k,i in zip(limit,symbol):
                if i=='=':
                    for i in k:
                        df1=db[db[i]==k[i]]
                    dfs.append(df1)
                elif i=='<':
                    for i in k:
                        df1=db[db[i]<k[i]]

                    dfs.append(df1)
                elif i=='>':
                    for i in k:
                        df1=db[db[i]==k[i]]
                    dfs.append(df1)
            for i in range(len(dfs)):
                if i<len(dfs)-1:
                    if link[i]=='and':


                        dfs[i+1]=pd.merge(dfs[i],dfs[i+1])
                    elif link[i]=='or':

                        dfs[i+1]=pd.merge(dfs[i],dfs[i+1],how='outer')
            df=dfs[len(dfs)-1]
            db = db.append(df)
            db = db.append(df)
            db.drop_duplicates(keep=False,inplace=True)
            for kk in columns_list:

                b=columns_list[kk]

                df[kk].replace(df[kk],b,inplace=True)
            db=pd.concat([df,db],axis=0)
            counts=df.shape[0]

        db=db.append(df_list)
    if (check_null(df_all))&(check_Constraint(df_all)):
        print('更改操作成功')
        print('一共更改%d个元组'%counts)
        db.to_csv('Data/'+table_name+'.csv')
    else:
        return False
    for i in range(len(index_list)):
        if table_name in index_list[i]:
            creat_index(index_list[i][0],index_list[i][1],index_list[i][2])

def creat_index(index_name,table_name,values):
    db=pd.read_csv('Data/'+table_name+'.csv',index_col=0)
    db=db.loc['tuple']
    column=values[0]
    a=list(db[column])
    b={}
    c=[]
    for i in range(len(a)):
        if hash(a[i]) not in c:
            c.append(hash(a[i]))
            df1=db[db[column]==a[i]]
            list1=SLinkList(100)
            list1.Append(df1)
            b[hash(a[i])]=list1
        else:
            df1=db[db[column]==a[i]]
            b[hash(a[i])].Append(df1)
    d=[b]
    df=pd.DataFrame(d)
    df.to_csv('Data/index/'+index_name+'.csv')

def delete(table_name,where_sql=''):
    db=pd.read_csv('Data/'+table_name+'.csv',index_col=0,header=0)
    df_list=db.loc[['type','primary key','unique','not null','foreign key','check']]
    counts=0
    if len(where_sql)==0:
        db=df_list
    else:
        if 'between' in where_sql:
            column,mins,maxs=where(where_sql)#'age', '17', '20'
            db=db.loc['tuple']
            df=db[(db[column]<maxs)&(db[column]>mins)]
            counts=df.shape[0]
            db = db.append(df)
            db = db.append(df)
            db.drop_duplicates(keep=False,inplace=True)

        elif 'in' in where_sql:#'age', ['19', '20']
            db=db.loc['tuple']
            column,values=where(where_sql)
            df=pd.DataFrame()
            for i in values:
                df1=db[db[column]==i]
                df=pd.concat([df,df1],axis=0)
            counts=df.shape[0]
            db = db.append(df)
            db = db.append(df)
            db.drop_duplicates(keep=False,inplace=True)   
        elif 'like' in where_sql:#'name', ['x', 'i', 'a', 'o', 'h', 'o'], []
            db=db.loc['tuple']
            column,list_f,list_l=where(where_sql)

            df=pd.DataFrame()
            list1=list(db[column])

            for i in range(len(list1)):
                list2_f=list(list1[i])
                a=list1[i]
                b=a[::-1]
                list2_l=list(b)
                flag=0
                flag1=0
                if '%' in list_l:
                    if len(list_f)!=len(list2_f):
                        flag=1
                    for j in range(len(list_f)):
                        try:
                            if (list_f[j]!=list2_f[j])&(list_f[j]!='_'):

                                flag=1
                                break
                        except:
                            flag=1
                            break
                    if flag==0:
                        df1=db[db[column]==list1[i]]
                        df=pd.concat([df,df1],axis=0)
                else:
                    for j in range(len(list_f)):
                        try:
                            if (list_f[j]!=list2_f[j])&(list_f[j]!='_'):

                                flag=1
                                break
                        except:
                            flag=1
                            break
                    if flag==0:
                        if len(list_l)>0:
                            for k in range(len(list_l)):
                                try:
                                    if (list_l[k]!=list2_l[k])&(list_l[k]!='_'):

                                        flag=1
                                        break
                                except:
                                    flag=1
                                    break                            
                            if flag1==0:
                                df1=db[db[column]==list1[i]]
                                df=pd.concat([df,df1],axis=0)
                        else:
                            df1=db[db[column]==list1[i]]
                            df=pd.concat([df,df1],axis=0)
            counts=df.shape[0]
            db = db.append(df)
            db = db.append(df)
            db.drop_duplicates(keep=False,inplace=True)           
        else:
            dfs=[]
            link,limit,symbol=where(where_sql)
            for k,i in zip(limit,symbol):
                if i=='=':
                    for i in k:
                        df1=db[db[i]==k[i]]
                    dfs.append(df1)
                elif i=='<':
                    for i in k:
                        df1=db[db[i]<k[i]]

                    dfs.append(df1)
                elif i=='>':
                    for i in k:
                        df1=db[db[i]==k[i]]
                    dfs.append(df1)
            for i in range(len(dfs)):
                if i<len(dfs)-1:
                    if link[i]=='and':


                        dfs[i+1]=pd.merge(dfs[i],dfs[i+1])
                    elif link[i]=='or':

                        dfs[i+1]=pd.merge(dfs[i],dfs[i+1],how='outer')
            df=dfs[len(dfs)-1]
            counts=df.shape[0]
            db = db.append(df)
            db = db.append(df)
            db.drop_duplicates(keep=False,inplace=True) 
        db=db.append(df_list)
    if not check_null(db):
        return False
    else:
        print('删除操作成功')
        print('一共删除%d个元组'%counts)
        db.to_csv('Data/'+table_name+'.csv')
    for i in range(len(index_list)):
        if table_name in index_list[i]:
            creat_index(index_list[i][0],index_list[i][1],index_list[i][2])

class item (object):#节点
    def __init__(self, data):
        self.data = data
        self.next = None

class SLinkList(object):
    def __init__(self, size = 100):
        '''
        初始化主要是用于初始化链表的大小
        '''
        self.link = [item(None) for i in range(size + 1)]    # 申请size大小的节点空间
        self.link[0].next = None    # 表示空表
        self.link[0].space = 1   # 指向第一个节点，因为初始化时第一个节点为空闲节点

        i = 1
        while i < size:
            self.link[i].next = i+1    # 利用空闲节点连成一个新的表，并且头结点的space始终指向下一个空闲的节点
            i += 1

        self.link[i].next = None    # 空闲表尾指向None

        self.length = 0    # 链表长度
        self.rear = 0    # 表尾指针
    def Malloc_SL(self):
            '''
            类似于C中malloc函数申请空间，返回空闲节点的下标
            '''
            i = self.link[0].space
            if self.link[0].space:
                self.link[0].space = self.link[i].next

            return i
    def Append(self, data):
            '''往链表表尾添加元素, 并返回新添加元素的下标'''
            node_index = self.Malloc_SL()

            if not node_index:
                print("Append: NO SPACE!")
                return False

            self.link[node_index].data = data
            self.link[node_index].next = None
            self.link[self.rear].next = node_index
            self.rear = node_index
            self.length += 1
            return node_index

def atoi(s):
    s = s[::-1]
    num = 0
    for i, v in enumerate(s):
        offset = ord(v) - ord('0')
        num += offset * (10 ** i)
    return num

def where(where_sql):
    if 'between' in where_sql:
        column=where_sql[0]
        mins=where_sql[2]
        maxs=where_sql[4]
        return column,mins,maxs
    elif ('in' in where_sql)|(' = ' in where_sql):
        column=where_sql[0]
        if 'select' in where_sql[2]:
            select_sql=' '.join(where_sql)
            values = re.findall('\((.*)\)', select_sql)[0].split(' ')

        else:
            values = re.findall('\((.*)\)', where_sql[2])[0].split(',')
        return column,values
    elif 'like' in where_sql:
        list_f=[]
        list_l=[]
        column=where_sql[0]
        list0=list(where_sql[2])
        if '%' not in where_sql[2]:
            list_f=list(where_sql[2])
            list_l=['%']
        else:
            if list0[0]=='%':
                list_f=[]
                _list=where_sql[2].split('%')
                a=_list[1]
                b=a[::-1]
                list_l=list(b)
            elif list0[len(list0)-1]=='%':
                list_l=[]
                _list=where_sql[2].split('%')
                a=_list[0]
                list_f=list(a)
            else:
                list0=where_sql[2].split('%')


                list_f=list(list0[0])
                a=list0[1]
                b=a[::-1]
                list_l=list(b)
        return column,list_f,list_l
    else:
        link=[]
        condi=[]
        symbol=[]
        limit=[]

        for i in range(len(where_sql)):
            if where_sql[i]=='and' or where_sql[i]=='or':
                link.append(where_sql[i])
            else:
                condi.append(where_sql[i])

        for i in range(len(condi)):
            if '=' in condi[i]:
                a=condi[i].split('=')
                limit_dict={a[0]:a[1]}
                limit.append(limit_dict)
                symbol.append('=')
            elif '>' in condi[i]:
                a=condi[i].split('>')
                limit_dict={a[0]:a[1]}
                limit.append(limit_dict)
                symbol.append('>')
            elif '<' in condi[i]:
                a=condi[i].split('<')
                limit_dict={a[0]:a[1]}
                limit.append(limit_dict)
                symbol.append('<')
        return link,limit,symbol

if __name__ == '__main__':

    run()

